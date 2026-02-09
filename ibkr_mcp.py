"""
IBKR MCP Server - Interactive Brokers TWS MCP Integration
=========================================================
MCP server that connects to IB TWS via ib_insync and exposes
trading tools for use with Claude (Cowork / Claude Code / etc.)

Orders require a two-step flow:
  1. ib_prepare_order  → validates & returns order details for review
  2. ib_submit_order   → submits a previously prepared order (needs confirmation token)

Connection: Local TWS on 127.0.0.1:7496 (live) or 7497 (paper)
"""

import json
import hashlib
import time
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Optional, List, Dict, Any

from mcp.server.fastmcp import FastMCP
from pydantic import BaseModel, Field, ConfigDict, field_validator
from ib_insync import (
    IB, Stock, Forex, Option, Future, Contract,
    MarketOrder, LimitOrder, StopOrder, StopLimitOrder,
    ExecutionFilter, ScannerSubscription, TagValue, util
)

# ─── Configuration ────────────────────────────────────────────────────────────

def _load_config() -> dict:
    """Load config from config.json next to this script."""
    config_path = Path(__file__).parent / "config.json"
    try:
        with open(config_path) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

_config = _load_config()
TWS_HOST = _config.get("tws_host", "127.0.0.1")
TWS_PORT = _config.get("tws_port", 7496)
CLIENT_ID = _config.get("client_id", 10)
CONNECT_TIMEOUT = _config.get("connect_timeout", 10)

# ─── Pending orders store (for confirmation flow) ─────────────────────────────

_pending_orders: Dict[str, Dict[str, Any]] = {}
_PENDING_TTL = _config.get("pending_order_ttl_seconds", 300)


# ─── IB Connection Management ─────────────────────────────────────────────────

_ib: Optional[IB] = None


async def _get_ib() -> IB:
    """Get or create IB connection."""
    global _ib
    if _ib is None or not _ib.isConnected():
        _ib = IB()
        await _ib.connectAsync(TWS_HOST, TWS_PORT, clientId=CLIENT_ID, timeout=CONNECT_TIMEOUT)
    return _ib


def _safe_disconnect():
    """Safely disconnect IB."""
    global _ib
    if _ib and _ib.isConnected():
        _ib.disconnect()
    _ib = None


@asynccontextmanager
async def app_lifespan(app):
    """Manage IB connection lifecycle."""
    try:
        yield {}
    finally:
        _safe_disconnect()


# ─── Initialize MCP Server ────────────────────────────────────────────────────

mcp = FastMCP("ibkr_mcp", lifespan=app_lifespan)


# ─── Helper Functions ─────────────────────────────────────────────────────────

def _format_currency(value: float, currency: str = "USD") -> str:
    """Format monetary values."""
    return f"{value:,.2f} {currency}"


def _contract_from_params(
    symbol: str,
    sec_type: str = "STK",
    exchange: str = "SMART",
    currency: str = "USD",
    expiry: Optional[str] = None,
    strike: Optional[float] = None,
    right: Optional[str] = None,
) -> Contract:
    """Create an IB Contract from parameters."""
    if sec_type == "STK":
        return Stock(symbol, exchange, currency)
    elif sec_type == "FX":
        return Forex(symbol)
    elif sec_type == "OPT":
        return Option(symbol, expiry or "", strike or 0, right or "C", exchange, currency=currency)
    elif sec_type == "FUT":
        return Future(symbol, expiry or "", exchange, currency=currency)
    else:
        c = Contract()
        c.symbol = symbol
        c.secType = sec_type
        c.exchange = exchange
        c.currency = currency
        return c


def _generate_confirmation_token(order_details: dict) -> str:
    """Generate a unique token for order confirmation."""
    payload = json.dumps(order_details, sort_keys=True) + str(time.time())
    return hashlib.sha256(payload.encode()).hexdigest()[:12]


def _cleanup_expired_pending():
    """Remove expired pending orders."""
    now = time.time()
    expired = [k for k, v in _pending_orders.items() if now - v["created_at"] > _PENDING_TTL]
    for k in expired:
        del _pending_orders[k]


def _format_account_value(tag: str, value: str, currency: str) -> str:
    """Format a single account value for display."""
    try:
        num = float(value)
        return f"{tag}: {_format_currency(num, currency)}"
    except (ValueError, TypeError):
        return f"{tag}: {value} {currency}"


# ─── Input Models ─────────────────────────────────────────────────────────────

class AccountSummaryInput(BaseModel):
    """Input for account summary."""
    model_config = ConfigDict(extra="forbid")
    tags: Optional[List[str]] = Field(
        default=None,
        description="Specific tags to retrieve. If empty, returns all. "
                    "Common tags: NetLiquidation, TotalCashValue, GrossPositionValue, "
                    "BuyingPower, AvailableFunds, MaintMarginReq, ExcessLiquidity"
    )


class PositionsInput(BaseModel):
    """Input for positions query."""
    model_config = ConfigDict(extra="forbid")
    symbol_filter: Optional[str] = Field(
        default=None,
        description="Filter positions by symbol (case-insensitive substring match)"
    )


class MarketDataInput(BaseModel):
    """Input for real-time market data snapshot."""
    model_config = ConfigDict(extra="forbid")
    symbol: str = Field(..., description="Ticker symbol (e.g. AAPL, EURUSD, SPY)", min_length=1, max_length=20)
    sec_type: str = Field(default="STK", description="Security type: STK, FX, OPT, FUT")
    exchange: str = Field(default="SMART", description="Exchange (default SMART for US stocks)")
    currency: str = Field(default="USD", description="Currency (default USD)")
    expiry: Optional[str] = Field(default=None, description="Expiry date YYYYMMDD (required for OPT/FUT, e.g. '20260220')")
    strike: Optional[float] = Field(default=None, description="Strike price (required for OPT, e.g. 50)")
    right: Optional[str] = Field(default=None, description="Option right: 'C' for call, 'P' for put (required for OPT)")


class HistoricalDataInput(BaseModel):
    """Input for historical data."""
    model_config = ConfigDict(extra="forbid")
    symbol: str = Field(..., description="Ticker symbol", min_length=1, max_length=20)
    sec_type: str = Field(default="STK", description="Security type: STK, FX, OPT, FUT")
    exchange: str = Field(default="SMART", description="Exchange")
    currency: str = Field(default="USD", description="Currency")
    duration: str = Field(
        default="30 D",
        description="Duration string: e.g. '30 D', '6 M', '1 Y', '3600 S'"
    )
    bar_size: str = Field(
        default="1 day",
        description="Bar size: '1 secs','5 secs','1 min','5 mins','15 mins','30 mins','1 hour','4 hours','1 day','1 week','1 month'"
    )
    what_to_show: str = Field(
        default="TRADES",
        description="Data type: TRADES, MIDPOINT, BID, ASK, ADJUSTED_LAST, HISTORICAL_VOLATILITY, OPTION_IMPLIED_VOLATILITY"
    )
    use_rth: bool = Field(default=True, description="Regular trading hours only")


class OrderType(str, Enum):
    MARKET = "MKT"
    LIMIT = "LMT"
    STOP = "STP"
    STOP_LIMIT = "STP_LMT"


class OrderAction(str, Enum):
    BUY = "BUY"
    SELL = "SELL"


class PrepareOrderInput(BaseModel):
    """Input for preparing an order (step 1 of 2)."""
    model_config = ConfigDict(extra="forbid")
    symbol: str = Field(..., description="Ticker symbol", min_length=1, max_length=20)
    sec_type: str = Field(default="STK", description="Security type: STK, FX, OPT, FUT")
    exchange: str = Field(default="SMART", description="Exchange")
    currency: str = Field(default="USD", description="Currency")
    action: OrderAction = Field(..., description="BUY or SELL")
    quantity: float = Field(..., description="Number of shares/contracts", gt=0)
    order_type: OrderType = Field(..., description="Order type: MKT, LMT, STP, STP_LMT")
    limit_price: Optional[float] = Field(default=None, description="Limit price (required for LMT and STP_LMT)")
    stop_price: Optional[float] = Field(default=None, description="Stop price (required for STP and STP_LMT)")
    # Options-specific
    expiry: Optional[str] = Field(default=None, description="Expiry date YYYYMMDD (for OPT/FUT)")
    strike: Optional[float] = Field(default=None, description="Strike price (for OPT)")
    right: Optional[str] = Field(default=None, description="C or P (for OPT)")
    tif: str = Field(default="DAY", description="Time in force: DAY, GTC, IOC, GTD")
    outside_rth: bool = Field(default=False, description="Allow execution outside regular trading hours")

    @field_validator("limit_price")
    @classmethod
    def validate_limit(cls, v: Optional[float], info) -> Optional[float]:
        order_type = info.data.get("order_type")
        if order_type in (OrderType.LIMIT, OrderType.STOP_LIMIT) and v is None:
            raise ValueError(f"limit_price is required for {order_type} orders")
        return v

    @field_validator("stop_price")
    @classmethod
    def validate_stop(cls, v: Optional[float], info) -> Optional[float]:
        order_type = info.data.get("order_type")
        if order_type in (OrderType.STOP, OrderType.STOP_LIMIT) and v is None:
            raise ValueError(f"stop_price is required for {order_type} orders")
        return v


class SubmitOrderInput(BaseModel):
    """Input for submitting a prepared order (step 2 of 2)."""
    model_config = ConfigDict(extra="forbid")
    confirmation_token: str = Field(
        ...,
        description="The confirmation token received from ib_prepare_order. "
                    "Present this to the user and only submit after explicit confirmation.",
        min_length=12,
        max_length=12,
    )


class CancelOrderInput(BaseModel):
    """Input for cancelling an active order."""
    model_config = ConfigDict(extra="forbid")
    order_id: int = Field(..., description="The IB order ID to cancel")


class OpenOrdersInput(BaseModel):
    """Input for listing open orders."""
    model_config = ConfigDict(extra="forbid")
    symbol_filter: Optional[str] = Field(
        default=None,
        description="Filter by symbol (case-insensitive substring match)"
    )


class ContractDetailsInput(BaseModel):
    """Input for contract details lookup."""
    model_config = ConfigDict(extra="forbid")
    symbol: str = Field(..., description="Ticker symbol", min_length=1, max_length=20)
    sec_type: str = Field(default="STK", description="Security type: STK, FX, OPT, FUT")
    exchange: str = Field(default="SMART", description="Exchange")
    currency: str = Field(default="USD", description="Currency")
    expiry: Optional[str] = Field(default=None, description="Expiry YYYYMMDD (for OPT/FUT)")
    strike: Optional[float] = Field(default=None, description="Strike (for OPT)")
    right: Optional[str] = Field(default=None, description="C or P (for OPT)")


class OptionChainInput(BaseModel):
    """Input for option chain lookup."""
    model_config = ConfigDict(extra="forbid")
    symbol: str = Field(..., description="Underlying ticker symbol", min_length=1, max_length=20)
    exchange: str = Field(default="SMART", description="Exchange")


class ExecutionsInput(BaseModel):
    """Input for executions query."""
    model_config = ConfigDict(extra="forbid")
    since_days: int = Field(
        default=0,
        description="How many days back to fetch executions. "
                    "0 = today only (default), max 7. "
                    "Note: IB Gateway supports only today; TWS supports up to 7 days "
                    "(requires Trade Log setting in TWS).",
        ge=0,
        le=7,
    )
    symbol_filter: Optional[str] = Field(
        default=None,
        description="Filter executions by symbol (case-insensitive substring match)"
    )
    client_id_filter: Optional[int] = Field(
        default=None,
        description="Filter executions by client ID"
    )


class ScannerInput(BaseModel):
    """Input for market scanner."""
    model_config = ConfigDict(extra="forbid")
    scan_code: str = Field(
        ...,
        description="Scanner code: TOP_PERC_GAIN, TOP_PERC_LOSE, MOST_ACTIVE, "
                    "HOT_BY_VOLUME, TOP_TRADE_COUNT, TOP_PRICE_RANGE, "
                    "HIGH_OPT_VOLUME_PUT_CALL_RATIO, HOT_BY_OPT_VOLUME, "
                    "TOP_VOLUME_RATE, TOP_OPEN_PERC_GAIN, TOP_OPEN_PERC_LOSE, "
                    "HIGH_OPEN_GAP, LOW_OPEN_GAP, SCAN_socialSentiment_net"
    )
    instrument: str = Field(
        default="STK",
        description="Instrument type: STK, FUT, IND"
    )
    location_code: str = Field(
        default="STK.US.MAJOR",
        description="Location code: STK.US.MAJOR, STK.US, STK.US.MINOR, STK.EU, "
                    "STK.AMEX, STK.NYSE, STK.NASDAQ.NMS, FUT.US, IND.US"
    )
    number_of_rows: int = Field(
        default=20,
        description="Number of results to return (max 50)",
        ge=1,
        le=50,
    )
    above_price: Optional[float] = Field(
        default=None,
        description="Minimum price filter"
    )
    below_price: Optional[float] = Field(
        default=None,
        description="Maximum price filter"
    )
    above_volume: Optional[int] = Field(
        default=None,
        description="Minimum average daily volume filter"
    )
    market_cap_above: Optional[float] = Field(
        default=None,
        description="Minimum market cap filter (in USD, e.g. 1e9 for $1B)"
    )
    market_cap_below: Optional[float] = Field(
        default=None,
        description="Maximum market cap filter (in USD)"
    )


# ─── Tools ────────────────────────────────────────────────────────────────────

# === ACCOUNT & PORTFOLIO ===

@mcp.tool(
    name="ib_account_summary",
    annotations={
        "title": "IB Account Summary",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    },
)
async def ib_account_summary(params: AccountSummaryInput) -> str:
    """Get account summary including net liquidation, cash, margin, buying power, etc.

    Returns key account metrics. Use specific tags to filter, or leave empty for full summary.
    Common tags: NetLiquidation, TotalCashValue, GrossPositionValue, BuyingPower,
    AvailableFunds, MaintMarginReq, ExcessLiquidity, UnrealizedPnL, RealizedPnL.

    Returns:
        str: Formatted account summary with requested financial metrics.
    """
    try:
        ib = await _get_ib()
        await asyncio.sleep(1)

        # accountValues() reads auto-populated cache (no event loop conflict)
        summary = ib.accountValues()
        if not summary:
            return "No account summary data available. Ensure TWS is connected and logged in."

        # Filter by tags if specified
        if params.tags:
            tag_set = {t.lower() for t in params.tags}
            summary = [s for s in summary if s.tag.lower() in tag_set]
        else:
            # Default: show most useful tags
            default_tags = {
                "netliquidation", "totalcashvalue", "grosspositionvalue",
                "buyingpower", "availablefunds", "maintmarginreq",
                "excessliquidity", "unrealizedpnl", "realizedpnl",
            }
            summary = [s for s in summary if s.tag.lower() in default_tags]

        if not summary:
            return "No matching account data found for the specified tags."

        # Group by account
        accounts: Dict[str, List] = {}
        for item in summary:
            accounts.setdefault(item.account, []).append(item)

        lines = ["# Account Summary", ""]
        for account, items in accounts.items():
            lines.append(f"## Account: {account}")
            lines.append("")
            for item in sorted(items, key=lambda x: x.tag):
                lines.append(_format_account_value(item.tag, item.value, item.currency))
            lines.append("")

        return "\n".join(lines)

    except Exception as e:
        return f"Error getting account summary: {e}"


@mcp.tool(
    name="ib_positions",
    annotations={
        "title": "IB Portfolio Positions",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    },
)
async def ib_positions(params: PositionsInput) -> str:
    """Get all current portfolio positions with P&L.

    Returns position details including symbol, quantity, average cost, market value,
    and unrealized P&L for each position.

    Args:
        params: Optional symbol filter for narrowing results.

    Returns:
        str: Formatted list of portfolio positions with P&L metrics.
    """
    try:
        ib = await _get_ib()

        # Use async reqPositionsAsync to avoid event loop conflict
        positions = await ib.reqPositionsAsync()
        if not positions:
            return "No positions found."

        # Filter
        if params.symbol_filter:
            flt = params.symbol_filter.upper()
            positions = [p for p in positions if flt in p.contract.symbol.upper()]

        if not positions:
            return f"No positions matching '{params.symbol_filter}'."

        # Build conId → set of clientIds from available executions
        client_ids_by_con: Dict[int, set] = {}
        fills = await ib.reqExecutionsAsync()
        for fill in fills:
            con_id = fill.contract.conId
            client_id = fill.execution.clientId
            client_ids_by_con.setdefault(con_id, set()).add(client_id)

        lines = ["# Portfolio Positions", ""]

        for p in sorted(positions, key=lambda x: x.contract.symbol):
            c = p.contract
            symbol_info = c.symbol
            if c.secType == "OPT":
                symbol_info += f" {c.lastTradeDateOrContractMonth} {c.strike}{c.right}"
            elif c.secType == "FUT":
                symbol_info += f" {c.lastTradeDateOrContractMonth}"

            lines.append(f"## {symbol_info} ({c.secType})")
            lines.append(f"Position: {p.position:,.0f} | Avg cost: {_format_currency(p.avgCost, c.currency)}")

            cids = client_ids_by_con.get(c.conId)
            if cids:
                lines.append(f"Client IDs (today's executions): {', '.join(str(i) for i in sorted(cids))}")

            lines.append("")

        return "\n".join(lines)

    except Exception as e:
        return f"Error getting positions: {e}"


# === MARKET DATA ===

@mcp.tool(
    name="ib_market_data",
    annotations={
        "title": "IB Market Data Snapshot",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": False,
        "openWorldHint": True,
    },
)
async def ib_market_data(params: MarketDataInput) -> str:
    """Get a real-time market data snapshot for a given symbol.

    Returns current bid/ask, last price, volume, high/low, and other tick data.
    For options, also returns Greeks (IV, delta, gamma, theta, vega).

    Supports stocks, forex, options, and futures. For options, provide
    sec_type="OPT" with expiry, strike, and right parameters.

    Example option query: symbol="XLE", sec_type="OPT", expiry="20260220",
    strike=50, right="P"

    Args:
        params: Symbol and contract parameters.

    Returns:
        str: Formatted market data snapshot with current prices and volume.
    """
    try:
        ib = await _get_ib()
        contract = _contract_from_params(
            params.symbol, params.sec_type, params.exchange, params.currency,
            params.expiry, params.strike, params.right,
        )
        await ib.qualifyContractsAsync(contract)

        # reqTickersAsync handles snapshot lifecycle automatically (await + cleanup)
        tickers = await ib.reqTickersAsync(contract)
        ticker = tickers[0]

        def _tv(val) -> str:
            """Format tick value — handle NaN and IB's -1.0 sentinel."""
            if val != val:  # NaN check
                return "N/A"
            if val is None or val <= 0:
                return "N/A"
            return f"{val:,.2f}"

        def _tv_int(val) -> str:
            if val != val or val is None or val < 0:
                return "N/A"
            return f"{val:,.0f}"

        # Build title
        title = params.symbol
        if params.sec_type == "OPT" and params.expiry:
            title += f" {params.expiry} {params.strike}{params.right}"

        lines = [
            f"# Market Data: {title}",
            "",
            f"Last: {_tv(ticker.last)}",
            f"Bid: {_tv(ticker.bid)} x {_tv_int(ticker.bidSize)}",
            f"Ask: {_tv(ticker.ask)} x {_tv_int(ticker.askSize)}",
            f"High: {_tv(ticker.high)}",
            f"Low: {_tv(ticker.low)}",
            f"Open: {_tv(ticker.open)}",
            f"Close: {_tv(ticker.close)}",
            f"Volume: {_tv_int(ticker.volume)}",
        ]

        # Add Greeks for options
        if params.sec_type == "OPT":
            greeks = ticker.modelGreeks or ticker.lastGreeks
            if greeks:
                lines.extend([
                    "",
                    "## Greeks",
                    f"IV: {greeks.impliedVol:.4f}" if greeks.impliedVol and greeks.impliedVol == greeks.impliedVol else "IV: N/A",
                    f"Delta: {greeks.delta:.4f}" if greeks.delta and greeks.delta == greeks.delta else "Delta: N/A",
                    f"Gamma: {greeks.gamma:.4f}" if greeks.gamma and greeks.gamma == greeks.gamma else "Gamma: N/A",
                    f"Theta: {greeks.theta:.4f}" if greeks.theta and greeks.theta == greeks.theta else "Theta: N/A",
                    f"Vega: {greeks.vega:.4f}" if greeks.vega and greeks.vega == greeks.vega else "Vega: N/A",
                ])
            else:
                lines.extend(["", "Greeks: not available (market may be closed)"])

        return "\n".join(lines)

    except Exception as e:
        return f"Error getting market data for {params.symbol}: {e}"


@mcp.tool(
    name="ib_historical_data",
    annotations={
        "title": "IB Historical Data",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True,
    },
)
async def ib_historical_data(params: HistoricalDataInput) -> str:
    """Get historical OHLCV bars for a symbol.

    Returns historical price data as OHLCV bars. Useful for analysis, charting,
    and strategy evaluation.

    Args:
        params: Symbol, duration, bar size, and data type parameters.

    Returns:
        str: JSON array of historical bars with date, open, high, low, close, volume.
    """
    try:
        ib = await _get_ib()
        contract = _contract_from_params(
            params.symbol, params.sec_type, params.exchange, params.currency
        )
        await ib.qualifyContractsAsync(contract)

        bars = await ib.reqHistoricalDataAsync(
            contract,
            endDateTime="",
            durationStr=params.duration,
            barSizeSetting=params.bar_size,
            whatToShow=params.what_to_show,
            useRTH=params.use_rth,
            formatDate=1,
        )

        if not bars:
            return f"No historical data returned for {params.symbol}."

        # Convert to list of dicts
        data = []
        for bar in bars:
            data.append({
                "date": str(bar.date),
                "open": bar.open,
                "high": bar.high,
                "low": bar.low,
                "close": bar.close,
                "volume": int(bar.volume) if bar.volume == bar.volume else 0,
                "average": bar.average if hasattr(bar, "average") and bar.average == bar.average else None,
                "barCount": bar.barCount if hasattr(bar, "barCount") and bar.barCount == bar.barCount else None,
            })

        summary = {
            "symbol": params.symbol,
            "duration": params.duration,
            "bar_size": params.bar_size,
            "bars_count": len(data),
            "date_range": f"{data[0]['date']} → {data[-1]['date']}",
            "bars": data,
        }

        return json.dumps(summary, indent=2, default=str)

    except Exception as e:
        return f"Error getting historical data for {params.symbol}: {e}"


# === MARKET SCANNER ===

@mcp.tool(
    name="ib_scanner",
    annotations={
        "title": "IB Market Scanner",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": False,
        "openWorldHint": True,
    },
)
async def ib_scanner(params: ScannerInput) -> str:
    """Run a market scanner to find stocks by criteria (top gainers, losers, most active, etc.).

    Returns a ranked list of instruments matching the scanner criteria. Useful for
    finding trading opportunities, screening for unusual activity, or monitoring
    market movers.

    Common scan codes:
    - TOP_PERC_GAIN / TOP_PERC_LOSE — top % gainers / losers
    - MOST_ACTIVE — highest volume
    - HOT_BY_VOLUME — unusual volume spike
    - TOP_TRADE_COUNT — most trades
    - TOP_PRICE_RANGE — largest intraday range
    - HIGH_OPT_VOLUME_PUT_CALL_RATIO — high options put/call ratio

    Args:
        params: Scanner parameters including scan code, instrument type, location, and filters.

    Returns:
        str: Markdown table with ranked scanner results.
    """
    try:
        ib = await _get_ib()

        sub = ScannerSubscription(
            instrument=params.instrument,
            locationCode=params.location_code,
            scanCode=params.scan_code,
            numberOfRows=params.number_of_rows,
        )

        if params.above_price is not None:
            sub.abovePrice = params.above_price
        if params.below_price is not None:
            sub.belowPrice = params.below_price
        if params.above_volume is not None:
            sub.aboveVolume = params.above_volume
        if params.market_cap_above is not None:
            sub.marketCapAbove = params.market_cap_above
        if params.market_cap_below is not None:
            sub.marketCapBelow = params.market_cap_below

        results = await ib.reqScannerDataAsync(sub, [])

        if not results:
            return f"No results for scanner '{params.scan_code}' at {params.location_code}."

        lines = [
            f"# Scanner: {params.scan_code}",
            f"Location: {params.location_code} | Instrument: {params.instrument}",
            "",
            "| Rank | Symbol | Name | Type |",
            "|------|--------|------|------|",
        ]

        for item in results:
            c = item.contractDetails.contract
            name = item.contractDetails.longName or ""
            rank = item.rank + 1  # IB ranks are 0-based
            lines.append(f"| {rank} | {c.symbol} | {name} | {c.secType} |")

        lines.append("")
        lines.append(f"*{len(results)} results returned*")

        return "\n".join(lines)

    except Exception as e:
        return f"Error running scanner '{params.scan_code}': {e}"


# === CONTRACT INFO ===

@mcp.tool(
    name="ib_contract_details",
    annotations={
        "title": "IB Contract Details",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True,
    },
)
async def ib_contract_details(params: ContractDetailsInput) -> str:
    """Look up full contract details for a symbol.

    Returns detailed contract information including trading hours, min tick,
    multiplier, and valid exchanges.

    Args:
        params: Symbol and contract type parameters.

    Returns:
        str: Formatted contract details.
    """
    try:
        ib = await _get_ib()
        contract = _contract_from_params(
            params.symbol, params.sec_type, params.exchange, params.currency,
            params.expiry, params.strike, params.right,
        )

        details_list = await ib.reqContractDetailsAsync(contract)
        if not details_list:
            return f"No contract details found for {params.symbol}."

        lines = [f"# Contract Details: {params.symbol}", ""]

        for i, det in enumerate(details_list[:10]):  # limit to 10
            c = det.contract
            lines.append(f"## {c.symbol} ({c.secType}) - {c.exchange}")
            lines.append(f"ConId: {c.conId}")
            lines.append(f"Currency: {c.currency}")
            lines.append(f"Long name: {det.longName}")
            if c.secType in ("OPT", "FUT"):
                lines.append(f"Expiry: {c.lastTradeDateOrContractMonth}")
            if c.secType == "OPT":
                lines.append(f"Strike: {c.strike} | Right: {c.right}")
            if det.minTick:
                lines.append(f"Min tick: {det.minTick}")
            if det.contractMonth:
                lines.append(f"Contract month: {det.contractMonth}")
            lines.append("")

        if len(details_list) > 10:
            lines.append(f"... and {len(details_list) - 10} more contracts")

        return "\n".join(lines)

    except Exception as e:
        return f"Error getting contract details: {e}"


@mcp.tool(
    name="ib_option_chains",
    annotations={
        "title": "IB Option Chain",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": True,
    },
)
async def ib_option_chains(params: OptionChainInput) -> str:
    """Get available option chain expirations and strikes for an underlying.

    Returns all available expiry dates and exchanges for the option chain.

    Args:
        params: Underlying symbol and exchange.

    Returns:
        str: Option chain overview with available expirations.
    """
    try:
        ib = await _get_ib()
        stock = Stock(params.symbol, params.exchange, "USD")
        await ib.qualifyContractsAsync(stock)

        chains = await ib.reqSecDefOptParamsAsync(stock.symbol, "", stock.secType, stock.conId)
        if not chains:
            return f"No option chains found for {params.symbol}."

        lines = [f"# Option Chains: {params.symbol}", ""]

        for chain in chains:
            lines.append(f"## Exchange: {chain.exchange}")
            lines.append(f"Trading class: {chain.tradingClass}")
            lines.append(f"Multiplier: {chain.multiplier}")

            expirations = sorted(chain.expirations)
            lines.append(f"Expirations ({len(expirations)}): {', '.join(expirations[:20])}")
            if len(expirations) > 20:
                lines.append(f"  ... and {len(expirations) - 20} more")

            strikes = sorted(chain.strikes)
            lines.append(f"Strikes ({len(strikes)}): {strikes[0]} to {strikes[-1]}")
            lines.append("")

        return "\n".join(lines)

    except Exception as e:
        return f"Error getting option chains: {e}"


# === ORDER MANAGEMENT ===

@mcp.tool(
    name="ib_open_orders",
    annotations={
        "title": "IB Open Orders",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    },
)
async def ib_open_orders(params: OpenOrdersInput) -> str:
    """List all currently open/pending orders.

    Returns details of all active orders including order type, status,
    quantity, and prices.

    Args:
        params: Optional symbol filter.

    Returns:
        str: Formatted list of open orders.
    """
    try:
        ib = await _get_ib()
        await asyncio.sleep(0.5)

        trades = ib.openTrades()
        if not trades:
            return "No open orders."

        if params.symbol_filter:
            flt = params.symbol_filter.upper()
            trades = [t for t in trades if flt in t.contract.symbol.upper()]

        if not trades:
            return f"No open orders matching '{params.symbol_filter}'."

        lines = ["# Open Orders", ""]

        for t in trades:
            c = t.contract
            o = t.order
            s = t.orderStatus

            symbol_info = c.symbol
            if c.secType == "OPT":
                symbol_info += f" {c.lastTradeDateOrContractMonth} {c.strike}{c.right}"

            lines.append(f"## Order #{o.orderId}: {o.action} {o.totalQuantity} {symbol_info}")
            lines.append(f"Type: {o.orderType} | TIF: {o.tif}")
            if o.lmtPrice:
                lines.append(f"Limit: {o.lmtPrice}")
            if o.auxPrice:
                lines.append(f"Stop/Aux: {o.auxPrice}")
            lines.append(f"Status: {s.status} | Filled: {s.filled}/{o.totalQuantity}")
            if s.avgFillPrice:
                lines.append(f"Avg fill: {s.avgFillPrice}")
            lines.append("")

        return "\n".join(lines)

    except Exception as e:
        return f"Error getting open orders: {e}"


@mcp.tool(
    name="ib_prepare_order",
    annotations={
        "title": "IB Prepare Order (Step 1/2)",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    },
)
async def ib_prepare_order(params: PrepareOrderInput) -> str:
    """Prepare and validate an order WITHOUT submitting it. Step 1 of 2.

    This creates a pending order and returns a confirmation token.
    The order will NOT be placed until ib_submit_order is called with the token.
    Always show the order details to the user and wait for explicit confirmation
    before calling ib_submit_order.

    ⚠️ IMPORTANT: Do NOT call ib_submit_order without the user explicitly confirming.

    Args:
        params: Full order specification (symbol, action, quantity, type, prices).

    Returns:
        str: Order summary with confirmation token for user review.
    """
    try:
        _cleanup_expired_pending()

        # Validate contract
        ib = await _get_ib()
        contract = _contract_from_params(
            params.symbol, params.sec_type, params.exchange, params.currency,
            params.expiry, params.strike, params.right,
        )
        qualified = await ib.qualifyContractsAsync(contract)
        if not qualified:
            return f"Error: Could not qualify contract for {params.symbol}. Check symbol and parameters."

        # Build order details
        order_details = {
            "symbol": params.symbol,
            "sec_type": params.sec_type,
            "exchange": params.exchange,
            "currency": params.currency,
            "action": params.action.value,
            "quantity": params.quantity,
            "order_type": params.order_type.value,
            "limit_price": params.limit_price,
            "stop_price": params.stop_price,
            "expiry": params.expiry,
            "strike": params.strike,
            "right": params.right,
            "tif": params.tif,
            "outside_rth": params.outside_rth,
            "con_id": contract.conId,
        }

        token = _generate_confirmation_token(order_details)

        _pending_orders[token] = {
            "order_details": order_details,
            "contract": contract,
            "created_at": time.time(),
        }

        # Format for user review
        lines = [
            "# ⚠️ Order Prepared — Awaiting Confirmation",
            "",
            f"**Action:** {params.action.value} {params.quantity:,.0f} x {params.symbol}",
            f"**Type:** {params.order_type.value}",
        ]

        if params.limit_price is not None:
            lines.append(f"**Limit price:** {params.limit_price}")
        if params.stop_price is not None:
            lines.append(f"**Stop price:** {params.stop_price}")

        lines.extend([
            f"**TIF:** {params.tif}",
            f"**Outside RTH:** {'Yes' if params.outside_rth else 'No'}",
            f"**Exchange:** {params.exchange}",
            f"**Currency:** {params.currency}",
            "",
            f"**Confirmation token:** `{token}`",
            "",
            "→ Ask the user to confirm this order before submitting.",
            f"→ Token expires in {_PENDING_TTL // 60} minutes.",
        ])

        return "\n".join(lines)

    except Exception as e:
        return f"Error preparing order: {e}"


@mcp.tool(
    name="ib_submit_order",
    annotations={
        "title": "IB Submit Order (Step 2/2)",
        "readOnlyHint": False,
        "destructiveHint": True,
        "idempotentHint": False,
        "openWorldHint": False,
    },
)
async def ib_submit_order(params: SubmitOrderInput) -> str:
    """Submit a previously prepared order. Step 2 of 2.

    ⚠️ CRITICAL: Only call this AFTER the user has explicitly confirmed the order.
    Uses the confirmation token from ib_prepare_order.

    Args:
        params: The confirmation token from ib_prepare_order.

    Returns:
        str: Order submission result with order ID and status.
    """
    try:
        _cleanup_expired_pending()

        token = params.confirmation_token
        if token not in _pending_orders:
            return (
                "Error: Invalid or expired confirmation token. "
                "Please use ib_prepare_order to create a new order."
            )

        pending = _pending_orders.pop(token)
        details = pending["order_details"]
        contract = pending["contract"]

        # Create the IB order object
        order_type = details["order_type"]
        if order_type == "MKT":
            order = MarketOrder(details["action"], details["quantity"])
        elif order_type == "LMT":
            order = LimitOrder(details["action"], details["quantity"], details["limit_price"])
        elif order_type == "STP":
            order = StopOrder(details["action"], details["quantity"], details["stop_price"])
        elif order_type == "STP_LMT":
            order = StopLimitOrder(
                details["action"], details["quantity"],
                details["limit_price"], details["stop_price"]
            )
        else:
            return f"Error: Unsupported order type '{order_type}'"

        order.tif = details["tif"]
        order.outsideRth = details["outside_rth"]

        # Place the order
        ib = await _get_ib()
        trade = ib.placeOrder(contract, order)
        await asyncio.sleep(1)  # wait for initial status

        status = trade.orderStatus

        lines = [
            "# ✅ Order Submitted",
            "",
            f"**Order ID:** {trade.order.orderId}",
            f"**Action:** {details['action']} {details['quantity']:,.0f} x {details['symbol']}",
            f"**Type:** {details['order_type']}",
            f"**Status:** {status.status}",
        ]

        if status.filled > 0:
            lines.append(f"**Filled:** {status.filled} @ {status.avgFillPrice}")

        return "\n".join(lines)

    except Exception as e:
        return f"Error submitting order: {e}"


@mcp.tool(
    name="ib_cancel_order",
    annotations={
        "title": "IB Cancel Order",
        "readOnlyHint": False,
        "destructiveHint": True,
        "idempotentHint": False,
        "openWorldHint": False,
    },
)
async def ib_cancel_order(params: CancelOrderInput) -> str:
    """Cancel an active order by order ID.

    ⚠️ Only cancel orders after user confirmation.

    Args:
        params: The IB order ID to cancel.

    Returns:
        str: Cancellation result.
    """
    try:
        ib = await _get_ib()
        trades = ib.openTrades()

        target = None
        for t in trades:
            if t.order.orderId == params.order_id:
                target = t
                break

        if not target:
            return f"Error: No open order found with ID {params.order_id}."

        ib.cancelOrder(target.order)
        await asyncio.sleep(1)

        return (
            f"# Order #{params.order_id} cancelled\n\n"
            f"**Symbol:** {target.contract.symbol}\n"
            f"**Action:** {target.order.action} {target.order.totalQuantity}\n"
            f"**Status:** {target.orderStatus.status}"
        )

    except Exception as e:
        return f"Error cancelling order: {e}"


# === EXECUTION HISTORY ===

@mcp.tool(
    name="ib_executions",
    annotations={
        "title": "IB Recent Executions",
        "readOnlyHint": True,
        "destructiveHint": False,
        "idempotentHint": True,
        "openWorldHint": False,
    },
)
async def ib_executions(params: ExecutionsInput) -> str:
    """Get execution/fill reports, optionally filtered by time period, symbol, or client ID.

    Returns fills including prices, quantities, commissions, P&L, and client IDs.
    By default returns today's executions. Use since_days to go back up to 7 days
    (requires TWS with Trade Log configured; IB Gateway only supports current day).

    Args:
        params: Optional filters — since_days, symbol_filter, client_id_filter.

    Returns:
        str: Formatted list of trade executions.
    """
    try:
        ib = await _get_ib()

        exec_filter = ExecutionFilter()

        if params.since_days > 0:
            since = datetime.now() - timedelta(days=params.since_days)
            exec_filter.time = since.strftime("%Y%m%d 00:00:00")

        if params.client_id_filter is not None:
            exec_filter.clientId = params.client_id_filter

        # Request executions to refresh the cache, then read from fills()
        # which has commission reports properly matched to executions
        await ib.reqExecutionsAsync(exec_filter)
        await asyncio.sleep(1)

        fills = ib.fills()
        if not fills:
            period = f"last {params.since_days} days" if params.since_days > 0 else "today"
            return f"No executions for {period}."

        # Apply time filter on cached fills (reqExecutionsAsync filters server-side
        # but fills() returns full cache — re-filter if since_days was specified)
        if params.since_days > 0:
            since = datetime.now() - timedelta(days=params.since_days)
            fills = [f for f in fills if f.execution.time >= since]

        # Apply client ID filter
        if params.client_id_filter is not None:
            fills = [f for f in fills if f.execution.clientId == params.client_id_filter]

        # Apply symbol filter
        if params.symbol_filter:
            flt = params.symbol_filter.upper()
            fills = [f for f in fills if flt in f.contract.symbol.upper()]

        if not fills:
            period = f"last {params.since_days} days" if params.since_days > 0 else "today"
            return f"No executions matching filters for {period}."

        period = f"Last {params.since_days} days" if params.since_days > 0 else "Today"
        lines = [f"# Executions — {period}", ""]

        total_commission = 0.0
        for fill in fills:
            c = fill.contract
            e = fill.execution
            comm = fill.commissionReport

            symbol_info = c.symbol
            if c.secType == "OPT":
                symbol_info += f" {c.lastTradeDateOrContractMonth} {c.strike}{c.right}"

            lines.append(f"## {e.side} {e.shares:,.0f} x {symbol_info}")
            lines.append(f"Price: {e.price} | Time: {e.time}")
            lines.append(f"Exchange: {e.exchange} | Order ID: {e.orderId} | Client ID: {e.clientId}")
            if comm and comm.commission < 1e9:  # valid commission
                lines.append(f"Commission: {_format_currency(comm.commission, comm.currency)}")
                if comm.realizedPNL < 1e9:
                    lines.append(f"Realized P&L: {_format_currency(comm.realizedPNL, comm.currency)}")
                total_commission += comm.commission
            lines.append("")

        lines.append("---")
        lines.append(f"**Total commissions: {_format_currency(total_commission)}**")
        lines.append(f"**Total fills: {len(fills)}**")

        return "\n".join(lines)

    except Exception as e:
        return f"Error getting executions: {e}"


# ─── Entry Point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    mcp.run()
