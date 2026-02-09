# Roadmap — claude-tws-connect

## A. New Order Types

| Type | Description | Use Case |
|------|-------------|----------|
| Trailing Stop | Stop price follows the stock price automatically | Protect profits without manual adjustments |
| Bracket Order | Entry + Take Profit + Stop Loss in a single order | Complete risk management in one step |
| OCO (One-Cancels-Other) | When one order fills, the other is cancelled | TP/SL pairs |

## B. New Analysis Tools

| Tool | Description | Status |
|------|-------------|--------|
| `ib_scanner` | Market scanners — top gainers, losers, most active, highest volume | ✅ Done |
| `ib_fundamental_data` | P/E, EPS, dividends, market cap — basic fundamentals | ✅ Done |
| `ib_pnl` | Real-time daily P&L for the account | ✅ Done |
| `ib_margin_impact` | Margin requirement estimate before placing an order | ✅ Done |

## C. Options & Multi-Leg Strategies

| Tool | Description |
|------|-------------|
| `ib_option_greeks` | Greeks for an entire option chain (not just a single contract) |
| `ib_combo_order` | Multi-leg orders — vertical spreads, straddles, iron condors |
| `ib_option_analysis` | IV rank, IV percentile, put/call ratio for an underlying |

## D. Improvements to Existing Tools

| Change | Description |
|--------|-------------|
| `ib_historical_data` as table | Return markdown table in addition to JSON for readability |
| `ib_positions` with market value | Add current market price and total position value | ✅ Done |
| `ib_executions` summary | Group executions by symbol, show total daily P&L |
| `ib_modify_order` | Change price/quantity of an existing order without cancel + re-create |

## E. Portfolio Management

| Tool | Description |
|------|-------------|
| `ib_portfolio_risk` | Beta, sector exposure, concentration risk |
| `ib_portfolio_greeks` | Aggregated Greeks across the entire portfolio (delta, gamma exposure) |
| `ib_currency_exposure` | Positions grouped by currency (important for multi-currency accounts) |

## F. Automation & Notifications

| Tool | Description |
|------|-------------|
| `ib_price_alert` | Set an alert when price crosses a level |
| `ib_conditional_order` | Order conditioned on another instrument's price |
| `ib_watchlist` | Monitor a group of symbols, display prices at once |

## Suggested Priority

1. **`ib_scanner`** — highest added value, low complexity, Claude can analyze results directly
2. **Trailing Stop + Bracket Order** — most requested order types in practice
3. **`ib_modify_order`** — missing basic functionality
4. **`ib_positions` with market value** — small change, big improvement in usefulness
5. **`ib_combo_order`** — opens the door to option strategies
