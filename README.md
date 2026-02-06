# claude-tws-connect

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![Sponsor](https://img.shields.io/badge/Sponsor-%E2%9D%A4-pink?logo=github)](https://github.com/sponsors/danielkristofik)

MCP (Model Context Protocol) server that connects [Claude](https://claude.ai) to [Interactive Brokers TWS](https://www.interactivebrokers.com/en/trading/tws.php) via `ib_insync`. Use natural language to check your portfolio, pull market data, and place orders — all from Claude Desktop, Claude Code, or any MCP-compatible client.

## Quick Start (macOS)

```bash
git clone https://github.com/danielkristofik/mcp_claude_ibkr.git
cd mcp_claude_ibkr
./install.sh
```

The installer will:
1. Find or install Python 3.10+
2. Create a virtual environment and install dependencies
3. Let you choose Paper or Live trading port
4. Configure Claude Desktop automatically

## Features

| Tool | Description | Read-only |
|---|---|---|
| `ib_account_summary` | Account overview — liquidation value, cash, margin, buying power | Yes |
| `ib_positions` | Portfolio positions with P&L | Yes |
| `ib_market_data` | Real-time snapshot (bid/ask/last/volume) | Yes |
| `ib_historical_data` | Historical OHLCV bars | Yes |
| `ib_contract_details` | Contract details | Yes |
| `ib_option_chains` | Option expirations and strikes | Yes |
| `ib_open_orders` | Active orders | Yes |
| `ib_executions` | Today's executions / fills | Yes |
| `ib_prepare_order` | Prepare an order for review (step 1/2) | Yes |
| `ib_submit_order` | Submit a confirmed order (step 2/2) | **No** |
| `ib_cancel_order` | Cancel an active order | **No** |

### Order Safety

Orders use a **two-step confirmation flow**:

1. `ib_prepare_order` — validates the contract, builds the order, and returns a **confirmation token**
2. Claude shows the order details to you and waits for explicit approval
3. `ib_submit_order` — submits the order only after confirmation (token expires in 5 minutes)

## Manual Installation

### Prerequisites

- Python 3.10+
- TWS or IB Gateway running and logged in
- TWS API enabled (Edit → Global Configuration → API → Settings)

### Setup

```bash
git clone https://github.com/danielkristofik/mcp_claude_ibkr.git
cd mcp_claude_ibkr

# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Claude Desktop Configuration

Edit `~/Library/Application Support/Claude/claude_desktop_config.json` (macOS) and add the IBKR server:

```json
{
  "mcpServers": {
    "ibkr": {
      "command": "/FULL/PATH/TO/claude-tws-connect/venv/bin/python",
      "args": ["/FULL/PATH/TO/claude-tws-connect/ibkr_mcp.py"]
    }
  }
}
```

> **Important:** Use the full absolute path to the Python binary inside the venv.

See [`claude_desktop_config_example.json`](claude_desktop_config_example.json) for a complete example.

### Verify

Restart Claude Desktop. The `ib_*` tools should appear in the tools list. Try:

```
"Show me my account summary"
"What positions do I have?"
"Get historical data for AAPL for the last month"
```

## Configuration

Edit `config.json` or the variables at the top of `ibkr_mcp.py`:

```python
TWS_HOST = "127.0.0.1"   # TWS host
TWS_PORT = 7496           # 7496 = live, 7497 = paper trading
CLIENT_ID = 10            # Unique client ID (avoid conflicts with other TWS connections)
```

### TWS Setup

In TWS: **Edit → Global Configuration → API → Settings**:
- Enable ActiveX and Socket Clients
- Socket port: **7496** (live) or **7497** (paper)
- Uncheck "Read-Only API" if you want to place orders
- Add `127.0.0.1` to Trusted IPs

### Remote IB Gateway

To connect to a remote IB Gateway, use an SSH tunnel:

```bash
ssh -L 4002:127.0.0.1:4002 user@remote-server
```

Then change `TWS_PORT` to `4002` in your config.

## Troubleshooting

| Problem | Solution |
|---|---|
| Connection refused | Make sure TWS is running and API is enabled |
| Port conflict | Run `lsof -i :7496` to check if something else is using the port |
| No data | A market data subscription is required for the given contract |
| Timeout | Increase `CONNECT_TIMEOUT` or check your network connection |
| Token expired | Re-run `ib_prepare_order` — tokens are valid for 5 minutes |

## Support

If you find this project useful, consider supporting its development:

- [GitHub Sponsors](https://github.com/sponsors/danielkristofik)
- [Buy Me a Coffee](https://buymeacoffee.com/PLACEHOLDER) *(link coming soon)*
- [Ko-fi](https://ko-fi.com/PLACEHOLDER) *(link coming soon)*

## Disclaimer

This software is provided for **educational and informational purposes only**. It is **not financial advice**. Trading securities involves significant risk and may result in the loss of your invested capital.

The authors and contributors are **not responsible** for any financial losses, damages, or other consequences resulting from the use of this software. **Use at your own risk.** Always do your own research and consult a qualified financial advisor before making trading decisions.

This project is not affiliated with, endorsed by, or connected to Interactive Brokers LLC.

## License

[MIT](LICENSE) — Copyright (c) 2025 Daniel Kristofik
