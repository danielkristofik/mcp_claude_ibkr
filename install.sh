#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# install.sh — macOS installer for claude-tws-connect (IBKR MCP Server)
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REQUIRED_PYTHON_MAJOR=3
REQUIRED_PYTHON_MINOR=10

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m' # No Color

info()    { echo -e "${BLUE}ℹ ${NC}$*"; }
success() { echo -e "${GREEN}✔ ${NC}$*"; }
warn()    { echo -e "${YELLOW}⚠ ${NC}$*"; }
error()   { echo -e "${RED}✖ ${NC}$*"; }

# ─── 1. Detect Python 3.10+ ─────────────────────────────────────────────────

check_python_version() {
    local py="$1"
    local version
    version=$("$py" -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')" 2>/dev/null) || return 1
    local major minor
    major=$(echo "$version" | cut -d. -f1)
    minor=$(echo "$version" | cut -d. -f2)
    if [[ "$major" -eq "$REQUIRED_PYTHON_MAJOR" && "$minor" -ge "$REQUIRED_PYTHON_MINOR" ]]; then
        echo "$version"
        return 0
    fi
    return 1
}

find_python() {
    echo -e "\n${BOLD}── Step 1/4: Checking Python ──${NC}\n"

    # 1) Check python3 in PATH
    if command -v python3 &>/dev/null; then
        local ver
        if ver=$(check_python_version python3); then
            PYTHON="$(command -v python3)"
            success "Found python3 $ver at $PYTHON"
            return 0
        fi
    fi

    # 2) Check Homebrew locations
    local brew_paths=(/opt/homebrew/bin /usr/local/bin)
    for dir in "${brew_paths[@]}"; do
        for py in "$dir"/python3.{14,13,12,11,10}; do
            if [[ -x "$py" ]]; then
                local ver
                if ver=$(check_python_version "$py"); then
                    PYTHON="$py"
                    success "Found Python $ver at $PYTHON"
                    return 0
                fi
            fi
        done
    done

    # 3) Not found — offer Homebrew install
    warn "Python ${REQUIRED_PYTHON_MAJOR}.${REQUIRED_PYTHON_MINOR}+ not found."
    echo ""

    if command -v brew &>/dev/null; then
        read -rp "$(echo -e "${YELLOW}Install Python 3.12 via Homebrew? [Y/n] ${NC}")" answer
        answer="${answer:-Y}"
        if [[ "$answer" =~ ^[Yy]$ ]]; then
            info "Running: brew install python@3.12"
            brew install python@3.12
            # Find the newly installed python
            for py in /opt/homebrew/bin/python3.12 /usr/local/bin/python3.12 python3.12 python3; do
                if command -v "$py" &>/dev/null; then
                    local ver
                    if ver=$(check_python_version "$py"); then
                        PYTHON="$(command -v "$py")"
                        success "Installed Python $ver at $PYTHON"
                        return 0
                    fi
                fi
            done
            error "Python install succeeded but could not find the binary. Please restart your terminal and re-run this script."
            exit 1
        else
            error "Python ${REQUIRED_PYTHON_MAJOR}.${REQUIRED_PYTHON_MINOR}+ is required. Aborting."
            exit 1
        fi
    else
        error "Homebrew not found. Please install Python ${REQUIRED_PYTHON_MAJOR}.${REQUIRED_PYTHON_MINOR}+ manually:"
        echo "  Option A: brew install python@3.12  (install Homebrew first from https://brew.sh)"
        echo "  Option B: Download from https://www.python.org/downloads/"
        exit 1
    fi
}

# ─── 2. Create venv & install dependencies ───────────────────────────────────

setup_venv() {
    echo -e "\n${BOLD}── Step 2/4: Setting up virtual environment ──${NC}\n"

    local venv_dir="$SCRIPT_DIR/venv"

    if [[ -d "$venv_dir" ]]; then
        warn "venv already exists at $venv_dir"
        read -rp "$(echo -e "${YELLOW}Recreate it? [y/N] ${NC}")" answer
        answer="${answer:-N}"
        if [[ "$answer" =~ ^[Yy]$ ]]; then
            info "Removing old venv..."
            rm -rf "$venv_dir"
        else
            info "Keeping existing venv."
        fi
    fi

    if [[ ! -d "$venv_dir" ]]; then
        info "Creating virtual environment..."
        "$PYTHON" -m venv "$venv_dir"
        success "venv created at $venv_dir"
    fi

    info "Installing dependencies from requirements.txt..."
    "$venv_dir/bin/pip" install --upgrade pip --quiet
    "$venv_dir/bin/pip" install -r "$SCRIPT_DIR/requirements.txt" --quiet
    success "Dependencies installed."
}

# ─── 3. Select TWS port ──────────────────────────────────────────────────────

select_tws_port() {
    echo -e "\n${BOLD}── Step 3/4: TWS Connection ──${NC}\n"

    echo "Which TWS mode will you use?"
    echo ""
    echo "  1) Paper Trading (port 7497) — recommended for testing"
    echo "  2) Live Trading  (port 7496)"
    echo ""

    local port
    while true; do
        read -rp "$(echo -e "${YELLOW}Select [1/2]: ${NC}")" choice
        case "$choice" in
            1) port=7497; break ;;
            2) port=7496; break ;;
            *) warn "Please enter 1 or 2." ;;
        esac
    done

    TWS_PORT="$port"
    success "TWS port set to $TWS_PORT"

    # Update TWS_PORT in ibkr_mcp.py
    local mcp_file="$SCRIPT_DIR/ibkr_mcp.py"
    if [[ -f "$mcp_file" ]]; then
        sed -i '' "s/^TWS_PORT = [0-9]*/TWS_PORT = $TWS_PORT/" "$mcp_file"
        success "Updated TWS_PORT in ibkr_mcp.py"
    fi

    # Update tws_port in config.json
    local config_file="$SCRIPT_DIR/config.json"
    if [[ -f "$config_file" ]]; then
        "$SCRIPT_DIR/venv/bin/python" -c "
import json, sys
path = sys.argv[1]
port = int(sys.argv[2])
with open(path) as f:
    cfg = json.load(f)
cfg['tws_port'] = port
with open(path, 'w') as f:
    json.dump(cfg, f, indent=2)
    f.write('\n')
" "$config_file" "$TWS_PORT"
        success "Updated tws_port in config.json"
    fi
}

# ─── 4. Configure Claude Desktop ─────────────────────────────────────────────

configure_claude_desktop() {
    echo -e "\n${BOLD}── Step 4/4: Claude Desktop configuration ──${NC}\n"

    local config_dir="$HOME/Library/Application Support/Claude"
    local config_file="$config_dir/claude_desktop_config.json"
    local venv_python="$SCRIPT_DIR/venv/bin/python"
    local mcp_script="$SCRIPT_DIR/ibkr_mcp.py"

    # Ensure config directory exists
    mkdir -p "$config_dir"

    if [[ ! -f "$config_file" ]]; then
        # Create new config
        info "Creating new Claude Desktop config..."
        "$venv_python" -c "
import json, sys
venv_py = sys.argv[1]
mcp_py  = sys.argv[2]
config = {
    'mcpServers': {
        'ibkr': {
            'command': venv_py,
            'args': [mcp_py],
            'env': {}
        }
    }
}
with open(sys.argv[3], 'w') as f:
    json.dump(config, f, indent=2)
    f.write('\n')
" "$venv_python" "$mcp_script" "$config_file"
        success "Created $config_file"
    else
        # Merge into existing config
        info "Updating existing Claude Desktop config..."
        "$venv_python" -c "
import json, sys
venv_py = sys.argv[1]
mcp_py  = sys.argv[2]
config_path = sys.argv[3]

with open(config_path) as f:
    config = json.load(f)

if 'mcpServers' not in config:
    config['mcpServers'] = {}

config['mcpServers']['ibkr'] = {
    'command': venv_py,
    'args': [mcp_py],
    'env': {}
}

with open(config_path, 'w') as f:
    json.dump(config, f, indent=2)
    f.write('\n')
" "$venv_python" "$mcp_script" "$config_file"
        success "Updated $config_file (existing servers preserved)"
    fi
}

# ─── 5. Summary ──────────────────────────────────────────────────────────────

print_summary() {
    echo ""
    echo -e "${GREEN}${BOLD}════════════════════════════════════════════════${NC}"
    echo -e "${GREEN}${BOLD}  Installation complete!${NC}"
    echo -e "${GREEN}${BOLD}════════════════════════════════════════════════${NC}"
    echo ""
    echo -e "  Python:     ${BOLD}$PYTHON${NC}"
    echo -e "  venv:       ${BOLD}$SCRIPT_DIR/venv${NC}"
    echo -e "  TWS port:   ${BOLD}$TWS_PORT${NC}"
    echo -e "  MCP server: ${BOLD}$SCRIPT_DIR/ibkr_mcp.py${NC}"
    echo ""
    echo -e "${YELLOW}${BOLD}Before using, make sure to:${NC}"
    echo ""
    echo "  1. Start TWS or IB Gateway"
    echo "  2. In TWS: Edit → Global Configuration → API → Settings"
    echo "     - Enable ActiveX and Socket Clients"
    echo "     - Socket port = $TWS_PORT"
    echo "     - Allow connections from localhost"
    echo "  3. Restart Claude Desktop"
    echo ""
}

# ─── Main ─────────────────────────────────────────────────────────────────────

echo ""
echo -e "${BOLD}╔══════════════════════════════════════════════╗${NC}"
echo -e "${BOLD}║  claude-tws-connect installer (macOS)        ║${NC}"
echo -e "${BOLD}╚══════════════════════════════════════════════╝${NC}"

find_python
setup_venv
select_tws_port
configure_claude_desktop
print_summary
