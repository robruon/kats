#!/usr/bin/env bash
# ============================================================
# scripts/setup.sh
# KronosTrade — one-shot environment setup
#
# Run from the project root:
#   chmod +x scripts/setup.sh && ./scripts/setup.sh
# ============================================================
set -e

BOLD="\033[1m"
GREEN="\033[32m"
YELLOW="\033[33m"
CYAN="\033[36m"
RESET="\033[0m"

info()  { echo -e "${CYAN}[setup]${RESET} $*"; }
ok()    { echo -e "${GREEN}[✓]${RESET} $*"; }
warn()  { echo -e "${YELLOW}[!]${RESET} $*"; }

echo -e "${BOLD}"
echo "  ╔══════════════════════════════════════╗"
echo "  ║     KronosTrade Environment Setup    ║"
echo "  ╚══════════════════════════════════════╝"
echo -e "${RESET}"

# ── 1. Find Python 3.11+ ──────────────────────────────────────────────────────
info "Locating Python 3.11+…"

PYTHON_BIN=""

# Start with explicit versioned binaries (fast, no external tools)
CANDIDATES=(python3.13 python3.12 python3.11 python3)

# Add Homebrew paths only if brew is already on PATH (avoids slow subprocess)
if command -v brew &>/dev/null; then
  BREW_PREFIX=$(brew --prefix 2>/dev/null)
  if [[ -n "$BREW_PREFIX" ]]; then
    CANDIDATES+=(
      "$BREW_PREFIX/bin/python3.13"
      "$BREW_PREFIX/bin/python3.12"
      "$BREW_PREFIX/bin/python3.11"
    )
  fi
fi

# Add pyenv paths only if pyenv is on PATH
if command -v pyenv &>/dev/null; then
  for v in 3.13 3.12 3.11; do
    PYENV_PATH=$(pyenv which "python$v" 2>/dev/null || true)
    [[ -n "$PYENV_PATH" ]] && CANDIDATES+=("$PYENV_PATH")
  done
fi

for candidate in "${CANDIDATES[@]}"; do
  [[ -z "$candidate" ]] && continue
  # Resolve to full path; skip if not found
  FULL_PATH=$(command -v "$candidate" 2>/dev/null || echo "$candidate")
  [[ ! -x "$FULL_PATH" ]] && continue
  MAJOR=$("$FULL_PATH" -c "import sys; print(sys.version_info.major)" 2>/dev/null || true)
  MINOR=$("$FULL_PATH" -c "import sys; print(sys.version_info.minor)" 2>/dev/null || true)
  if [[ "$MAJOR" -eq 3 && "$MINOR" -ge 11 ]]; then
    PYTHON_BIN="$FULL_PATH"
    break
  fi
done

if [[ -z "$PYTHON_BIN" ]]; then
  echo ""
  echo "  ERROR: Python 3.11+ not found."
  echo ""
  echo "  Install it with Homebrew:"
  echo "    brew install python@3.12"
  echo ""
  echo "  Or with pyenv:"
  echo "    pyenv install 3.12 && pyenv global 3.12"
  echo ""
  exit 1
fi

PYVER=$("$PYTHON_BIN" --version)
ok "Found $PYVER at $PYTHON_BIN"

# ── 2. Poetry ─────────────────────────────────────────────────────────────────
info "Checking Poetry…"
if ! command -v poetry &> /dev/null; then
  warn "Poetry not found — installing…"
  curl -sSL https://install.python-poetry.org | "$PYTHON_BIN" -
  export PATH="$HOME/.local/bin:$PATH"
fi
ok "Poetry $(poetry --version)"

# ── 3. Point Poetry at the correct Python interpreter ─────────────────────────
info "Configuring Poetry environment…"

# Keep venv inside the project so VS Code finds it automatically
poetry config virtualenvs.in-project true

# Tell Poetry exactly which Python to use
poetry env use "$PYTHON_BIN"
ok "Poetry env set to $PYTHON_BIN"

# ── 4. Install Python dependencies ───────────────────────────────────────────
info "Installing Python dependencies…"
poetry install --no-interaction --quiet
ok "Dependencies installed"

# ── 4b. Attempt optional Databento install ───────────────────────────────────
info "Trying optional Databento install (binary wheels may not exist for all platforms)..."
if poetry install -E databento --no-interaction >/dev/null 2>&1; then
  ok "Databento installed — futures/forex feed available"
elif poetry run pip install databento >/dev/null 2>&1; then
  ok "Databento installed via pip — futures/forex feed available"
else
  warn "Databento DBN wheels unavailable for your platform — skipping"
  warn "Alpaca and CCXT feeds will be used instead"
  warn "To retry later: poetry install -E databento"
fi

# ── 5. Initialize Kronos submodule ─────────────────────────────────────────────
if [[ ! -d "vendor/Kronos" ]]; then
  info "Initializing Kronos submodule…"
  git submodule update --init --recursive
  ok "Kronos submodule initialized"
else
  ok "Kronos already present at vendor/Kronos"
fi

# ── 6. Install Kronos dependencies ───────────────────────────────────────────
if [[ -f "vendor/Kronos/requirements.txt" ]]; then
  info "Installing Kronos requirements…"
  poetry run pip install -r vendor/Kronos/requirements.txt --quiet
  ok "Kronos requirements installed"
fi

# ── 7. Redis check ───────────────────────────────────────────────────────────
info "Checking Redis…"
if command -v redis-cli &> /dev/null && redis-cli ping &> /dev/null; then
  ok "Redis is running"
else
  warn "Redis not running — start with: redis-server"
  warn "Or use Docker: docker run -d -p 6379:6379 redis:alpine"
fi

# ── 8. .env setup ─────────────────────────────────────────────────────────────
if [[ ! -f ".env" ]]; then
  info "Creating .env from template…"
  cp .env.example .env
  warn "Edit .env and fill in your API keys before running"
  ok ".env created"
else
  ok ".env already exists"
fi

# ── 9. Log directory ──────────────────────────────────────────────────────────
mkdir -p logs
ok "logs/ directory ready"

# ── 10. GPU check ─────────────────────────────────────────────────────────────
info "Checking GPU/CUDA…"
GPU=$(poetry run python3 -c "import torch; print('CUDA' if torch.cuda.is_available() else 'MPS' if hasattr(torch.backends,'mps') and torch.backends.mps.is_available() else 'CPU')" 2>/dev/null || echo "unknown")
if [[ "$GPU" == "CUDA" ]]; then
  ok "GPU: CUDA available — Kronos will run on GPU"
elif [[ "$GPU" == "MPS" ]]; then
  ok "GPU: Apple MPS available"
else
  warn "No GPU detected — Kronos will run on CPU (10-20× slower)"
  warn "Consider setting KRONOS_MODEL_SIZE=mini in .env for faster CPU inference"
fi

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}${BOLD}Setup complete!${RESET}"
echo ""
echo "  Next steps:"
echo "  1. Edit .env with your API keys"
echo "  2. Run tests:        poetry run pytest tests/ -v"
echo "  3. Run backtest:     poetry run python scripts/backtest.py --symbol BTCUSD"
echo "  4. Start system:     poetry run python scripts/run_system.py --tui"
echo ""