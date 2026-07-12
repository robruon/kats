#!/usr/bin/env bash
# ── Fix dotenv prompts in ~/.zshrc ────────────────────────────────────────────
# The ZSH_DOTENV_PROMPT variable must be set BEFORE oh-my-zsh is sourced.
# This script finds the 'source $ZSH/oh-my-zsh.sh' line and inserts
# the variable above it.

ZSHRC="$HOME/.zshrc"
MARKER="# KronosTrade: disable dotenv prompts"
TARGET='source \$ZSH/oh-my-zsh.sh'

if grep -q "$MARKER" "$ZSHRC" 2>/dev/null; then
  echo "Already patched — nothing to do"
  exit 0
fi

if ! grep -q 'source.*oh-my-zsh.sh' "$ZSHRC" 2>/dev/null; then
  # No oh-my-zsh — just append to end
  echo "" >> "$ZSHRC"
  echo "$MARKER" >> "$ZSHRC"
  echo "export ZSH_DOTENV_PROMPT=false" >> "$ZSHRC"
  echo "export AUTOENV_ASSUME_YES=1" >> "$ZSHRC"
  echo "Appended to ~/.zshrc (no oh-my-zsh found)"
  exit 0
fi

# Insert before the oh-my-zsh source line
TMPFILE=$(mktemp)
while IFS= read -r line; do
  if echo "$line" | grep -q 'source.*oh-my-zsh.sh'; then
    echo "$MARKER"
    echo "export ZSH_DOTENV_PROMPT=false"
    echo "export AUTOENV_ASSUME_YES=1"
    echo ""
  fi
  echo "$line"
done < "$ZSHRC" > "$TMPFILE"

cp "$TMPFILE" "$ZSHRC"
rm "$TMPFILE"
echo "Patched ~/.zshrc — run: source ~/.zshrc"