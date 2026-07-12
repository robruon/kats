"""
Two-terminal layout:
  ┌─────────────────┬──────────────────────┐
  │  System Output  │    TUI Dashboard     │
  │  COLS_MAIN      │    COLS_TUI          │
  │  × ROWS_MAIN    │    full height       │
  └─────────────────┴──────────────────────┘

Pixel sizes are calculated from character dimensions using CHAR_W / ROW_H.
Tune those constants if your terminal font is larger or smaller than default.
"""
from __future__ import annotations

import os
import subprocess
import time
from pathlib import Path

from loguru import logger

_TITLE_TUI = "KronosTrade TUI"

TMP_DIR = Path(__file__).parent.parent / ".tmp"

# ── Screen bounds ─────────────────────────────────────────────────────────────

def get_screen_bounds() -> tuple[int, int, int, int]:
    """
    Usable area excluding menu bar and Dock.
    Returns (left, top, width, height) in AppleScript coordinates.
    """
    try:
        r = subprocess.run(
            ["osascript", "-e",
             'tell application "Finder" to get bounds of window of desktop'],
            capture_output=True, text=True, timeout=5,
        )
        if r.returncode == 0:
            l, t, right, b = [int(n.strip()) for n in r.stdout.strip().split(",")]
            logger.debug(f"[launcher] Finder desktop {right-l}x{b-t}  top={t}")
            return l, t, right - l, b - t
    except Exception as e:
        logger.debug(f"[launcher] bounds fallback: {e}")

    return 0, 25, 1512, 957


# ── Script / window helpers ───────────────────────────────────────────────────

def _cleanup_tmp() -> None:
    """Remove leftover launcher scripts and AppleScripts from previous runs."""
    if not TMP_DIR.exists():
        return
    for pattern in ("kronos_*.sh", "kronos_close_*.scpt"):
        for f in TMP_DIR.glob(pattern):
            try:
                f.unlink()
            except Exception:
                pass


def _write_script(name: str, cwd: str, cmd: str, title: str) -> Path:
    """
    Write a bash launcher to .tmp.
    Self-deletes on exit; uses a heredoc for the close AppleScript.
    """
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    path = TMP_DIR / f"kronos_{name}.sh"
    with open(path, "w") as f:
        f.write(f"""
#!/bin/bash
printf '\\033]0;{title}\\007'
cd {repr(cwd)}
{cmd}

# Self-cleanup then close this Terminal window
rm -f "$0"
osascript << 'OSASCRIPT'
tell application "Terminal"
    repeat with w in windows
        if name of w contains "{title}" then
            close w saving no
        end if
    end repeat
end tell
OSASCRIPT
""")
    os.chmod(path, 0o755)
    return path


def _close_stale(title: str) -> None:
    """Close any Terminal windows from a previous run."""
    TMP_DIR.mkdir(parents=True, exist_ok=True)
    scpt = TMP_DIR / f"kronos_close_{abs(hash(title)) & 0xFFFF}.scpt"
    with open(scpt, "w") as f:
        f.write(f"""
tell application "Terminal"
    repeat with w in windows
        if name of w contains "{title}" then
            close w saving no
        end if
    end repeat
end tell
""")
    try:
        subprocess.run(["osascript", scpt], capture_output=True, timeout=5)
    except Exception: pass
    finally: scpt.unlink(missing_ok=True)


def _set_front_bounds(x: int, y: int, w: int, h: int) -> None:
    """Resize the currently active Terminal window to pixel bounds."""
    script = "\n".join([
        'tell application "Terminal"',
        f'    set bounds of front window to {{{x}, {y}, {x + w}, {y + h}}}',
        'end tell',
    ])
    try:
        subprocess.run(["osascript", "-e", script], capture_output=True, timeout=5)
    except Exception: pass


def _open_window(script_path: Path, x: int, y: int, w: int, h: int) -> None:
    """Open a new Terminal window and set its pixel bounds."""
    script = "\n".join([
        'tell application "Terminal"',
        f'    do script "bash {script_path}"',
        '    delay 0.3',
        f'    set bounds of front window to {{{x}, {y}, {x + w}, {y + h}}}',
        'end tell',
    ])
    subprocess.Popen(["osascript", "-e", script])


# ── Main launcher ─────────────────────────────────────────────────────────────

def launch_terminals(cwd: str, use_demo: bool = False) -> None:
    """
    Open two Terminal windows:
    Left  — main process output, COLS_MAIN × ROWS_MAIN
    Right — TUI dashboard, COLS_TUI wide, full usable height
    """
    left, top, sw, sh = get_screen_bounds()
    half_w = sw // 2

    logger.info(
        f"[launcher] screen={sw}x{sh}  "
        f"main={top + half_w}x{sh}px  "
        f"tui={top + half_w}x{sh}px"
    )

    # 1. Clean up any leftover scripts from previous runs
    _cleanup_tmp()

    # 2. Resize the current (main) window
    _set_front_bounds(left, top, half_w, sh)

    # 3. Close any stale TUI windows from a previous run
    _close_stale(_TITLE_TUI)
    time.sleep(0.3)

    # 3. Open TUI — starts where main ends, fills to screen right and bottom
    tui_cmd = "poetry run python kronos_trade/dashboard/tui.py"
    if use_demo: tui_cmd += " --demo"
    tui_script = _write_script("tui", cwd, tui_cmd, _TITLE_TUI)

    _open_window(tui_script, left + half_w, top, half_w, sh)