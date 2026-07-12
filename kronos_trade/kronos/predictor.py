"""
Async wrapper around KronosPredictor from the Kronos repo.

Setup: clone https://github.com/shiyu-coder/Kronos.git to vendor/Kronos
       The module is added to sys.path automatically by this file.
"""
from __future__ import annotations

import asyncio
import contextlib
import os
import sys
import threading
from datetime import timezone
from pathlib import Path

import numpy as np
import pandas as pd
from loguru import logger
from rich import console

from kronos_trade.config import kats_cfg, settings

# ── Permanent tqdm suppression ────────────────────────────────────────────────
# Set the env var first so any early tqdm import sees it.
os.environ["TQDM_DISABLE"] = "1"

# Monkeypatch tqdm.__init__ to hard-wire disable=True on every instance.
# This is more reliable than the env var alone: tqdm checks TQDM_DISABLE only
# at instantiation time, and HuggingFace / transformers create fresh bar
# objects on every forward pass — so if env var suppression "wore off" between
# runs it's because new instances were being created after the env var check.
# Patching the class in-place affects every call site in every module,
# regardless of whether they imported tqdm before or after this file.
try:
    import tqdm as _tqdm_mod
    _real_tqdm_init = _tqdm_mod.tqdm.__init__

    def _tqdm_disabled_init(self, *args, **kwargs):
        kwargs["disable"] = True
        _real_tqdm_init(self, *args, **kwargs)

    _tqdm_mod.tqdm.__init__ = _tqdm_disabled_init
    # tqdm.auto and tqdm.notebook both subclass tqdm.tqdm, but some code
    # imports them directly — patch their __init__ too for safety.
    for _sub in ("auto", "notebook", "asyncio"):
        try:
            _sub_mod = getattr(_tqdm_mod, _sub, None)
            if _sub_mod and hasattr(_sub_mod, "tqdm"):
                _sub_mod.tqdm.__init__ = _tqdm_disabled_init
        except Exception:
            pass
except Exception:
    pass  # tqdm not installed — nothing to patch


# ── HF/stdlib logger suppression ─────────────────────────────────────────────

@contextlib.contextmanager
def _quiet_hf():
    """
    Silence HuggingFace / transformers logger noise during model loading and
    inference while keeping loguru (stderr) fully visible.

    Design notes:
      - stdout/stderr are NOT redirected here.  sys.stdout is a global shared
        by all threads in a ThreadPoolExecutor; redirecting it in one thread
        corrupts output for every other thread.  tqdm is handled at the class
        level above (monkeypatch) so no stdout redirect is needed.
      - stdlib logging levels for noisy HF libraries are raised to CRITICAL
        for the duration of the call and restored in the finally block.
    """
    import logging

    _noisy = [
        "transformers", "transformers.modeling_utils",
        "transformers.tokenization_utils_base",
        "transformers.generation", "transformers.generation.utils",
        "huggingface_hub", "huggingface_hub.file_download",
        "huggingface_hub.utils._headers", "huggingface_hub.utils._cache_manager",
        "filelock", "torch", "accelerate", "safetensors",
    ]
    _saved: dict[str, int] = {}
    for name in _noisy:
        lg = logging.getLogger(name)
        _saved[name] = lg.level
        lg.setLevel(logging.CRITICAL)
    try:
        yield
    finally:
        for name, level in _saved.items():
            logging.getLogger(name).setLevel(level)

_console = console.Console(stderr=True)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _models_cached() -> bool:
    """Return True if both Kronos model + tokenizer are present in the local HF cache."""
    try:
        from huggingface_hub import try_to_load_from_cache
        tok = try_to_load_from_cache(settings.kronos_hf_tokenizer_id, "config.json")
        mdl = try_to_load_from_cache(kats_cfg.kronos_hf_model_id,     "config.json")
        return isinstance(tok, str) and isinstance(mdl, str)
    except Exception:
        return False


def _heartbeat(stop: threading.Event, label: str) -> threading.Thread:
    """Log a progress line every 15 s until stop is set — makes long steps visible."""
    def _run() -> None:
        elapsed = 0
        while not stop.wait(15):
            elapsed += 15
            logger.info(f"[kronos] {label} still running ({elapsed}s)…")
    t = threading.Thread(target=_run, daemon=True, name=f"kronos-hb-{label}")
    t.start()
    return t


# ── Engine ─────────────────────────────────────────────────────────────────────
class KronosEngine:
    """
    Thread-safe async wrapper around the Kronos KronosPredictor.

    - Loads model once at startup (can be GPU or CPU)
    - Runs inference in a ThreadPoolExecutor so it doesn't block the event loop
    - Returns raw forecast arrays: mean + lower/upper uncertainty bands
    """

    def __init__(self) -> None:
        self._predictor  = None
        self._lock       = asyncio.Lock()
        self._loaded     = False
        self._cancel_evt = threading.Event()

    # ── Load ──────────────────────────────────────────────────────────────────
    async def load(self) -> None:
        """Load model weights from Hugging Face Hub (once)."""
        async with self._lock:
            if self._loaded: return

            logger.info(
                f"[kronos] loading {kats_cfg.kronos_model_size} model "
                f"on {kats_cfg.kronos_device}..."
            )

            loop = asyncio.get_running_loop()
            await  loop.run_in_executor(None, self._load_sync)
            self._loaded = True
            logger.success(
                f"[kronos] model ready | "
                f"size={kats_cfg.kronos_model_size} device={kats_cfg.kronos_device}"
            )

    def _load_sync(self) -> None:
        import warnings
        warnings.filterwarnings("ignore")
        os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")

        vendor_path = Path(__file__).parents[2] / "vendor" / "Kronos"
        if vendor_path.exists() and str(vendor_path) not in sys.path:
            sys.path.insert(0, str(vendor_path))

        try:
            from model import Kronos, KronosPredictor, KronosTokenizer
        except ImportError as e:
            raise RuntimeError(
                "Could not import Kronos model. "
                "Run: git clone https://github.com/shiyu-coder/Kronos.git vendor/Kronos"
            ) from e

        token = settings.hf_token or None

        # If both model and tokenizer are already cached, skip the HuggingFace Hub
        # network check entirely.  Without this, from_pretrained makes an HTTPS
        # request on every startup that can stall for 30-60 s on a slow connection.
        if _models_cached():
            os.environ["HF_HUB_OFFLINE"] = "1"
            logger.info("[kronos] models found in local cache — skipping hub check")

        # Determine effective device and pre-warm MPS so its init cost is
        # visible and accounted for before we try to move model weights.
        device = kats_cfg.kronos_device
        if device == "mps":
            import torch
            logger.info("[kronos] initialising MPS device…")
            _stop = threading.Event()
            _hb   = _heartbeat(_stop, "MPS init")
            try:
                _probe = torch.zeros(1, device="mps")
                del _probe
                logger.info("[kronos] MPS ready")
            except Exception as exc:
                logger.warning(f"[kronos] MPS unavailable ({exc}), falling back to cpu")
                device = "cpu"
            finally:
                _stop.set()
                _hb.join(timeout=1)

        logger.info("[kronos] loading tokenizer…")
        _stop = threading.Event()
        _hb   = _heartbeat(_stop, "tokenizer load")
        try:
            with _quiet_hf():
                tokenizer = KronosTokenizer.from_pretrained(
                    settings.kronos_hf_tokenizer_id, token=token,  # tokenizer is size-independent
                )
        finally:
            _stop.set(); _hb.join(timeout=1)

        logger.info("[kronos] loading model weights…")
        _stop = threading.Event()
        _hb   = _heartbeat(_stop, "model load")
        try:
            with _quiet_hf():
                model = Kronos.from_pretrained(
                    kats_cfg.kronos_hf_model_id, token=token,
                )
        finally:
            _stop.set(); _hb.join(timeout=1)

        logger.info(f"[kronos] placing model on {device}…")
        _stop = threading.Event()
        _hb   = _heartbeat(_stop, f"move to {device}")
        try:
            self._predictor = KronosPredictor(
                model,
                tokenizer,
                device=device,
                max_context=kats_cfg.kronos_max_context,
            )
        finally:
            _stop.set(); _hb.join(timeout=1)

    @property
    def is_loaded(self) -> bool: return self._loaded

    # ── Predict ───────────────────────────────────────────────────────────────
    async def predict(
        self,
        df: pd.DataFrame,
        x_timestamp: pd.Series,
        horizon: int | None = None,
        symbol: str = "?"
    ) -> dict:
        """
        Run Kronos forecast.

        Args:
            df:          DataFrame with columns [open, high, low, close, volume?, amount?]
            x_timestamp: pd.Series of timestamps for each historical row
            horizon:     Number of bars to forecast (defaults to settings.kronos_forecast_horizon)

        Returns:
            {
                "mean":       np.ndarray shape (horizon,)   – predicted close prices
                "lower":      np.ndarray shape (horizon,)   – 10th percentile
                "upper":      np.ndarray shape (horizon,)   – 90th percentile
                "timestamps": list[datetime]
                "samples":    np.ndarray shape (n_samples, horizon)  – raw MC draws
            }
        """
        if not self._loaded:
            raise RuntimeError("KronosEngine.load() must be called before predict()")

        horizon    = horizon or kats_cfg.kronos_forecast_horizon
        n_samples  = kats_cfg.kronos_mc_samples
        tf_delta   = self._infer_tf_delta(x_timestamp)
        last_ts    = x_timestamp.iloc[-1]

        y_timestamps = pd.Series([
            last_ts + tf_delta * (i + 1) for i in range(horizon)
        ])

        self._cancel_evt.clear()
        loop = asyncio.get_running_loop()

        logger.info(
            f"[kronos] {symbol}: running {n_samples} MC samples "
            f"({horizon}-bar horizon)..."
        )

        try:
            result = await loop.run_in_executor(
                None,
                self._predict_sync,
                df.copy(),
                x_timestamp.copy(),
                y_timestamps,
                horizon,
                n_samples
            )
        except asyncio.CancelledError:
            self._cancel_evt.set()
            logger.warning(f"[kronos] {symbol}: prediction cancelled")
            raise

        n = len(result.get("samples", []))
        logger.info(f"[kronos] {symbol}: {n}/{n_samples} samples → forecast ready")

        result["timestamps"] = [
            ts.to_pydatetime().replace(tzinfo=timezone.utc)
            if hasattr(ts, "to_pydatetime") else ts
            for ts in y_timestamps
        ]

        return result

    def _predict_sync(
        self,
        df: pd.DataFrame,
        x_ts: pd.Series,
        y_ts: pd.Series,
        pred_len: int,
        n_samples: int,
    ) -> dict:
        """Blocking inference — runs in ThreadPoolExecutor."""
        # Collect multiple Monte Carlo samples for uncertainty quantification
        all_samples: list[np.ndarray] = []

        for i in range(n_samples):
            if self._cancel_evt.is_set(): break

            with _quiet_hf():
                pred_df = self._predictor.predict(
                    df=df,
                    x_timestamp=x_ts,
                    y_timestamp=y_ts,
                    pred_len=pred_len,
                    T=1.0,
                    top_p=0.9,
                    sample_count=1,
                )

            if isinstance(pred_df, pd.DataFrame) and "close" in pred_df.columns:
                all_samples.append(pred_df["close"].values)
            else:
                arr = np.asarray(pred_df).flatten()[:pred_len]
                all_samples.append(arr)

        if not all_samples:
            raise RuntimeError("Prediction cancelled before any samples completed")

        samples = np.stack(all_samples, axis=0)  # (n_samples, horizon)
        return {
            "mean":    samples.mean(axis=0),
            "lower":   np.percentile(samples, 10, axis=0),
            "upper":   np.percentile(samples, 90, axis=0),
            "samples": samples,
        }

    @staticmethod
    def _infer_tf_delta(timestamps: pd.Series) -> pd.Timedelta:
        """Guess the bar duration from the timestamp series."""
        if len(timestamps) < 2: return pd.Timedelta(hours=1)
        diffs = timestamps.diff().dropna()
        return diffs.mode().iloc[0]
