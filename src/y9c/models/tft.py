"""
TFT (Temporal Fusion Transformer) Data Preparation

Data loading is handled by :mod:`src.y9c.datasets` (reusable).
This module re-exports the helpers for convenience and is the place to
add TFT-specific feature engineering (lags, normalisation, encoding, etc.).

Usage
-----
from src.y9c.models.tft import load_joined

df = load_joined()                         # all institutions
df = load_joined(rssd_ids=["1447376"])     # USAA only
"""

from __future__ import annotations

try:
    from ..datasets import load_y9c_wide, load_economic_wide, load_joined  # noqa: F401
except ImportError:
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent))
    from src.y9c.datasets import load_y9c_wide, load_economic_wide, load_joined  # noqa: F401

if __name__ == "__main__":
    df = load_joined()
    print(df)
