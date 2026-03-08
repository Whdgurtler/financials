"""
Model data-preparation subpackage.

Available modules
-----------------
src.y9c.models.tft     – TFT feature prep  (re-exports load_joined etc.)
src.y9c.models.deepar  – DeepAR / GluonTS entry-dict builder
src.y9c.models.xgb     – XGBoost flat feature-matrix builder
"""

from .tft import load_y9c_wide, load_economic_wide, load_joined  # noqa: F401
from .deepar import prepare_deepar_data  # noqa: F401
from .xgb import prepare_xgb_data  # noqa: F401
