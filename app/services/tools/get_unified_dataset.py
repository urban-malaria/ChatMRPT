import os
import pandas as pd
from log import logger

def _get_unified_dataset(session_id: str) -> pd.DataFrame:
    """Get unified dataset - NO FALLBACKS."""
    try:
        # Only try unified dataset - NO fallback to original CSV
        unified_path = f'sessions/{session_id}/unified_dataset.csv'
        
        if not os.path.exists(unified_path):
            raise FileNotFoundError(f"❌ UNIFIED DATASET NOT FOUND: {unified_path}")
        
        unified_df = pd.read_csv(unified_path)
        
        if unified_df.empty:
            raise ValueError(f"❌ UNIFIED DATASET IS EMPTY: {unified_path}")
        
        logger.info(f"✅ Unified dataset loaded: {len(unified_df)} rows, {len(unified_df.columns)} columns")
        return unified_df
        
    except Exception as e:
        logger.error(f"❌ Unified dataset loading failed: {e}")
        raise  # Re-raise the exception - NO fallback 