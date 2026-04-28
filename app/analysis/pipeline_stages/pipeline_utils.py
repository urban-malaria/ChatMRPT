import logging

logger = logging.getLogger(__name__)


def apply_composite_scores_fix(data_handler):
    """Apply the critical fix to set composite_scores for visualization compatibility"""
    print("PIPELINE DEBUG: About to apply composite_scores fix...")
    
    # **CRITICAL FIX: Set the main composite_scores attribute for visualization compatibility**
    # The visualization functions expect data_handler.composite_scores, not composite_scores_mean
    if hasattr(data_handler, 'composite_scores_mean') and data_handler.composite_scores_mean is not None:
        print(f"PIPELINE DEBUG: composite_scores_mean exists and is not None")
        print(f"PIPELINE DEBUG: composite_scores_mean type: {type(data_handler.composite_scores_mean)}")
        
        # Handle both dictionary and direct DataFrame formats
        if isinstance(data_handler.composite_scores_mean, dict):
            # Full copy of the dict including formulas
            data_handler.composite_scores = data_handler.composite_scores_mean.copy()
            if 'scores' in data_handler.composite_scores:
                print(f"PIPELINE DEBUG: Set data_handler.composite_scores = full dict with scores (shape: {data_handler.composite_scores['scores'].shape}) and formulas")
            else:
                print(f"PIPELINE DEBUG: Set data_handler.composite_scores = dict but no 'scores' key found")
        else:
            data_handler.composite_scores = data_handler.composite_scores_mean
            print(f"PIPELINE DEBUG: Set data_handler.composite_scores = composite_scores_mean (shape: {data_handler.composite_scores.shape})")
        
        return True
    else:
        print(f"WARNING PIPELINE DEBUG: No composite_scores_mean to set as default composite_scores")
        print(f"WARNING PIPELINE DEBUG: hasattr(data_handler, 'composite_scores_mean'): {hasattr(data_handler, 'composite_scores_mean')}")
        if hasattr(data_handler, 'composite_scores_mean'):
            print(f"WARNING PIPELINE DEBUG: data_handler.composite_scores_mean is None: {data_handler.composite_scores_mean is None}")
        
        return False 