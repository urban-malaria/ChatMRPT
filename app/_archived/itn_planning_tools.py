"""
ITN (Insecticide-Treated Net) Distribution Planning Tools

Tools for planning optimal distribution of bed nets based on malaria vulnerability rankings.
Activates after analysis is complete and uses composite or PCA rankings for prioritization.
"""

import logging
from typing import Optional, List, Dict, Any
from pydantic import Field, validator

from .base import BaseTool, ToolCategory, ToolExecutionResult
from ..analysis.itn_pipeline import calculate_itn_distribution
from ..models.data_handler import DataHandler
from ..services.container import get_service_container

logger = logging.getLogger(__name__)


class PlanITNDistribution(BaseTool):
    """
    Plan ITN (bed net) distribution based on vulnerability rankings.
    
    This tool helps allocate insecticide-treated nets optimally across wards,
    prioritizing high-risk areas based on malaria vulnerability analysis.
    """
    
    total_nets: Optional[int] = Field(
        None, 
        description="Total number of nets available for distribution"
    )
    
    avg_household_size: Optional[float] = Field(
        None,
        description="Average household size in the region (default: 5.0)"
    )
    
    urban_threshold: Optional[float] = Field(
        None,
        description="Urban percentage threshold for prioritization (default: 30%)"
    )
    
    method: str = Field(
        "composite",
        description="Ranking method to use: 'composite' or 'pca'"
    )
    
    @classmethod
    def get_tool_name(cls) -> str:
        return "plan_itn_distribution"
    
    @classmethod
    def get_description(cls) -> str:
        return "Plan optimal distribution of ITN/bed nets based on malaria vulnerability rankings"
    
    @classmethod
    def get_category(cls) -> ToolCategory:
        return ToolCategory.ITN_PLANNING
    
    @classmethod
    def get_examples(cls) -> List[str]:
        return [
            "Plan ITN distribution",
            "I want to distribute bed nets",
            "I want to plan bed net distribution",
            "Help me plan net distribution",
            "Allocate 10000 nets across wards",
            "Plan bed net distribution with 50000 nets",
            "Plan ITN/bed net distribution",
            "Help me distribute ITNs"
        ]
    
    @validator('method')
    def validate_method(cls, v):
        if v not in ['composite', 'pca']:
            raise ValueError("Method must be either 'composite' or 'pca'")
        return v
    
    def execute(self, session_id: str) -> ToolExecutionResult:
        """Execute ITN distribution planning"""
        try:
            # Get service container
            container = get_service_container()
            data_service = container.get('data_service')
            
            # Get data handler
            data_handler = data_service.get_handler(session_id)
            if not data_handler:
                return self._create_error_result(
                    "No data available. Please upload data first."
                )
            
            # Check if analysis is complete
            if not self._check_analysis_complete(data_handler):
                return self._create_error_result(
                    "Analysis has not been completed yet. Please run malaria risk analysis first before planning ITN distribution."
                )
            
            # Check if parameters are provided
            if self.total_nets is None:
                return self._create_parameter_request_result()
            
            # Use defaults if not provided
            avg_household_size = self.avg_household_size or 5.0
            urban_threshold = self.urban_threshold or 30.0
            
            # Run ITN distribution calculation
            result = calculate_itn_distribution(
                data_handler=data_handler,
                session_id=session_id,
                total_nets=self.total_nets,
                avg_household_size=avg_household_size,
                urban_threshold=urban_threshold,
                method=self.method
            )
            
            if result['status'] != 'success':
                return self._create_error_result(
                    result.get('message', 'ITN distribution calculation failed')
                )
            
            # Format success message
            stats = result['stats']
            message = self._format_distribution_summary(stats, result)
            
            # Add iframe HTML for visualization display
            map_path = result.get('map_path')
            if map_path:
                message += f'\n\n<iframe src="{map_path}" width="100%" height="600" frameborder="0"></iframe>'
            
            # Generate export documents
            download_links = []
            try:
                logger.info(f"Generating ITN distribution export documents for session {session_id}")
                from .export_tools import ExportITNResults
                
                # Create export tool instance
                export_tool = ExportITNResults(
                    include_dashboard=True,
                    include_csv=True,
                    include_maps=False  # We already have the map
                )
                
                # Execute export
                export_result = export_tool.execute(session_id)
                
                if export_result.success:
                    # Extract the exported files from the result
                    export_data = export_result.data
                    
                    # Add download links for exported files
                    if 'csv_path' in export_data and export_data['csv_path']:
                        csv_filename = export_data['csv_path'].name
                        download_links.append({
                            'url': f'/export/download/{session_id}/{csv_filename}',
                            'filename': csv_filename,
                            'description': '📊 ITN Distribution Results (CSV)',
                            'type': 'csv'
                        })
                    
                    if 'dashboard_path' in export_data and export_data['dashboard_path']:
                        dashboard_filename = export_data['dashboard_path'].name
                        download_links.append({
                            'url': f'/export/download/{session_id}/{dashboard_filename}',
                            'filename': dashboard_filename,
                            'description': '📈 Interactive Dashboard (HTML)',
                            'type': 'html'
                        })
                    
                    if download_links:
                        logger.info(f"✅ Generated {len(download_links)} export documents for ITN distribution")
                else:
                    logger.warning(f"Export generation failed: {export_result.message}")
            except Exception as e:
                logger.error(f"Error generating export documents: {e}")
                # Continue without exports - don't fail the main operation
            
            # Prepare result data
            result_data = {
                'stats': stats,
                'map_path': map_path,
                'method_used': self.method,
                'urban_threshold': urban_threshold,
                'household_size': avg_household_size,
                'top_priority_wards': self._get_top_priority_wards(result['prioritized'])
            }
            
            # CRITICAL: Mark ITN planning complete in Redis for multi-worker consistency
            try:
                from ..core.redis_state_manager import get_redis_state_manager
                redis_manager = get_redis_state_manager()
                redis_success = redis_manager.mark_itn_planning_complete(session_id)
                if redis_success:
                    logger.info(f"🎯 Redis: ITN planning marked complete for session {session_id}")
                else:
                    logger.warning(f"⚠️ Redis: Failed to mark ITN planning complete for {session_id}")
            except Exception as e:
                logger.error(f"Redis state manager error: {e}")
                # Continue - fallback to session flags
            
            # CRITICAL: Ensure analysis_complete flag is set for report generation
            try:
                from flask import session
                session['analysis_complete'] = True
                session['itn_planning_complete'] = True
                session.modified = True
                logger.info("✅ Set analysis_complete and itn_planning_complete flags in session")
            except Exception as e:
                logger.warning(f"Could not set session flags: {e}")
            
            return self._create_success_result(
                message=message,
                data=result_data,
                web_path=map_path
            )
            
        except Exception as e:
            logger.error(f"Error in ITN planning: {e}", exc_info=True)
            return self._create_error_result(f"ITN planning failed: {str(e)}")
    
    def _check_analysis_complete(self, data_handler: DataHandler) -> bool:
        """Check if analysis has been completed"""
        logger.info("🔍 Checking if analysis is complete...")
        
        # Get session ID from multiple sources
        session_id = None
        try:
            # Try from data_handler first
            session_id = getattr(data_handler, 'session_id', None)
            if not session_id:
                # Try from Flask session
                from flask import session as flask_session
                session_id = flask_session.get('session_id')
            logger.info(f"Session ID: {session_id}")
        except Exception as e:
            logger.warning(f"Could not get session ID: {e}")
        
        # Method 0: Check Redis FIRST (MOST RELIABLE for multi-worker)
        # This is the authoritative source across ALL workers
        if session_id:
            try:
                from ..core.redis_state_manager import get_redis_state_manager
                redis_manager = get_redis_state_manager()
                if redis_manager.is_analysis_complete(session_id):
                    logger.info("✅ Redis confirms: Analysis is complete")
                    return True
                else:
                    logger.info("❌ Redis: Analysis not marked complete")
            except Exception as e:
                logger.warning(f"Redis check failed (will try fallbacks): {e}")
        
        # Method 1: Check for analysis completion marker file (MOST RELIABLE)
        if session_id:
            try:
                import os
                from pathlib import Path
                
                session_folder = Path("instance/uploads") / session_id
                marker_file = session_folder / ".analysis_complete"
                if marker_file.exists():
                    logger.info("✅ Found .analysis_complete marker file")
                    return True
            except Exception as e:
                logger.warning(f"Could not check marker file: {e}")
        
        # Method 2: Direct file check (reliable for multi-worker)
        if session_id:
            try:
                import os
                from pathlib import Path
                
                # Check for analysis result files
                session_folder = Path("instance/uploads") / session_id
                if session_folder.exists():
                    # Check for key analysis files
                    analysis_files = [
                        "unified_dataset.geoparquet",
                        "unified_dataset.csv",
                        "analysis_results_composite.csv",
                        "analysis_results_pca.csv",
                        "composite_analysis_results.csv",
                        "analysis_composite_scores.csv",
                        "analysis_vulnerability_rankings.csv",
                        "composite_scores.csv"
                    ]
                    
                    for filename in analysis_files:
                        filepath = session_folder / filename
                        if filepath.exists():
                            logger.info(f"✅ Found analysis file: {filename}")
                            return True
                    
                    # Check if any CSV has ranking columns
                    for csv_file in session_folder.glob("*.csv"):
                        try:
                            import pandas as pd
                            df = pd.read_csv(csv_file, nrows=5)
                            if any(col in df.columns for col in ['composite_rank', 'pca_rank']):
                                logger.info(f"✅ Found rankings in: {csv_file.name}")
                                return True
                        except:
                            continue
                            
                    logger.debug(f"No analysis files found in {session_folder}")
            except Exception as e:
                logger.warning(f"Could not check files directly: {e}")
        
        # Method 3: Check unified data state (file-based, works across workers)
        try:
            from ..core.unified_data_state import get_data_state
            if session_id:
                data_state = get_data_state(session_id)
                
                # Check if analysis is marked complete in data state
                if data_state.analysis_complete:
                    logger.info("✅ Analysis complete in unified data state")
                    return True
                
                # Check if current data has analysis columns
                if data_state.current_data is not None:
                    df = data_state.current_data
                    analysis_columns = ['composite_score', 'composite_rank', 'pca_score', 'pca_rank']
                    has_columns = any(col in df.columns for col in analysis_columns)
                    if has_columns:
                        logger.info(f"✅ Analysis columns found: {[col for col in analysis_columns if col in df.columns]}")
                        return True
                    else:
                        logger.debug(f"No analysis columns in dataset. Columns: {list(df.columns)[:10]}...")
        except Exception as e:
            logger.warning(f"Could not check unified data state: {e}")
        
        # Method 4: Check Flask session flag (might not work with multi-worker)
        try:
            from flask import session
            if session.get('analysis_complete', False):
                logger.info("✅ Analysis complete flag found in Flask session")
                return True
            else:
                logger.debug("Flask session flag not set")
        except Exception as e:
            logger.debug(f"Could not check session flag: {e}")
        
        # Method 5: Check data handler attributes (legacy)
        has_composite = hasattr(data_handler, 'vulnerability_rankings') and data_handler.vulnerability_rankings is not None
        has_pca = hasattr(data_handler, 'vulnerability_rankings_pca') and data_handler.vulnerability_rankings_pca is not None
        has_unified = hasattr(data_handler, 'unified_dataset') and data_handler.unified_dataset is not None
        
        if has_composite or has_pca or has_unified:
            logger.info(f"✅ Found in data_handler: composite={has_composite}, pca={has_pca}, unified={has_unified}")
            return True
        
        logger.warning("❌ Analysis not complete - no indicators found")
        return False
    
    def _create_parameter_request_result(self) -> ToolExecutionResult:
        """Create result requesting parameters from user"""
        message = """I'll help you plan ITN (Insecticide-Treated Net) distribution.

To optimize the distribution, I need a few inputs:

1. **Total number of nets available**: How many ITN nets do you have for distribution?
2. **Average household size**: What's the typical household size in this region? (default is 5)
3. **Urban threshold**: What percentage of urbanization should we use as the threshold for prioritizing rural areas? (default is 30%)

Please provide these values and I'll calculate the optimal distribution plan based on the vulnerability rankings."""
        
        return ToolExecutionResult(
            success=True,
            message=message,
            data={'waiting_for_parameters': True},
            metadata={'requires_user_input': True}
        )
    
    def _format_distribution_summary(self, stats: Dict[str, Any], result: Dict[str, Any]) -> str:
        """Format the distribution summary message"""
        prioritized = result.get('prioritized', None)
        
        summary = f"""✅ **ITN Distribution Plan Complete!**

**Allocation Summary:**
- Total nets available: {stats['total_nets']:,}
- Nets allocated: {stats['allocated']:,}
- Remaining: {stats['remaining']:,}

**Coverage Statistics:**
- Population covered: {stats['covered_population']:,} ({stats['coverage_percent']}%)
- Prioritized rural wards: {stats['prioritized_wards']}
- Additional urban wards: {stats['reprioritized_wards']}"""
        
        # Add top priority wards if available
        if prioritized is not None and len(prioritized) > 0:
            top_5 = prioritized.nlargest(5, 'nets_allocated')[['WardName', 'nets_allocated', 'Population']]
            summary += "\n\n**Top 5 Priority Wards:**"
            for idx, ward in top_5.iterrows():
                coverage = (ward['nets_allocated'] * 1.8 / ward['Population'] * 100) if ward['Population'] > 0 else 0
                summary += f"\n{idx+1}. **{ward['WardName']}** - {ward['nets_allocated']} nets ({coverage:.1f}% coverage)"
        
        summary += "\n\n📊 View the interactive distribution map below to see the allocation across all wards."
        
        return summary
    
    def _get_top_priority_wards(self, prioritized) -> List[Dict[str, Any]]:
        """Get top priority wards for metadata"""
        if prioritized is None or len(prioritized) == 0:
            return []
        
        top_wards = []
        for _, ward in prioritized.nlargest(10, 'nets_allocated').iterrows():
            top_wards.append({
                'ward_name': ward['WardName'],
                'nets_allocated': int(ward['nets_allocated']),
                'population': int(ward['Population']) if 'Population' in ward else 0,
                'coverage_percent': float((ward['nets_allocated'] * 1.8 / ward['Population'] * 100)) if ward.get('Population', 0) > 0 else 0
            })
        
        return top_wards