"""
TPR Workflow Handler

Handles all TPR (Test Positivity Rate) workflow logic including state selection,
facility selection, age group selection, and calculation triggering.
"""

import os
import logging
from typing import Dict, Any, Optional
from pathlib import Path
from enum import Enum

from .state_manager import DataAnalysisStateManager, ConversationStage
from .tpr_data_analyzer import TPRDataAnalyzer
from .data_analysis_v3.core.encoding_handler import find_raw_data_file, read_raw_data

logger = logging.getLogger(__name__)


class TPRWorkflowHandler:
    """Handles TPR workflow progression and state management."""
    
    def __init__(self, session_id: str, state_manager: DataAnalysisStateManager, 
                 tpr_analyzer: TPRDataAnalyzer):
        """
        Initialize TPR workflow handler.
        
        Args:
            session_id: Session identifier
            state_manager: State management instance
            tpr_analyzer: TPR data analyzer instance
        """
        self.session_id = session_id
        self.state_manager = state_manager
        self.tpr_analyzer = tpr_analyzer
        self.tpr_selections = {}
        self.current_stage = ConversationStage.INITIAL
        self.uploaded_data = None
        self.session_folder = f"instance/uploads/{session_id}"
    
    def set_data(self, data):
        """Set the uploaded data for analysis."""
        self.uploaded_data = data
    
    def set_stage(self, stage: ConversationStage):
        """Update the current workflow stage."""
        self.current_stage = stage
    
    def handle_workflow(self, user_query: str) -> Optional[Dict[str, Any]]:
        """
        Handle TPR workflow progression based on current stage.
        
        Args:
            user_query: User's input message
            
        Returns:
            Response dictionary or None if not in TPR workflow
        """
        logger.info("="*60)
        logger.info("🔄 TPR: handle_workflow called")
        logger.info(f"  📝 Query: {user_query[:100]}...")
        logger.info(f"  🎯 Current Stage: {self.current_stage}")
        logger.info(f"  🆔 Session ID: {self.session_id}")
        logger.info(f"  📊 Selections: {self.tpr_selections}")
        logger.info("="*60)
        
        if self.current_stage == ConversationStage.TPR_STATE_SELECTION:
            logger.info("🟢 Routing to handle_state_selection")
            return self.handle_state_selection(user_query)
        
        elif self.current_stage == ConversationStage.TPR_FACILITY_LEVEL:
            logger.info("🟢 Routing to handle_facility_selection")
            return self.handle_facility_selection(user_query)
        
        elif self.current_stage == ConversationStage.TPR_AGE_GROUP:
            logger.info("🟢 Routing to handle_age_group_selection")
            return self.handle_age_group_selection(user_query)
        
        elif self.current_stage == ConversationStage.TPR_COMPLETE:
            # Check if user wants to proceed to risk analysis
            query_lower = user_query.lower().strip()
            logger.info(f"🟡 TPR_COMPLETE stage - User said: '{query_lower}'")
            
            if any(word in query_lower for word in ['yes', 'proceed', 'continue', 'go ahead', 'sure', 'ok', 'okay', 'yeah']):
                logger.info("🟡 User confirmed - triggering risk analysis transition")
                # Trigger risk analysis pipeline
                return self.trigger_risk_analysis()
            elif any(word in query_lower for word in ['no', 'not now', 'later', 'stop']):
                # End workflow - NOW we mark it complete
                self.current_stage = ConversationStage.INITIAL
                self.state_manager.update_workflow_stage(self.current_stage)
                self.state_manager.mark_tpr_workflow_complete()
                logger.info("🔴 User declined risk analysis - marking TPR workflow as COMPLETE")
                return {
                    "success": True,
                    "message": "No problem! The TPR results have been saved. You can proceed to risk analysis anytime by saying 'analyze risk' or 'rank wards'.",
                    "session_id": self.session_id
                }
        
        return None
    
    def start_workflow(self) -> Dict[str, Any]:
        """Start the TPR workflow."""
        logger.info("Starting TPR workflow")
        
        # CRITICAL: Mark TPR workflow as active
        self.state_manager.mark_tpr_workflow_active()
        logger.info("🔴 CRITICAL: Marked TPR workflow as ACTIVE")
        
        # Reset TPR selections
        self.tpr_selections = {}
        
        # Analyze available states
        state_analysis = self.tpr_analyzer.analyze_states(self.uploaded_data)
        
        # Check if there's only one state - if so, skip state selection
        if state_analysis.get('total_states') == 1:
            # Auto-select the single state
            single_state = list(state_analysis['states'].keys())[0]
            logger.info(f"Single state detected: {single_state}, skipping state selection")
            
            self.tpr_selections['state'] = single_state
            self.state_manager.save_tpr_selection('state', single_state)
            
            # Move directly to facility selection
            self.current_stage = ConversationStage.TPR_FACILITY_LEVEL
            self.state_manager.update_workflow_stage(self.current_stage)
            
            # Analyze facility levels
            facility_analysis = self.tpr_analyzer.analyze_facility_levels(self.uploaded_data, single_state)
            
            # Import formatter
            from .formatters import MessageFormatter
            formatter = MessageFormatter(self.session_id)
            message = formatter.format_facility_selection_only(facility_analysis)
            
            return {
                "success": True,
                "message": message,
                "session_id": self.session_id,
                "workflow": "tpr",
                "stage": "facility_selection"
            }
        
        # Multiple states - show state selection
        self.current_stage = ConversationStage.TPR_STATE_SELECTION
        self.state_manager.update_workflow_stage(self.current_stage)
        
        from .formatters import MessageFormatter
        formatter = MessageFormatter(self.session_id)
        message = formatter.format_state_selection(state_analysis)
        
        return {
            "success": True,
            "message": message,
            "session_id": self.session_id,
            "workflow": "tpr",
            "stage": "state_selection"
        }
    
    def handle_state_selection(self, user_query: str) -> Dict[str, Any]:
        """Handle state selection in TPR workflow."""
        # Extract selected state
        selected_state = self.extract_state_from_query(user_query)
        
        if not selected_state:
            return {
                "success": True,
                "message": "I didn't catch which state you'd like to analyze. Please specify the state name or number from the list.",
                "session_id": self.session_id
            }
        
        # Save selection
        self.tpr_selections['state'] = selected_state
        self.state_manager.save_tpr_selection('state', selected_state)
        
        # Move to facility level selection
        self.current_stage = ConversationStage.TPR_FACILITY_LEVEL
        self.state_manager.update_workflow_stage(self.current_stage)
        
        # Analyze facility levels for selected state
        facility_analysis = self.tpr_analyzer.analyze_facility_levels(self.uploaded_data, selected_state)
        
        from .formatters import MessageFormatter
        formatter = MessageFormatter(self.session_id)
        message = formatter.format_facility_selection(selected_state, facility_analysis)
        
        return {
            "success": True,
            "message": message,
            "session_id": self.session_id,
            "workflow": "tpr",
            "stage": "facility_selection"
        }
    
    def handle_facility_selection(self, user_query: str) -> Dict[str, Any]:
        """Handle facility level selection in TPR workflow."""
        logger.info(f"🔵 Handling facility selection with query: '{user_query}'")
        
        # Extract facility level
        selected_level = self.extract_facility_level(user_query)
        logger.info(f"🔵 Extracted facility level: {selected_level}")
        
        # Save selection
        self.tpr_selections['facility_level'] = selected_level
        self.state_manager.save_tpr_selection('facility_level', selected_level)
        logger.info(f"🔵 Saved facility selection: {selected_level}")
        
        # Move to age group selection
        self.current_stage = ConversationStage.TPR_AGE_GROUP
        self.state_manager.update_workflow_stage(self.current_stage)
        logger.info(f"🔵 Updated stage to: {self.current_stage}")
        
        # Analyze age groups
        age_analysis = self.tpr_analyzer.analyze_age_groups(
            self.uploaded_data,
            self.tpr_selections.get('state', ''),
            selected_level
        )
        
        logger.info(f"🔵 Age analysis complete, formatting response")
        
        from .formatters import MessageFormatter
        formatter = MessageFormatter(self.session_id)
        message = formatter.format_age_group_selection(age_analysis)
        
        logger.info(f"🔵 Returning age selection prompt (length: {len(message)})")
        
        return {
            "success": True,
            "message": message,
            "session_id": self.session_id,
            "workflow": "tpr",
            "stage": "age_selection"
        }
    
    def handle_age_group_selection(self, user_query: str) -> Dict[str, Any]:
        """Handle age group selection and trigger TPR calculation."""
        logger.info(f"🟣 Handling age group selection with query: '{user_query}'")
        logger.info(f"🟣 Current TPR selections: {self.tpr_selections}")
        
        # CRITICAL: Check if user is typing a facility term instead of age selection
        # This happens when the frontend sends the same message twice
        if any(word in user_query.lower() for word in ['primary', 'secondary', 'tertiary']):
            logger.warning(f"🟣 User entered facility term '{user_query}' during age selection - BLOCKING and re-prompting!")
            
            # Don't process this as age selection!
            # Re-show the age group prompt
            age_analysis = self.tpr_analyzer.analyze_age_groups(
                self.uploaded_data,
                self.tpr_selections.get('state', ''),
                self.tpr_selections.get('facility_level', 'primary')
            )
            
            from .formatters import MessageFormatter
            formatter = MessageFormatter(self.session_id)
            message = formatter.format_age_group_selection(age_analysis)
            
            # Add clear warning
            message = "⚠️ **You've already selected the facility level!**\n\nPlease select an age group:\n\n" + message
            
            return {
                "success": True,
                "message": message,
                "session_id": self.session_id,
                "workflow": "tpr",
                "stage": "age_selection",
                "require_input": True  # Signal that we need user input
            }
        
        # Extract age group
        selected_age = self.extract_age_group(user_query)
        logger.info(f"🟣 Extracted age_group: {selected_age}")
        
        # Check if we didn't get a valid age group selection
        if selected_age == 'all_ages' and not any(word in user_query.lower() for word in ['1', 'all', 'ages', 'combined']):
            logger.warning(f"🟣 No valid age group detected in '{user_query}', re-prompting")
            # Re-prompt with clarification
            age_analysis = self.tpr_analyzer.analyze_age_groups(
                self.uploaded_data,
                self.tpr_selections.get('state', ''),
                self.tpr_selections.get('facility_level', 'primary')
            )
            from .formatters import MessageFormatter
            formatter = MessageFormatter(self.session_id)
            message = formatter.format_age_group_selection(age_analysis)
            message = "⚠️ Please select an age group by entering a number (1-3):\n\n" + message
            return {
                "success": True,
                "message": message,
                "session_id": self.session_id,
                "workflow": "tpr",
                "stage": "age_selection"
            }
        
        # Save selection
        self.tpr_selections['age_group'] = selected_age
        self.state_manager.save_tpr_selection('age_group', selected_age)
        logger.info(f"🟣 Saved age_group selection: {selected_age}")
        
        # Move to calculation stage
        self.current_stage = ConversationStage.TPR_CALCULATING
        self.state_manager.update_workflow_stage(self.current_stage)
        logger.info(f"🟣 Updated stage to TPR_CALCULATING")
        
        # Perform TPR calculation
        logger.info("🟣 About to call calculate_tpr()")
        result = self.calculate_tpr()
        logger.info(f"🟣 calculate_tpr returned: success={result.get('success')}")
        return result
    
    def calculate_tpr(self) -> Dict[str, Any]:
        """Calculate TPR using the full-featured tool."""
        logger.info(f"🟣 calculate_tpr() method called with selections: {self.tpr_selections}")
        
        import json
        import os
        import time
        from datetime import datetime
        from ..tools.tpr_analysis_tool import analyze_tpr_data
        
        # Start timing
        start_time = time.time()
        debug_info = {
            "timestamp": datetime.now().isoformat(),
            "session_id": self.session_id,
            "selections": self.tpr_selections,
            "stages": {}
        }
        
        logger.info("🟣 Imported analyze_tpr_data successfully")
        
        message = f"""✅ **Calculating TPR for {self.tpr_selections['state']}**
        
Settings:
- Facility Level: {self.tpr_selections['facility_level'].replace('_', ' ').title()}
- Age Group: {self.tpr_selections['age_group'].replace('_', ' ').title()}

Analyzing test data and generating visualizations..."""
        
        try:
            # Prepare options for the tool
            options = {
                'age_group': self.tpr_selections['age_group'],
                'facility_level': self.tpr_selections['facility_level'],
                'test_method': 'both'  # Always use maximum of both methods (WHO standard)
            }
            
            # Create graph state for the tool
            graph_state = {
                'session_id': self.session_id,
                'data_loaded': True,
                'data_file': f"instance/uploads/{self.session_id}/uploaded_data.csv"
            }
            
            # Save data to CSV for tool to access (if not already saved)
            data_path = f"instance/uploads/{self.session_id}/uploaded_data.csv"
            if not os.path.exists(data_path):
                self.uploaded_data.to_csv(data_path, index=False)
                logger.info(f"✅ Saved data to {data_path} for TPR tool")
                debug_info["stages"]["data_save"] = {"success": True, "path": data_path}
            else:
                logger.info(f"📋 Data already exists at {data_path}")
                debug_info["stages"]["data_save"] = {"success": True, "exists": True}
            
            # Call the tool with calculate_tpr action
            logger.info(f"🎯 Calling TPR tool with options: {options}")
            logger.info("🔍 DEBUG: About to invoke analyze_tpr_data tool")
            logger.info(f"🔍 DEBUG: Tool input - action: calculate_tpr, options: {json.dumps(options)}")
            
            result = analyze_tpr_data.invoke({
                'thought': f"Calculating TPR for {self.tpr_selections['state']} with user selections",
                'action': "calculate_tpr",
                'options': json.dumps(options),
                'graph_state': graph_state
            })
            
            tool_time = time.time() - start_time
            logger.info(f"✅ Tool invocation completed in {tool_time:.2f}s, result type: {type(result)}")
            logger.info(f"📋 Tool result preview: {result[:500] if result else 'None'}")
            debug_info["stages"]["tpr_calculation"] = {"success": True, "time": tool_time}
            
            # Format the tool results
            from .formatters import MessageFormatter
            formatter = MessageFormatter(self.session_id)
            message = formatter.format_tool_tpr_results(result)
            
            # Check for various output files
            tpr_results_path = os.path.join(self.session_folder, 'tpr_results.csv')
            raw_data_path = find_raw_data_file(self.session_folder)
            shapefile_path = os.path.join(self.session_folder, 'raw_shapefile.zip')
            map_path = os.path.join(self.session_folder, 'tpr_distribution_map.html')

            files_status = {
                "tpr_results.csv": os.path.exists(tpr_results_path),
                "raw_data": raw_data_path is not None,
                "raw_shapefile.zip": os.path.exists(shapefile_path),
                "tpr_distribution_map.html": os.path.exists(map_path)
            }
            
            logger.info(f"📂 Files created status: {json.dumps(files_status, indent=2)}")
            debug_info["stages"]["file_creation"] = files_status
            
            # Check if map was created and add to visualizations
            visualizations = []
            if os.path.exists(map_path):
                logger.info(f"✅ TPR map found at {map_path}, adding to visualizations")
                file_size = os.path.getsize(map_path)
                logger.info(f"📊 Map file size: {file_size} bytes")
                debug_info["stages"]["map_creation"] = {"success": True, "size": file_size}
                
                # Create visualization object like production does
                visualization = {
                    'type': 'iframe',
                    'url': f'/serve_viz_file/{self.session_id}/tpr_distribution_map.html',
                    'title': f'TPR Distribution - {self.tpr_selections.get("state", "State")}',
                    'height': 600
                }
                visualizations.append(visualization)
                
                # Remove iframe HTML from message if it exists
                if '<iframe' in message:
                    # Extract message before iframe tag
                    message = message.split('<iframe')[0].strip()
                    # Add note about map
                    if '📍' not in message:
                        message += "\n\n📍 TPR Map Visualization created (shown above)"
            else:
                logger.warning(f"❌ TPR map NOT found at {map_path}")
                debug_info["stages"]["map_creation"] = {"success": False, "error": "File not created"}
            
            # Update stage to TPR_COMPLETE but keep workflow ACTIVE
            # We need to wait for user's decision on risk analysis
            self.current_stage = ConversationStage.TPR_COMPLETE
            self.state_manager.update_workflow_stage(self.current_stage)
            
            # DO NOT mark workflow as complete yet - wait for user response
            logger.info("✅ Stage set to TPR_COMPLETE, workflow still ACTIVE waiting for user decision")
            
            # Save TPR completion flag for potential risk analysis
            self.state_manager.update_state({'tpr_completed': True})
            
            # Calculate total time
            total_time = time.time() - start_time
            debug_info["total_time"] = total_time
            logger.info(f"⏱️ Total TPR calculation time: {total_time:.2f}s")
            
            # Generate export documents for TPR results
            download_links = []
            try:
                logger.info(f"Generating TPR export documents for session {self.session_id}")
                
                # Check if CSV and shapefile exist
                if os.path.exists(raw_data_path):
                    download_links.append({
                        'url': f'/export/download/{self.session_id}/raw_data.csv',
                        'filename': 'raw_data.csv',
                        'description': '📊 TPR Analysis Results (CSV)',
                        'type': 'csv'
                    })
                    logger.info(f"✅ Added TPR CSV download link")
                
                if os.path.exists(shapefile_path):
                    download_links.append({
                        'url': f'/export/download/{self.session_id}/raw_shapefile.zip',
                        'filename': 'raw_shapefile.zip',
                        'description': '🗺️ Ward Boundaries Shapefile (ZIP)',
                        'type': 'zip'
                    })
                    logger.info(f"✅ Added shapefile download link")
                
                # Generate comprehensive HTML dashboard
                try:
                    from pathlib import Path
                    import pandas as pd
                    
                    # Read TPR results
                    if os.path.exists(tpr_results_path):
                        tpr_df = pd.read_csv(tpr_results_path)
                    elif raw_data_path:
                        tpr_df = read_raw_data(self.session_folder)
                    else:
                        tpr_df = None
                    
                    if tpr_df is not None:
                        # Create HTML dashboard
                        dashboard_html = f"""
                        <!DOCTYPE html>
                        <html>
                        <head>
                            <title>TPR Analysis Dashboard - {self.tpr_selections.get('state', 'State')}</title>
                            <style>
                                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                                h1 {{ color: #2c3e50; }}
                                .summary {{ background: #f8f9fa; padding: 15px; border-radius: 5px; margin: 20px 0; }}
                                .metrics {{ display: grid; grid-template-columns: repeat(3, 1fr); gap: 15px; }}
                                .metric {{ background: white; padding: 15px; border: 1px solid #dee2e6; border-radius: 5px; }}
                                .metric-value {{ font-size: 24px; font-weight: bold; color: #007bff; }}
                                table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
                                th, td {{ padding: 8px; text-align: left; border-bottom: 1px solid #dee2e6; }}
                                th {{ background: #f8f9fa; font-weight: bold; }}
                            </style>
                        </head>
                        <body>
                            <h1>TPR Analysis Dashboard</h1>
                            <div class="summary">
                                <h2>Analysis Summary</h2>
                                <div class="metrics">
                                    <div class="metric">
                                        <div>State</div>
                                        <div class="metric-value">{self.tpr_selections.get('state', 'N/A')}</div>
                                    </div>
                                    <div class="metric">
                                        <div>Facility Level</div>
                                        <div class="metric-value">{self.tpr_selections.get('facility_level', 'N/A')}</div>
                                    </div>
                                    <div class="metric">
                                        <div>Age Group</div>
                                        <div class="metric-value">{self.tpr_selections.get('age_group', 'N/A')}</div>
                                    </div>
                                    <div class="metric">
                                        <div>Total Wards</div>
                                        <div class="metric-value">{len(tpr_df) if 'WardName' in tpr_df.columns else 'N/A'}</div>
                                    </div>
                                    <div class="metric">
                                        <div>Average TPR</div>
                                        <div class="metric-value">{tpr_df['TPR'].mean():.2f}%</div>
                                    </div>
                                    <div class="metric">
                                        <div>Max TPR</div>
                                        <div class="metric-value">{tpr_df['TPR'].max():.2f}%</div>
                                    </div>
                                </div>
                            </div>
                            <h2>Top 10 Wards by TPR</h2>
                            <table>
                                <thead>
                                    <tr>
                                        <th>Ward Name</th>
                                        <th>TPR (%)</th>
                                        <th>Tests Positive</th>
                                        <th>Tests Examined</th>
                                    </tr>
                                </thead>
                                <tbody>
                        """
                        
                        # Add top 10 wards
                        top_wards = tpr_df.nlargest(10, 'TPR') if 'TPR' in tpr_df.columns else tpr_df.head(10)
                        for _, row in top_wards.iterrows():
                            dashboard_html += f"""
                                    <tr>
                                        <td>{row.get('WardName', 'N/A')}</td>
                                        <td>{row.get('TPR', 0):.2f}</td>
                                        <td>{row.get('Tests_Positive', 0):.0f}</td>
                                        <td>{row.get('Tests_Examined', 0):.0f}</td>
                                    </tr>
                            """
                        
                        dashboard_html += """
                                </tbody>
                            </table>
                        </body>
                        </html>
                        """
                        
                        # Save dashboard
                        dashboard_path = Path(self.session_folder) / 'tpr_dashboard.html'
                        dashboard_path.write_text(dashboard_html)
                        
                        download_links.append({
                            'url': f'/export/download/{self.session_id}/tpr_dashboard.html',
                            'filename': 'tpr_dashboard.html',
                            'description': '📈 Interactive TPR Dashboard (HTML)',
                            'type': 'html'
                        })
                        logger.info(f"✅ Generated TPR dashboard at {dashboard_path}")
                    
                except Exception as e:
                    logger.warning(f"Could not generate TPR dashboard: {e}")
                
                if download_links:
                    logger.info(f"✅ Generated {len(download_links)} export documents for TPR analysis")
                    
            except Exception as e:
                logger.error(f"Error generating TPR export documents: {e}")
                # Continue without exports - don't fail the main operation
            
            # Save debug info to file
            debug_file = os.path.join(self.session_folder, 'tpr_debug.json')
            with open(debug_file, 'w') as f:
                json.dump(debug_info, f, indent=2)
            logger.info(f"💾 Debug info saved to {debug_file}")
            
        except Exception as e:
            import traceback
            error_trace = traceback.format_exc()
            logger.error(f"❌ Error calculating TPR: {e}\n{error_trace}")
            
            debug_info["stages"]["error"] = {
                "message": str(e),
                "trace": error_trace
            }
            
            # Save error debug info
            debug_file = os.path.join(self.session_folder, 'tpr_error_debug.json')
            with open(debug_file, 'w') as f:
                json.dump(debug_info, f, indent=2)
            
            message = f"Error calculating TPR: {str(e)}"
            visualizations = []
            download_links = []
        
        # Add debug info to response for browser console visibility
        response = {
            "success": True,
            "message": message,
            "session_id": self.session_id,
            "workflow": "tpr",
            "stage": "complete",
            "visualizations": visualizations,
            "debug": {
                "selections": self.tpr_selections,
                "files_created": files_status if 'files_status' in locals() else {},
                "total_time": total_time if 'total_time' in locals() else 0,
                "has_map": len(visualizations) > 0,
                "debug_file": "tpr_debug.json"
            }
        }
        
        logger.info(f"📤 Returning response with debug info: {json.dumps(response['debug'], indent=2)}")
        return response
    
    def trigger_risk_analysis(self) -> Dict[str, Any]:
        """
        Transition from TPR workflow to standard data upload workflow.
        This mimics the behavior of uploading data through the standard Upload tab.
        """
        logger.info("Transitioning from TPR to standard upload workflow")
        
        # Check if files are ready
        import os
        import pandas as pd
        session_folder = f"instance/uploads/{self.session_id}"
        raw_data_path = find_raw_data_file(session_folder)

        if not raw_data_path:
            return {
                "success": False,
                "message": "Error: TPR data file not found. Please re-run TPR calculation.",
                "session_id": self.session_id
            }

        try:
            # Load the data that TPR created
            df = read_raw_data(session_folder)
            logger.info(f"Loaded TPR output data: {len(df)} rows, {len(df.columns)} columns")
            
            # Generate the standard upload workflow message
            # This matches what users see when they upload data through the Upload tab
            message = f"I've loaded your data from your region. It has {len(df)} rows and {len(df.columns)} columns.\n\n"
            
            # Add the standard "What would you like to do?" menu
            message += "## What would you like to do?\n\n"
            message += "- **Map variable distribution** - Visualize how variables are spread across wards\n"
            message += "- **Check data quality** - Validate your dataset\n"
            message += "- **Explore specific variables** - Dive into individual indicators\n"
            message += "- **Run malaria risk analysis** - Rank wards for ITN distribution\n"
            message += "- **Something else** - Ask me anything\n\n"
            message += "Just tell me what you're interested in."
            
            # Reset conversation stage to INITIAL (like a fresh upload)
            self.current_stage = ConversationStage.INITIAL
            self.state_manager.update_workflow_stage(self.current_stage)
            
            # Mark TPR workflow as complete and ensure data_loaded is set
            self.state_manager.mark_tpr_workflow_complete()
            logger.info("🔴 User accepted risk analysis - marking TPR workflow as COMPLETE")
            
            # Check if analysis was completed before transition
            from pathlib import Path
            analysis_marker = Path(f"instance/uploads/{self.session_id}/.analysis_complete")
            analysis_was_complete = analysis_marker.exists()
            
            self.state_manager.update_state({
                'tpr_completed': True,
                'data_loaded': True,  # CRITICAL: Set this for main workflow to recognize data
                'csv_loaded': True,   # Also set this for compatibility
                'workflow_transitioned': True,
                'analysis_complete': analysis_was_complete  # PRESERVE analysis state!
            })
            
            if analysis_was_complete:
                logger.info(f"📌 Preserving analysis_complete=True during transition for {self.session_id}")
            
            # Use WorkflowStateManager for proper transition
            from app.core.workflow_state_manager import WorkflowStateManager, WorkflowSource, WorkflowStage
            workflow_manager = WorkflowStateManager(self.session_id)
            
            # Perform workflow transition with proper cleanup
            # CRITICAL FIX: Don't delete .analysis_complete marker - it's evidence!
            success = workflow_manager.transition_workflow(
                from_source=WorkflowSource.DATA_ANALYSIS_V3,
                to_source=WorkflowSource.STANDARD,
                new_stage=WorkflowStage.DATA_PREPARED,
                clear_markers=['.data_analysis_mode']  # Only clear V3-specific markers
            )
            
            if not success:
                logger.error(f"Failed to transition workflow for session {self.session_id}")
            
            # Also update Flask session for immediate availability
            from flask import session
            session['csv_loaded'] = True
            session['shapefile_loaded'] = True
            session['data_loaded'] = True
            session['tpr_transition_complete'] = True
            session['previous_workflow'] = 'tpr'
            # Force Redis persistence
            session.permanent = True
            session.modified = True
            logger.info(f"✅ Completed workflow transition from Data Analysis V3 to Standard for session {self.session_id}")
            
            logger.info("Successfully transitioned to standard upload workflow")
            
            return {
                "success": True,
                "message": message,
                "session_id": self.session_id,
                "workflow": "data_upload",
                "stage": "complete",
                "transition": "tpr_to_upload",
                "exit_data_analysis_mode": True  # Signal frontend to exit Data Analysis mode
                # Don't send redirect_message - let user choose from the menu
            }
                
        except Exception as e:
            logger.error(f"Error transitioning to upload workflow: {e}")
            return {
                "success": False,
                "message": f"Error transitioning to data analysis: {str(e)}",
                "session_id": self.session_id
            }
    
    def extract_state_from_query(self, query: str) -> Optional[str]:
        """Extract state name from user query."""
        query_lower = query.lower().strip()
        
        # Check for number selection
        if query_lower in ['1', '1.', 'first']:
            # Would need to look up from analysis
            return None
        
        # Look for state names (simplified)
        return query.strip()
    
    def extract_facility_level(self, query: str) -> str:
        """Extract facility level from query."""
        query_lower = query.lower().strip()
        
        # Match exact numbers first, then text
        if query_lower == '1':
            return 'primary'
        elif 'primary' in query_lower:
            return 'primary'
        elif '2' in query_lower or 'secondary' in query_lower:
            return 'secondary'
        elif '3' in query_lower or 'tertiary' in query_lower:
            return 'tertiary'
        elif '4' in query_lower or 'all' in query_lower:
            return 'all'
        
        return 'primary'  # Default to primary
    
    def extract_age_group(self, query: str) -> str:
        """Extract age group from query."""
        query_lower = query.lower().strip()
        
        # Check for specific number selections first
        if query_lower == '1':
            return 'u5'  # Under 5 is typically option 1
        elif query_lower == '2':
            return 'o5'  # Over 5 is typically option 2
        elif query_lower == '3':
            return 'pw'  # Pregnant women is typically option 3
        elif query_lower == '4' or 'all' in query_lower or 'combined' in query_lower:
            return 'all_ages'
            
        # Check for text-based selections
        if 'under 5' in query_lower or 'u5' in query_lower or 'under_5' in query_lower:
            return 'u5'
        elif 'over 5' in query_lower or 'o5' in query_lower or 'over_5' in query_lower:
            return 'o5'
        elif 'pregnant' in query_lower or 'pw' in query_lower:
            return 'pw'
        
        return 'all_ages'  # Default only if explicitly mentioned or no match
