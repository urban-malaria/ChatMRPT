# Patch to fix session updates in streaming endpoint
# This file contains the corrected generate() function

def generate():
    try:
        with app.app_context():
            logger.info(f"Processing streaming message: '{user_message[:100]}...'")
            
            # Use the real streaming system for proper formatting
            logger.info("Using real streaming system with proper line break preservation")
            
            # Track streaming result for logging
            final_chunk = None
            response_content = ""
            tools_used = []
            
            # Use the actual streaming method
            for chunk in request_interpreter.process_message_streaming(user_message, session_id, session_data):
                # Accumulate content for logging
                if chunk.get('content'):
                    response_content += chunk.get('content', '')
                
                # Track tools used
                if chunk.get('tools_used'):
                    tools_used.extend(chunk.get('tools_used', []))
                
                # Track final chunk
                if chunk.get('done'):
                    final_chunk = chunk
                    
                chunk_json = json.dumps(chunk)
                logger.debug(f"Sending streaming chunk: {chunk_json}")
                yield f"data: {chunk_json}\n\n"
            
            # CRITICAL: Update session state after streaming completes
            # This must happen in the request context
            from flask import session as flask_session
            if tools_used:
                # Update session state based on tools used
                if any(tool in tools_used for tool in ['run_composite_analysis', 'run_pca_analysis', 'runcompleteanalysis']):
                    flask_session['analysis_complete'] = True
                    if 'runcompleteanalysis' in tools_used:
                        flask_session['analysis_type'] = 'dual_method'
                    elif 'run_composite_analysis' in tools_used:
                        flask_session['analysis_type'] = 'composite'
                    else:
                        flask_session['analysis_type'] = 'pca'
                    # CRITICAL: Mark session as modified
                    flask_session.modified = True
                    logger.info(f"Session {session_id}: Analysis completed via streaming, session updated")
                
                # Clear any pending actions if analysis was run
                if any(tool in tools_used for tool in ['run_composite_analysis', 'run_pca_analysis', 'runcompleteanalysis']):
                    flask_session.pop('pending_action', None)
                    flask_session.pop('pending_variables', None)
                    flask_session.modified = True
            
            # Log completion using final chunk data
            if final_chunk:
                tools_used_final = final_chunk.get('tools_used', [])
                if any(tool in tools_used_final for tool in ['run_composite_analysis', 'run_pca_analysis', 'runcompleteanalysis']):
                    if 'runcompleteanalysis' in tools_used_final:
                        analysis_type = 'dual_method'
                    elif 'run_composite_analysis' in tools_used_final:
                        analysis_type = 'composite'
                    else:
                        analysis_type = 'pca'
                    logger.info(f"Session {session_id}: Analysis completed via streaming ({analysis_type})")
                
                # Simplified logging for streaming
                if hasattr(app, 'services') and app.services.interaction_logger:
                    interaction_logger = app.services.interaction_logger
                    interaction_logger.log_message(
                        session_id=session_id,
                        sender='assistant',
                        content=response_content,
                        intent=final_chunk.get('intent_type', 'streaming'),
                        entities={
                            'streaming': True,
                            'tools_used': tools_used_final,
                            'status': final_chunk.get('status', 'success')
                        }
                    )
                    
    except Exception as e:
        logger.error(f"Error in streaming processing: {e}")
        error_json = json.dumps({'content': f'Error: {str(e)}', 'status': 'error', 'done': True})
        yield f"data: {error_json}\n\n"