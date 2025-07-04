# app/web/routes/upload_routes.py
"""
Upload Routes module for file upload operations.

This module contains the file upload routes for the ChatMRPT web application:
- Modern dual file upload (CSV + shapefile)
- Legacy single file upload 
- Sample data loading
- File validation and processing logic
"""

import os
import uuid
import logging
from flask import Blueprint, session, request, current_app, jsonify
from werkzeug.utils import secure_filename

from ...core.decorators import handle_errors, log_execution_time, validate_session
from ...core.exceptions import ValidationError, DataProcessingError
from ...core.utils import convert_to_json_serializable
from ...core.responses import ResponseBuilder

logger = logging.getLogger(__name__)

# Create the upload routes blueprint
upload_bp = Blueprint('upload', __name__)

# File upload configurations
ALLOWED_EXTENSIONS_CSV = {'csv', 'txt', 'xlsx', 'xls'}  # Added Excel file extensions
ALLOWED_EXTENSIONS_SHP = {'zip'}  # Shapefiles are uploaded as ZIP files


def allowed_file(filename, allowed_extensions):
    """Check if file extension is allowed."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in allowed_extensions


class UploadTypeDetector:
    """
    Level 2: Upload Type Detection (Following Diagram Structure)
    Detects upload type according to diagram: CSV+Shapefile vs TPR-Only
    """
    
    TPR_INDICATORS = [
        'tpr', 'test_positivity', 'test_positivity_rate', 'rapid_diagnostic',
        'malaria_test_positive', 'positive_tests', 'tests_positive'
    ]
    
    def detect_upload_type(self, files: dict, csv_content=None) -> str:
        """
        Detect upload type according to diagram flow
        
        Returns:
        - 'csv_shapefile': Full Dataset Path (CSV + Shapefile)
        - 'tpr_only': TPR-Only Path (TPR data without shapefile)
        - 'invalid': Invalid upload combination
        """
        csv_file = files.get('csv_file')
        shapefile = files.get('shapefile')
        
        has_csv = csv_file and csv_file.filename != ''
        has_shapefile = shapefile and shapefile.filename != ''
        
        if has_csv and has_shapefile:
            return 'csv_shapefile'  # Full Dataset Path
        elif has_csv and not has_shapefile:
            # Check if it's TPR data
            if self._is_tpr_data(csv_content):
                return 'tpr_only'  # TPR-Only Path
            else:
                return 'csv_only'  # Regular CSV without shapefile
        else:
            return 'invalid'
    
    def _is_tpr_data(self, csv_content) -> bool:
        """Detect if CSV contains TPR-specific columns"""
        if not csv_content:
            return False
            
        try:
            import pandas as pd
            import io
            
            # Read first few rows to check columns
            csv_io = io.StringIO(csv_content.decode('utf-8'))
            sample_df = pd.read_csv(csv_io, nrows=5)
            column_names = [col.lower().replace(' ', '_') for col in sample_df.columns]
            
            # Check for TPR indicators in column names
            for indicator in self.TPR_INDICATORS:
                if any(indicator in col_name for col_name in column_names):
                    return True
            return False
            
        except Exception as e:
            logger.warning(f"Error detecting TPR data: {e}")
            return False
    
    def get_upload_summary(self, upload_type: str, file_info: dict) -> dict:
        """Generate upload type summary for user"""
        summaries = {
            'csv_shapefile': {
                'path': 'Full Dataset Path',
                'description': 'CSV data + Shapefile boundaries',
                'next_step': 'Store raw data and generate summary'
            },
            'tpr_only': {
                'path': 'TPR-Only Path', 
                'description': 'TPR data will be enhanced with climate variables',
                'next_step': 'Process TPR data and load Nigeria boundaries'
            },
            'csv_only': {
                'path': 'CSV-Only Path',
                'description': 'CSV data without geographic boundaries',
                'next_step': 'Data analysis without mapping capabilities'
            },
            'invalid': {
                'path': 'Invalid Upload',
                'description': 'No valid files detected',
                'next_step': 'Please upload valid CSV and/or shapefile'
            }
        }
        
        return {
            'upload_type': upload_type,
            'file_info': file_info,
            **summaries.get(upload_type, summaries['invalid'])
        }


@upload_bp.route('/upload_both_files', methods=['POST'])
@handle_errors
@validate_session
@log_execution_time
def upload_both_files():
    """
    Enhanced upload handler following diagram structure:
    Level 1: User Uploads Data → Level 2: Upload Type Detection → Level 3A/3B: Path Selection
    
    Supports: Full Dataset Path (CSV+Shapefile) and TPR-Only Path
    """
    session_id = session.get('session_id')
    
    # Level 1: User Uploads Data - Extract files from request
    csv_file = request.files.get('csv_file')
    shapefile = request.files.get('shapefile')
    
    # Basic file validation
    if csv_file and csv_file.filename == '':
        csv_file = None
    if shapefile and shapefile.filename == '':
        shapefile = None
        
    if not csv_file and not shapefile:
        raise ValidationError("No files selected for upload")
    
    # Level 2: Upload Type Detection (Following Diagram)
    detector = UploadTypeDetector()
    
    # Read CSV content for TPR detection if needed
    csv_content = None
    if csv_file:
        csv_content = csv_file.read()
        csv_file.seek(0)  # Reset file pointer for later use
    
    upload_type = detector.detect_upload_type(
        {'csv_file': csv_file, 'shapefile': shapefile}, 
        csv_content
    )
    
    file_info = {
        'csv_filename': csv_file.filename if csv_file else None,
        'shapefile_filename': shapefile.filename if shapefile else None,
        'session_id': session_id
    }
    
    upload_summary = detector.get_upload_summary(upload_type, file_info)
    
    logger.info(f"🔍 Upload Type Detected: {upload_type} for session {session_id}")
    logger.info(f"📊 Upload Summary: {upload_summary['description']}")
    
    # Route to appropriate path based on detection
    if upload_type == 'csv_shapefile':
        return handle_full_dataset_path(session_id, csv_file, shapefile, upload_summary)
    elif upload_type == 'tpr_only':
        return handle_tpr_only_path(session_id, csv_file, upload_summary)
    elif upload_type == 'csv_only':
        return handle_csv_only_path(session_id, csv_file, upload_summary)
    else:
        raise ValidationError(f"Invalid upload combination: {upload_summary['description']}")


def handle_full_dataset_path(session_id: str, csv_file, shapefile, upload_summary: dict):
    """
    Level 3A: Full Dataset Path (CSV + Shapefile)
    Level 4A: Store Raw Data (raw_data.csv, shapefile.zip, NO cleaning yet)
    Level 5A: Generate Dynamic Summary
    """
    # Create session folder for file storage
    session_folder = os.path.join(current_app.config['UPLOAD_FOLDER'], session_id)
    os.makedirs(session_folder, exist_ok=True)
    
    logger.info(f"📁 Starting Full Dataset Path for session {session_id}")
    
    # Level 4A: Store Raw Data (Following Diagram - NO cleaning yet)
    raw_storage_result = store_raw_data_files(session_folder, csv_file, shapefile)
    
    if raw_storage_result['status'] != 'success':
        raise DataProcessingError(f"Failed to store raw data: {raw_storage_result['message']}")
    
    # Level 5A: Generate Dynamic Summary (Following Diagram)
    summary_result = generate_dynamic_data_summary(session_id, session_folder, 'csv_shapefile')
    
    # Set session flags for next steps
    session['upload_type'] = 'csv_shapefile'
    session['raw_data_stored'] = True
    # session['should_describe_data'] = True  # DISABLED - Frontend handles this now
    session['should_ask_analysis_permission'] = True  # Trigger permission system
    
    # Return comprehensive result
    return jsonify({
        'status': 'success',
        'upload_type': 'csv_shapefile',
        'upload_summary': upload_summary,
        'raw_storage': raw_storage_result,
        'data_summary': summary_result,
        'message': f"Full dataset uploaded successfully. {raw_storage_result['files_stored']} files stored as raw data.",
        'next_step': 'Data summary will be presented for your review and permission.'
    })


def store_raw_data_files(session_folder: str, csv_file, shapefile):
    """
    Level 4A: Store Raw Data (Following Diagram)
    Store files exactly as uploaded - NO cleaning, NO processing, NO modifications
    """
    try:
        stored_files = []
        
        # Store CSV as raw_data.csv (preserving original data)
        if csv_file and allowed_file(csv_file.filename, ALLOWED_EXTENSIONS_CSV):
            raw_csv_path = os.path.join(session_folder, 'raw_data.csv')
            csv_file.save(raw_csv_path)
            stored_files.append('raw_data.csv')
            logger.info(f"✅ Stored raw CSV: {csv_file.filename} → raw_data.csv")
        
        # Store shapefile as raw_shapefile.zip (preserving original)
        if shapefile and allowed_file(shapefile.filename, ALLOWED_EXTENSIONS_SHP):
            raw_shp_path = os.path.join(session_folder, 'raw_shapefile.zip')
            shapefile.save(raw_shp_path)
            stored_files.append('raw_shapefile.zip')
            logger.info(f"✅ Stored raw shapefile: {shapefile.filename} → raw_shapefile.zip")
        
        return {
            'status': 'success',
            'message': 'Raw data files stored successfully',
            'files_stored': len(stored_files),
            'stored_files': stored_files,
            'preservation_note': 'Original files preserved without any modifications'
        }
        
    except Exception as e:
        logger.error(f"❌ Error storing raw data files: {e}")
        return {
            'status': 'error',
            'message': f'Failed to store raw data: {str(e)}'
        }


def generate_dynamic_data_summary(session_id: str, session_folder: str, upload_type: str):
    """
    Level 5A: Generate Dynamic Summary (Following Diagram)
    Row/column count, Data preview, Column type detection, Data completeness
    """
    try:
        import pandas as pd
        
        summary = {
            'session_id': session_id,
            'upload_type': upload_type,
            'analysis_timestamp': pd.Timestamp.now().isoformat()
        }
        
        # Analyze raw CSV data
        raw_csv_path = os.path.join(session_folder, 'raw_data.csv')
        if os.path.exists(raw_csv_path):
            raw_data = pd.read_csv(raw_csv_path)
            
            summary.update({
                'total_rows': len(raw_data),
                'total_columns': len(raw_data.columns),
                'column_names': raw_data.columns.tolist(),
                'preview_rows': raw_data.head(5).to_dict('records'),
                'column_types': detect_column_types(raw_data),
                'data_completeness': calculate_data_completeness(raw_data),
                'data_quality_assessment': assess_data_quality(raw_data)
            })
        
        # Analyze shapefile if present
        raw_shp_path = os.path.join(session_folder, 'raw_shapefile.zip')
        if os.path.exists(raw_shp_path):
            # Basic shapefile info (without processing)
            summary['shapefile_info'] = {
                'filename': 'raw_shapefile.zip',
                'size_mb': round(os.path.getsize(raw_shp_path) / (1024*1024), 2),
                'status': 'stored'
            }
        
        logger.info(f"📊 Generated dynamic summary for session {session_id}: {summary['total_rows']} rows, {summary['total_columns']} columns")
        
        return summary
        
    except Exception as e:
        logger.error(f"❌ Error generating dynamic summary: {e}")
        return {
            'status': 'error',
            'message': f'Failed to generate data summary: {str(e)}'
        }


def detect_column_types(df):
    """Detect and categorize column types for summary"""
    types = {}
    for col in df.columns:
        if df[col].dtype in ['int64', 'float64']:
            types[col] = 'numeric'
        elif df[col].dtype == 'object':
            # Check if it's categorical vs text
            unique_ratio = df[col].nunique() / len(df)
            if unique_ratio < 0.1:  # Less than 10% unique values
                types[col] = 'categorical'
            else:
                types[col] = 'text'
        else:
            types[col] = 'other'
    return types


def calculate_data_completeness(df):
    """Calculate completeness percentage for each column"""
    completeness = {}
    for col in df.columns:
        total_values = len(df)
        non_null_values = df[col].count()
        completeness[col] = round((non_null_values / total_values) * 100, 2)
    
    overall_completeness = round(df.count().sum() / (len(df) * len(df.columns)) * 100, 2)
    
    return {
        'by_column': completeness,
        'overall': overall_completeness
    }


def assess_data_quality(df):
    """Basic data quality assessment"""
    issues = []
    
    # Check for completely empty columns
    empty_cols = df.columns[df.isnull().all()].tolist()
    if empty_cols:
        issues.append(f"Empty columns detected: {empty_cols}")
    
    # Check for duplicate rows
    duplicate_count = df.duplicated().sum()
    if duplicate_count > 0:
        issues.append(f"Duplicate rows detected: {duplicate_count}")
    
    # Check for missing ward names (common issue)
    if 'ward_name' in df.columns or 'WardName' in df.columns:
        ward_col = 'ward_name' if 'ward_name' in df.columns else 'WardName'
        missing_wards = df[ward_col].isnull().sum()
        if missing_wards > 0:
            issues.append(f"Missing ward names: {missing_wards}")
    
    return {
        'issues_found': len(issues),
        'issues': issues,
        'quality_score': max(0, 100 - (len(issues) * 20))  # Simple scoring
    }


def handle_tpr_only_path(session_id: str, csv_file, upload_summary: dict):
    """
    Level 3B: TPR-Only Path (Will be implemented in Phase 2)
    For now, return a placeholder response
    """
    logger.info(f"🌍 TPR-Only path detected for session {session_id} - Phase 2 implementation")
    
    return jsonify({
        'status': 'success',
        'upload_type': 'tpr_only',
        'upload_summary': upload_summary,
        'message': 'TPR-only upload detected. Enhanced with climate data in Phase 2.',
        'phase': 'Phase 2 - TPR-GEE Integration',
        'next_step': 'TPR enhancement with Google Earth Engine will be implemented in Phase 2.'
    })


def handle_csv_only_path(session_id: str, csv_file, upload_summary: dict):
    """
    CSV-Only Path (Basic CSV without shapefile)
    Store raw CSV and generate summary
    """
    session_folder = os.path.join(current_app.config['UPLOAD_FOLDER'], session_id)
    os.makedirs(session_folder, exist_ok=True)
    
    logger.info(f"📊 CSV-Only path for session {session_id}")
    
    # Store raw CSV
    raw_storage_result = store_raw_csv_only(session_folder, csv_file)
    
    if raw_storage_result['status'] != 'success':
        raise DataProcessingError(f"Failed to store raw CSV: {raw_storage_result['message']}")
    
    # Generate summary
    summary_result = generate_dynamic_data_summary(session_id, session_folder, 'csv_only')
    
    # Set session flags
    session['upload_type'] = 'csv_only'
    session['raw_data_stored'] = True
    # session['should_describe_data'] = True  # DISABLED - Frontend handles this now
    session['should_ask_analysis_permission'] = True
    
    return jsonify({
        'status': 'success',
        'upload_type': 'csv_only',
        'upload_summary': upload_summary,
        'raw_storage': raw_storage_result,
        'data_summary': summary_result,
        'message': 'CSV uploaded successfully. Note: No mapping capabilities without shapefile.',
        'next_step': 'Data summary will be presented for analysis without geographic visualization.'
    })


def store_raw_csv_only(session_folder: str, csv_file):
    """Store raw CSV file only"""
    try:
        if csv_file and allowed_file(csv_file.filename, ALLOWED_EXTENSIONS_CSV):
            raw_csv_path = os.path.join(session_folder, 'raw_data.csv')
            csv_file.save(raw_csv_path)
            logger.info(f"✅ Stored raw CSV: {csv_file.filename} → raw_data.csv")
            
            return {
                'status': 'success',
                'message': 'Raw CSV file stored successfully',
                'files_stored': 1,
                'stored_files': ['raw_data.csv']
            }
        else:
            return {
                'status': 'error',
                'message': 'Invalid CSV file'
            }
            
    except Exception as e:
        logger.error(f"❌ Error storing raw CSV: {e}")
        return {
            'status': 'error',
            'message': f'Failed to store raw CSV: {str(e)}'
        }



# ================================================================
# PHASE 1 IMPLEMENTATION COMPLETE: Upload Type Detection & Raw Storage
# ================================================================
# ✅ Level 2: Upload Type Detection (csv_shapefile, tpr_only, csv_only)
# ✅ Level 3A: Full Dataset Path Handler
# ✅ Level 4A: Raw Data Storage (NO cleaning)
# ✅ Level 5A: Dynamic Summary Generation
# 
# Next Phase: Level 6-7 Summary Presentation & Permission System
# ================================================================

# === LEGACY ROUTES (kept for backward compatibility) ===@upload_bp.route('/upload', methods=['POST'])  @handle_errors@validate_session@log_execution_timedef upload():    """    Legacy upload route for backwards compatibility.    Redirects to upload_both_files function.    """    return upload_both_files()@upload_bp.route('/load_sample_data', methods=['POST'])@validate_session@handle_errors@log_execution_timedef load_sample_data():    """Load sample data for demonstration purposes."""    try:        session_id = session.get('session_id')        logger.info(f"Loading sample data for session {session_id}")                return jsonify({            'status': 'success',            'message': 'Sample data loading will be implemented in Phase 2'        })            except Exception as e:        logger.error(f"Error loading sample data: {e}")        return jsonify({            'status': 'error',            'message': f'Failed to load sample data: {str(e)}'        })def validate_csv_file(file_obj):    """    Validate CSV file structure and content.    """    try:        import pandas as pd        import io                # Read file content        content = file_obj.read().decode('utf-8')        file_obj.seek(0)  # Reset file pointer                # Try to parse as CSV        csv_io = io.StringIO(content)        df = pd.read_csv(csv_io, nrows=5)  # Read first 5 rows for validation                return {            'status': 'success',            'message': 'CSV file is valid',            'rows_sample': len(df),            'columns': len(df.columns)        }            except Exception as e:        return {            'status': 'error',            'message': f'Invalid CSV file: {str(e)}'        }def validate_shapefile(file_obj):    """    Validate shapefile ZIP structure.    """    try:        import zipfile                # Check if it's a valid ZIP file        if not zipfile.is_zipfile(file_obj):            return {                'status': 'error',                'message': 'File is not a valid ZIP archive'            }                return {            'status': 'success',            'message': 'Shapefile ZIP is valid'        }            except Exception as e:        return {            'status': 'error',            'message': f'Invalid shapefile: {str(e)}'        }def create_session_upload_folder(session_id):    """Create upload folder for session if it doesn't exist."""    try:        session_folder = os.path.join(current_app.config['UPLOAD_FOLDER'], session_id)        os.makedirs(session_folder, exist_ok=True)        return session_folder    except Exception as e:        logger.error(f"Error creating session folder: {e}")        raisedef cleanup_session_files(session_id, file_types=None):    """    Clean up uploaded files for a session.        Args:        session_id: Session identifier        file_types: List of file types to clean up (optional)            Returns:        dict: Cleanup result with status and message    """    try:        session_folder = os.path.join(current_app.config['UPLOAD_FOLDER'], session_id)                if not os.path.exists(session_folder):            return {'status': 'success', 'message': 'No files to clean up'}                files_removed = 0                if file_types:            # Remove specific file types            for filename in os.listdir(session_folder):                file_ext = filename.rsplit('.', 1)[-1].lower() if '.' in filename else ''                if file_ext in file_types:                    file_path = os.path.join(session_folder, filename)                    os.remove(file_path)                    files_removed += 1                    logger.info(f"Removed file: {filename}")        else:            # Remove all files in session folder            for filename in os.listdir(session_folder):                file_path = os.path.join(session_folder, filename)                if os.path.isfile(file_path):                    os.remove(file_path)                    files_removed += 1                    logger.info(f"Removed file: {filename}")                        # Remove empty directory            if not os.listdir(session_folder):                os.rmdir(session_folder)                logger.info(f"Removed empty session folder: {session_folder}")                return {'status': 'success', 'message': f'Cleaned up {files_removed} files'}            except Exception as e:        logger.error(f"Error cleaning up files for session {session_id}: {e}")        return {'status': 'error', 'message': f'File cleanup failed: {str(e)}'}