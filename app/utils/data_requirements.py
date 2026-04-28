"""
Data Requirements Helper Module

Provides clear guidance on data format requirements and pre-upload validation.
Helps users understand what data is needed before attempting uploads.
"""

import logging
import pandas as pd
from typing import Dict, Any, List, Tuple, Optional
from pathlib import Path

logger = logging.getLogger(__name__)


class DataRequirementsHelper:
    """Helper class for data requirements and validation."""

    def __init__(self):
        """Initialize the data requirements helper."""
        # Define required and optional columns
        self.required_columns = {
            'WardName': {
                'type': 'text',
                'description': 'Name of the ward/administrative unit',
                'examples': ['Ward Alpha', 'Tudun Wada', 'Fagge']
            },
            'population': {
                'type': 'numeric',
                'description': 'Total population of the ward',
                'examples': [12500, 28000, 45000],
                'alternatives': ['Population', 'pop', 'total_population']
            }
        }

        self.recommended_columns = {
            'tpr': {
                'type': 'numeric',
                'description': 'Test Positivity Rate (%)',
                'examples': [15.5, 28.2, 35.9],
                'alternatives': ['TPR', 'test_positivity_rate']
            },
            'urbanPercentage': {
                'type': 'numeric',
                'description': 'Percentage of urban area',
                'examples': [10.5, 65.8, 88.1],
                'alternatives': ['urban_percentage', 'urban_pct', 'urbanization']
            },
            'avg_rainfall': {
                'type': 'numeric',
                'description': 'Average rainfall (mm)',
                'examples': [85.2, 110.5, 125.8],
                'alternatives': ['rainfall', 'precipitation']
            },
            'ndvi_score': {
                'type': 'numeric',
                'description': 'Normalized Difference Vegetation Index',
                'examples': [0.65, 0.75, 0.82],
                'alternatives': ['ndvi', 'vegetation_index']
            },
            'dist_to_clinic': {
                'type': 'numeric',
                'description': 'Distance to nearest health facility (meters)',
                'examples': [1200, 550, 3100],
                'alternatives': ['distance_to_health', 'clinic_distance']
            }
        }

        self.shapefile_requirements = {
            'required_files': ['.shp', '.shx', '.dbf'],
            'optional_files': ['.prj', '.cpg', '.qpj'],
            'format': 'ZIP archive',
            'max_size_mb': 100
        }

    def get_data_requirements_summary(self) -> Dict[str, Any]:
        """
        Get a comprehensive summary of data requirements.

        Returns:
            Dictionary with all data requirements
        """
        return {
            'csv_requirements': {
                'required_columns': self._format_column_requirements(self.required_columns),
                'recommended_columns': self._format_column_requirements(self.recommended_columns),
                'file_formats': ['CSV', 'Excel (.xlsx, .xls)'],
                'encoding': 'UTF-8 recommended',
                'max_size_mb': 32
            },
            'shapefile_requirements': self.shapefile_requirements,
            'sample_data_available': True,
            'validation_available': True
        }

    def _format_column_requirements(self, columns: Dict) -> List[Dict]:
        """Format column requirements for display."""
        formatted = []
        for col_name, col_info in columns.items():
            formatted.append({
                'name': col_name,
                'type': col_info['type'],
                'description': col_info['description'],
                'examples': col_info.get('examples', []),
                'alternatives': col_info.get('alternatives', [])
            })
        return formatted

    def validate_dataframe(self, df: pd.DataFrame) -> Tuple[bool, List[str], List[str]]:
        """
        Validate a dataframe against requirements.

        Args:
            df: DataFrame to validate

        Returns:
            Tuple of (is_valid, errors, warnings)
        """
        errors = []
        warnings = []

        # Check for required columns
        for col_name, col_info in self.required_columns.items():
            if not self._find_column(df, col_name, col_info.get('alternatives', [])):
                errors.append(f"Missing required column: '{col_name}' (or alternatives: {col_info.get('alternatives', [])})")

        # Check for recommended columns
        for col_name, col_info in self.recommended_columns.items():
            if not self._find_column(df, col_name, col_info.get('alternatives', [])):
                warnings.append(f"Missing recommended column: '{col_name}' - {col_info['description']}")

        # Check data types
        for col in df.columns:
            if df[col].dtype == 'object':
                # Check if it should be numeric
                for req_col, req_info in {**self.required_columns, **self.recommended_columns}.items():
                    if self._matches_column(col, req_col, req_info.get('alternatives', [])):
                        if req_info['type'] == 'numeric':
                            try:
                                pd.to_numeric(df[col], errors='coerce')
                                warnings.append(f"Column '{col}' should be numeric but is text. Will attempt conversion.")
                            except:
                                errors.append(f"Column '{col}' should be numeric but contains non-numeric values")

        # Check for minimum rows
        if len(df) < 2:
            errors.append("Dataset must contain at least 2 wards/rows")

        # Check for duplicate ward names
        ward_col = self._find_ward_column(df)
        if ward_col and df[ward_col].duplicated().any():
            warnings.append(f"Duplicate ward names found in column '{ward_col}'")

        is_valid = len(errors) == 0
        return is_valid, errors, warnings

    def _find_column(self, df: pd.DataFrame, target: str, alternatives: List[str]) -> Optional[str]:
        """Find a column by name or alternatives."""
        # Check exact match (case-insensitive)
        for col in df.columns:
            if col.lower() == target.lower():
                return col

        # Check alternatives
        for alt in alternatives:
            for col in df.columns:
                if col.lower() == alt.lower():
                    return col

        return None

    def _matches_column(self, col_name: str, target: str, alternatives: List[str]) -> bool:
        """Check if column name matches target or alternatives."""
        if col_name.lower() == target.lower():
            return True
        for alt in alternatives:
            if col_name.lower() == alt.lower():
                return True
        return False

    def _find_ward_column(self, df: pd.DataFrame) -> Optional[str]:
        """Find the ward name column."""
        return self._find_column(df, 'WardName', ['ward', 'ward_name', 'location', 'area'])

    def get_validation_report(self, file_path: str) -> Dict[str, Any]:
        """
        Generate a validation report for an uploaded file.

        Args:
            file_path: Path to the uploaded file

        Returns:
            Validation report with detailed feedback
        """
        try:
            # Read the file
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
            elif file_path.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file_path)
            else:
                return {
                    'valid': False,
                    'errors': ['Unsupported file format. Please use CSV or Excel.'],
                    'warnings': []
                }

            # Validate
            is_valid, errors, warnings = self.validate_dataframe(df)

            # Generate detailed report
            report = {
                'valid': is_valid,
                'errors': errors,
                'warnings': warnings,
                'statistics': {
                    'rows': len(df),
                    'columns': len(df.columns),
                    'column_names': list(df.columns),
                    'numeric_columns': list(df.select_dtypes(include=['number']).columns),
                    'text_columns': list(df.select_dtypes(include=['object']).columns)
                },
                'recommendations': self._get_recommendations(df, errors, warnings)
            }

            return report

        except Exception as e:
            logger.error(f"Error validating file: {e}")
            return {
                'valid': False,
                'errors': [f"Error reading file: {str(e)}"],
                'warnings': [],
                'recommendations': ["Ensure file is a valid CSV or Excel file"]
            }

    def _get_recommendations(self, df: pd.DataFrame, errors: List[str], warnings: List[str]) -> List[str]:
        """Generate recommendations based on validation results."""
        recommendations = []

        if errors:
            recommendations.append("📌 Fix the errors above before proceeding with analysis")

        if 'Missing required column' in str(errors):
            recommendations.append("📊 Ensure your data has 'WardName' and 'population' columns at minimum")

        if warnings and 'recommended column' in str(warnings):
            recommendations.append("💡 Adding recommended columns will improve analysis accuracy")

        if len(df) < 10:
            recommendations.append("📈 Consider adding more wards for better statistical analysis")

        if not errors and not warnings:
            recommendations.append("✅ Your data looks great! Ready for analysis")

        return recommendations

    def format_requirements_message(self) -> str:
        """
        Format data requirements as a user-friendly message.

        Returns:
            Formatted message string
        """
        lines = []
        lines.append("## 📋 Data Requirements for ChatMRPT\n")

        lines.append("### CSV/Excel File Requirements:\n")
        lines.append("**Required Columns:**")
        for col_name, col_info in self.required_columns.items():
            lines.append(f"• **{col_name}** ({col_info['type']}) - {col_info['description']}")
            if col_info.get('alternatives'):
                lines.append(f"  Alternative names: {', '.join(col_info['alternatives'])}")

        lines.append("\n**Recommended Columns for Better Analysis:**")
        for col_name, col_info in self.recommended_columns.items():
            lines.append(f"• **{col_name}** ({col_info['type']}) - {col_info['description']}")

        lines.append("\n### Shapefile Requirements (for mapping):\n")
        lines.append(f"• Format: {self.shapefile_requirements['format']}")
        lines.append(f"• Required files: {', '.join(self.shapefile_requirements['required_files'])}")
        lines.append(f"• Maximum size: {self.shapefile_requirements['max_size_mb']}MB")

        lines.append("\n### 💡 Tips:")
        lines.append("• Column names are case-insensitive")
        lines.append("• Missing recommended columns won't prevent analysis")
        lines.append("• Use our sample data to see the expected format")
        lines.append("• You can validate your data before uploading")

        return "\n".join(lines)

    def get_sample_csv_content(self) -> str:
        """
        Generate sample CSV content showing the expected format.

        Returns:
            Sample CSV content as string
        """
        sample = """WardName,population,tpr,urbanPercentage,avg_rainfall,ndvi_score,dist_to_clinic
Ward Alpha,12500,15.5,10.5,85.2,0.65,1200
Ward Beta,28000,28.2,65.8,110.5,0.75,550
Ward Gamma,9500,12.1,5.2,78.9,0.55,2500
Ward Delta,42000,35.9,88.1,105.1,0.80,300
Ward Epsilon,15000,18.3,25.0,92.5,0.68,1500"""
        return sample

    def check_file_before_upload(self, filename: str, file_size: int) -> Tuple[bool, str]:
        """
        Quick check before file upload.

        Args:
            filename: Name of the file
            file_size: Size in bytes

        Returns:
            Tuple of (is_ok, message)
        """
        # Check file extension
        if filename.endswith('.csv'):
            max_size = 32 * 1024 * 1024  # 32MB
            file_type = "CSV"
        elif filename.endswith(('.xlsx', '.xls')):
            max_size = 32 * 1024 * 1024  # 32MB
            file_type = "Excel"
        elif filename.endswith('.zip'):
            max_size = 100 * 1024 * 1024  # 100MB
            file_type = "Shapefile (ZIP)"
        else:
            return False, f"Unsupported file type. Please upload CSV, Excel, or ZIP (shapefile) files."

        # Check file size
        if file_size > max_size:
            size_mb = max_size / (1024 * 1024)
            return False, f"{file_type} file exceeds maximum size of {size_mb}MB"

        return True, f"{file_type} file accepted"