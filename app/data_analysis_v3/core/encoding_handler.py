"""
Encoding Handler for Data Analysis V3
Fully dynamic encoding detection and fixing - no hardcoding
"""

import pandas as pd
import logging
from pathlib import Path
from typing import Optional, Dict, Any, Tuple, List
import chardet

# Try to import ftfy for automatic text fixing
try:
    import ftfy
    FTFY_AVAILABLE = True
except ImportError:
    FTFY_AVAILABLE = False

logger = logging.getLogger(__name__)


class EncodingHandler:
    """Handles encoding issues dynamically without hardcoding."""
    
    @staticmethod
    def detect_encoding(file_path: str) -> str:
        """
        Detect the encoding of a file using chardet.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Detected encoding
        """
        try:
            # Read a sample of the file for detection
            with open(file_path, 'rb') as f:
                raw_data = f.read(100000)  # Read up to 100KB for better detection
                
            # Use chardet to detect encoding
            result = chardet.detect(raw_data)
            encoding = result.get('encoding', 'utf-8')
            confidence = result.get('confidence', 0)
            
            logger.info(f"Detected encoding: {encoding} (confidence: {confidence:.2f})")
            
            # If low confidence, try alternative detection methods
            if confidence < 0.8:
                # Try reading with different encodings to see what works
                test_encodings = ['utf-8', 'windows-1252', 'iso-8859-1', 'cp1252', 'latin1']
                for test_enc in test_encodings:
                    try:
                        raw_data.decode(test_enc)
                        logger.info(f"Alternative encoding {test_enc} seems to work")
                        return test_enc
                    except:
                        continue
                        
            return encoding if encoding else 'utf-8'
            
        except Exception as e:
            logger.warning(f"Could not detect encoding: {e}, using utf-8")
            return 'utf-8'
    
    @staticmethod
    def fix_text_encoding(text: str) -> str:
        """
        Fix encoding issues in text using automatic detection.
        
        Args:
            text: Text that may have encoding issues
            
        Returns:
            Fixed text
        """
        if not text or not isinstance(text, str):
            return text
            
        # Method 1: Use ftfy if available (best automatic fixer)
        if FTFY_AVAILABLE:
            try:
                fixed = ftfy.fix_text(text)
                if fixed != text:
                    logger.debug(f"ftfy fixed text: '{text[:50]}...' -> '{fixed[:50]}...'")
                return fixed
            except Exception as e:
                logger.debug(f"ftfy failed: {e}")
        
        # Method 2: Try to detect and fix mojibake automatically
        # Check for common signs of encoding issues
        mojibake_indicators = ['Ã', 'â€', 'Â', 'ï¿½']
        has_mojibake = any(indicator in text for indicator in mojibake_indicators)
        
        if has_mojibake:
            # Try different encoding/decoding combinations
            encoding_pairs = [
                ('windows-1252', 'utf-8'),
                ('iso-8859-1', 'utf-8'),
                ('cp1252', 'utf-8'),
                ('latin1', 'utf-8'),
            ]
            
            for decode_from, encode_to in encoding_pairs:
                try:
                    # Attempt to fix by re-encoding
                    fixed = text.encode(decode_from, errors='ignore').decode(encode_to, errors='ignore')
                    
                    # Check if the fix improved things (no more mojibake indicators)
                    if not any(indicator in fixed for indicator in mojibake_indicators):
                        logger.debug(f"Fixed encoding: {decode_from} -> {encode_to}")
                        return fixed
                except:
                    continue
        
        return text
    
    @staticmethod
    def fix_column_names(df: pd.DataFrame) -> pd.DataFrame:
        """
        Fix encoding issues in all column names.
        
        Args:
            df: DataFrame with potentially corrupted column names
            
        Returns:
            DataFrame with fixed column names
        """
        # Apply automatic fixing to all column names
        new_columns = []
        changed_count = 0
        
        for col in df.columns:
            fixed_col = EncodingHandler.fix_text_encoding(str(col))
            
            if fixed_col != col:
                logger.info(f"Fixed column: '{col[:50]}...' -> '{fixed_col[:50]}...'")
                changed_count += 1
            
            new_columns.append(fixed_col)
        
        if changed_count > 0:
            df.columns = new_columns
            logger.info(f"Fixed {changed_count} column names with encoding issues")
        
        return df
    
    @staticmethod
    def read_csv_with_encoding(
        file_path: str, 
        nrows: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Read CSV file with automatic encoding detection and fixing.
        
        Args:
            file_path: Path to CSV file
            nrows: Number of rows to read (optional)
            
        Returns:
            DataFrame with fixed encoding
        """
        # Step 1: Detect the file encoding
        detected_encoding = EncodingHandler.detect_encoding(file_path)
        
        # Step 2: Try to read with detected encoding
        df = None
        encodings_to_try = [detected_encoding, 'utf-8', 'windows-1252', 'iso-8859-1', 'cp1252']
        encodings_to_try = list(dict.fromkeys(encodings_to_try))  # Remove duplicates
        
        for encoding in encodings_to_try:
            try:
                df = pd.read_csv(file_path, encoding=encoding, nrows=nrows)
                logger.info(f"Successfully read {file_path} with {encoding} encoding")
                break
            except UnicodeDecodeError:
                continue
            except Exception as e:
                logger.debug(f"Failed to read with {encoding}: {e}")
                continue
        
        if df is None:
            # Last resort: read with error handling
            df = pd.read_csv(file_path, encoding='utf-8', errors='ignore', nrows=nrows)
            logger.warning(f"Read {file_path} with utf-8 and ignored errors")
        
        # Step 3: Fix column names
        df = EncodingHandler.fix_column_names(df)
        
        # Step 4: Fix string data in the DataFrame
        for col in df.columns:
            if df[col].dtype == 'object':
                try:
                    df[col] = df[col].apply(lambda x: EncodingHandler.fix_text_encoding(str(x)) if pd.notna(x) else x)
                except:
                    pass  # Skip if column can't be fixed
        
        return df
    
    @staticmethod
    def read_excel_with_encoding(
        file_path: str,
        sheet_name: Optional[Any] = 0,
        nrows: Optional[int] = None,
        header: int = 0,
    ) -> pd.DataFrame:
        """
        Read Excel file with automatic encoding fixing.

        Args:
            file_path: Path to Excel file
            sheet_name: Sheet to read
            nrows: Number of rows to read
            header: Row number to use as column names (0-indexed).
                    Pass the value returned by LLM schema inference when the file
                    has a blank first row (e.g. DHIS2 exports use header=1).

        Returns:
            DataFrame with fixed encoding
        """
        # Excel files handle encoding differently, but we still fix the content
        df = pd.read_excel(file_path, sheet_name=sheet_name, nrows=nrows, header=header)

        # Fix column names
        df = EncodingHandler.fix_column_names(df)
        
        # Fix string data
        for col in df.columns:
            if df[col].dtype == 'object':
                try:
                    df[col] = df[col].apply(lambda x: EncodingHandler.fix_text_encoding(str(x)) if pd.notna(x) else x)
                except:
                    pass

        return df


def find_raw_data_file(session_folder: str) -> Optional[str]:
    """
    Find the raw data file in a session folder (csv or xlsx).

    Args:
        session_folder: Path to the session folder

    Returns:
        Path to the raw data file, or None if not found
    """
    for ext in ['csv', 'xlsx', 'xls']:
        path = Path(session_folder) / f'raw_data.{ext}'
        if path.exists():
            return str(path)
    return None


def read_raw_data(session_folder: str, nrows: Optional[int] = None) -> pd.DataFrame:
    """
    Find and read the raw data file from a session folder.

    Uses EncodingHandler methods to properly handle encoding issues.

    Args:
        session_folder: Path to the session folder
        nrows: Number of rows to read (optional)

    Returns:
        DataFrame with the raw data

    Raises:
        FileNotFoundError: If no raw data file exists in the session folder
    """
    filepath = find_raw_data_file(session_folder)
    if not filepath:
        raise FileNotFoundError(f"No raw data file found in {session_folder}")

    ext = Path(filepath).suffix.lower()
    if ext in ('.xlsx', '.xls'):
        return EncodingHandler.read_excel_with_encoding(filepath, nrows=nrows)
    else:
        return EncodingHandler.read_csv_with_encoding(filepath, nrows=nrows)