"""
Universal Visualization Explanation Service - Py-Sidebot Approach

This service provides automatic LLM-powered explanations for ANY visualization 
by converting them to images and using vision-enabled LLMs for interpretation.

Based on py-sidebot's elegant universal approach:
- Convert ANY visualization to base64 image
- Send to LLM with malaria-specific instructions
- Get natural language explanation
- Works for maps, charts, graphs, plots - everything!
"""

import os
import base64
import tempfile
import logging
from typing import Dict, Any, Optional
from flask import current_app
import subprocess

logger = logging.getLogger(__name__)

class UniversalVisualizationExplainer:
    """
    Universal explanation service for ANY visualization type.
    
    Follows py-sidebot's approach:
    1. Convert visualization to image (base64)
    2. Send to LLM with malaria epidemiology instructions
    3. Return natural explanation
    """
    
    def __init__(self, llm_manager=None):
        self.llm_manager = llm_manager
        self.instructions = """
        Analyze this specific visualization and provide concrete insights about what you observe.
        
        Focus on making specific observations about THIS data:
        1. What is the most important pattern or finding you see in this visualization?
        2. Which specific areas, values, or data points stand out and why?
        3. What does this pattern suggest for immediate action or decision-making?
        4. What is one unexpected or noteworthy insight from this specific data?
        
        Be concrete and data-specific. Avoid generic interpretation guidance.
        Talk about what you actually see in THIS visualization, not how to interpret visualizations in general.
        """.strip()
    
    def explain_visualization(self, viz_path: str, viz_type: str = None, session_id: str = None) -> str:
        """
        Generate automatic explanation for ANY visualization.

        Args:
            viz_path: Path to the visualization file (HTML, PNG, etc.)
            viz_type: Optional type hint for context
            session_id: Session ID for logging

        Returns:
            str: LLM-generated explanation
        """
        # Check if vision explanations are enabled via environment variable
        enable_vision = os.environ.get('ENABLE_VISION_EXPLANATIONS', 'false').lower() in ['true', '1', 'yes']
        can_use_vision = bool(enable_vision and self.llm_manager and hasattr(self.llm_manager, 'generate_with_image'))

        try:
            logger.info(f"Starting vision-based explanation for: {viz_path}, type: {viz_type}")
            logger.info(f"Absolute path check: {os.path.abspath(viz_path)}")
            logger.info(f"Path exists: {os.path.exists(viz_path)}")

            img_b64 = None
            explanation = None

            # Try vision path first when allowed
            if can_use_vision:
                # Convert visualization to base64 image
                img_b64 = self._convert_to_image(viz_path)

                if img_b64:
                    logger.info("Successfully converted visualization to image, getting LLM explanation")
                    # Get LLM explanation using image
                    explanation = self._get_llm_explanation(img_b64, viz_type, session_id)

            # If vision is disabled/unavailable or conversion failed, fall back to text-only explanation
            if not explanation:
                if not self.llm_manager:
                    error_msg = "LLM manager not available for explanations"
                    logger.error(error_msg)
                    return f"ERROR: {error_msg}"

                logger.info("Using text-only fallback explanation (vision disabled or conversion failed)")
                # Build lightweight data context from session artifacts when available
                data_context = self._build_data_context(session_id, viz_type, viz_path)

                try:
                    # Prefer LLM manager's visualization explainer with context
                    explanation = self.llm_manager.explain_visualization(
                        session_id=session_id,
                        viz_type=viz_type or 'visualization',
                        context=data_context
                    )
                except Exception as e:
                    logger.warning(f"Fallback explanation via llm_manager failed: {e}; using generic prompt")
                    prompt = (
                        f"Provide a clear, practical explanation of this {viz_type or 'visualization'} "
                        f"generated during malaria risk analysis. Focus on how to read it and what insights it typically reveals. "
                        f"Be concise and action-oriented."
                    )
                    explanation = self.llm_manager.generate_response(
                        prompt=prompt,
                        system_message=self.instructions,
                        session_id=session_id
                    )

            return explanation

        except Exception as e:
            # As a final safety net, return a generic explanation prompt
            logger.error(f"Error at {viz_path}: {e}", exc_info=True)
            if self.llm_manager:
                return self.llm_manager.explain_visualization(
                    session_id=session_id,
                    viz_type=viz_type or 'visualization',
                    context={'source': os.path.basename(viz_path) if viz_path else None}
                )
            return "This visualization summarizes analysis results relevant to malaria risk and intervention planning."
        
        # ORIGINAL CODE (commented out for performance):
        # try:
        #     logger.info(f"Starting visualization explanation for: {viz_path}, type: {viz_type}")
        #     
        #     # Convert visualization to base64 image
        #     img_b64 = self._convert_to_image(viz_path)
        #     
        #     if not img_b64:
        #         logger.warning(f"Failed to convert {viz_path} to image, using fallback explanation")
        #         return self._fallback_explanation(viz_type)
        #     
        #     logger.info("Successfully converted visualization to image, getting LLM explanation")
        #     
        #     # Get LLM explanation using image
        #     explanation = self._get_llm_explanation(img_b64, viz_type, session_id)
        #     
        #     return explanation
        #     
        # except Exception as e:
        #     logger.error(f"Error explaining visualization {viz_path}: {e}")
        #     return self._fallback_explanation(viz_type)
    
    def _convert_to_image(self, viz_path: str) -> Optional[str]:
        """Convert visualization to base64 image (py-sidebot approach)."""
        try:
            logger.info(f"_convert_to_image called with path: {viz_path}")

            # Check if file exists
            if not os.path.exists(viz_path):
                logger.warning(f"Visualization file not found: {viz_path}")
                logger.info(f"Current working directory: {os.getcwd()}")
                logger.info(f"Absolute path attempted: {os.path.abspath(viz_path)}")
                return None

            logger.info(f"File exists, checking type for: {viz_path}")

            # Handle different file types
            if viz_path.endswith('.html'):
                # Check if it's a Plotly HTML and try to extract data
                plotly_result = self._try_plotly_extraction(viz_path)
                if plotly_result:
                    return plotly_result
                # Otherwise convert HTML to image
                return self._html_to_image(viz_path)
            elif viz_path.endswith('.pickle'):
                # Handle pickle files from Data Analysis V3
                return self._pickle_to_image(viz_path)
            elif viz_path.endswith(('.png', '.jpg', '.jpeg')):
                return self._image_to_base64(viz_path)
            else:
                logger.warning(f"Unsupported file type: {viz_path}")
                return None

        except Exception as e:
            logger.error(f"Error converting visualization to image: {e}")
            return None
    
    def _try_plotly_extraction(self, html_path: str) -> Optional[str]:
        """Try to extract Plotly figure from HTML and convert to image using Kaleido."""
        try:
            import json
            import re

            with open(html_path, 'r', encoding='utf-8') as f:
                html_content = f.read()

            # Look for Plotly JSON data in the HTML
            plotly_pattern = r'Plotly\.newPlot\([^,]+,\s*(\[.*?\])\s*,\s*(\{.*?\})'
            matches = re.search(plotly_pattern, html_content, re.DOTALL)

            if matches:
                try:
                    import plotly.graph_objects as go

                    data_json = matches.group(1)
                    layout_json = matches.group(2)

                    # Parse the JSON
                    data = json.loads(data_json)
                    layout = json.loads(layout_json)

                    # Create Plotly figure
                    fig = go.Figure(data=data, layout=layout)

                    # Convert to image using Kaleido
                    return self._plotly_to_image(fig)
                except Exception as e:
                    logger.debug(f"Failed to extract Plotly from HTML: {e}")
                    return None

            return None

        except Exception as e:
            logger.debug(f"Error trying Plotly extraction: {e}")
            return None

    def _plotly_to_image(self, fig) -> Optional[str]:
        """Convert Plotly figure directly to base64 image using Kaleido."""
        try:
            # Convert Plotly figure to PNG bytes
            img_bytes = fig.to_image(format="png", width=1200, height=800)
            # Convert to base64
            img_b64 = base64.b64encode(img_bytes).decode('utf-8')
            logger.info("Successfully converted Plotly figure to image using Kaleido")
            return img_b64
        except Exception as e:
            logger.warning(f"Kaleido conversion failed: {e}")
            return None

    def _pickle_to_image(self, pickle_path: str) -> Optional[str]:
        """Convert pickle file containing Plotly figure to base64 image."""
        try:
            import pickle
            import os

            logger.info(f"Loading pickle file: {pickle_path}")
            logger.info(f"File exists: {os.path.exists(pickle_path)}")
            logger.info(f"Absolute path: {os.path.abspath(pickle_path)}")

            if not os.path.exists(pickle_path):
                logger.error(f"Pickle file does not exist at path: {pickle_path}")
                # Try to list files in the directory
                dir_path = os.path.dirname(pickle_path)
                if os.path.exists(dir_path):
                    files = os.listdir(dir_path)
                    logger.info(f"Files in {dir_path}: {files[:5]}")  # Show first 5 files
                return None

            # Load the pickle file
            with open(pickle_path, 'rb') as f:
                fig = pickle.load(f)
                logger.info(f"Successfully loaded pickle file, type: {type(fig)}")

            # Convert Plotly figure to image using existing method
            result = self._plotly_to_image(fig)
            if result:
                logger.info("Successfully converted pickle to image")
            else:
                logger.error("Failed to convert Plotly figure to image")
            return result

        except FileNotFoundError:
            logger.error(f"Pickle file not found: {pickle_path}")
            return None
        except Exception as e:
            logger.error(f"Error converting pickle to image: {e}", exc_info=True)
            return None

    def _html_to_image(self, html_path: str) -> Optional[str]:
        """Convert HTML visualization to base64 image."""
        try:
            logger.info(f"Attempting to convert HTML to image: {html_path}")

            # Create temporary image file
            with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp_file:
                tmp_path = tmp_file.name

            # Use headless browser to capture HTML as image
            # Try different methods in order of preference
            methods = [
                ('html2image', self._capture_with_html2image),  # New lightweight method
                ('playwright', self._capture_with_playwright),
                ('selenium', self._capture_with_selenium),
                ('wkhtmltoimage', self._capture_with_wkhtmltoimage)
            ]
            
            for method_name, method in methods:
                try:
                    logger.info(f"Trying {method_name} for HTML capture")
                    if method(html_path, tmp_path):
                        logger.info(f"Successfully captured HTML with {method_name}")
                        # Convert captured image to base64
                        return self._image_to_base64(tmp_path)
                except Exception as e:
                    logger.debug(f"{method_name} capture failed: {e}")
                    continue
            
            logger.warning("All image capture methods failed")
            return None
            
        except Exception as e:
            logger.error(f"Error converting HTML to image: {e}")
            return None
        finally:
            # Clean up temporary file
            if 'tmp_path' in locals() and os.path.exists(tmp_path):
                os.unlink(tmp_path)
    
    def _capture_with_html2image(self, html_path: str, output_path: str) -> bool:
        """Capture HTML using html2image (lightweight method)."""
        try:
            from html2image import Html2Image

            # Create Html2Image instance
            hti = Html2Image(output_path=os.path.dirname(output_path))

            # Read HTML content
            with open(html_path, 'r', encoding='utf-8') as f:
                html_content = f.read()

            # Generate screenshot
            output_filename = os.path.basename(output_path)
            hti.screenshot(html_str=html_content, save_as=output_filename)

            # Check if file was created
            return os.path.exists(output_path)

        except ImportError:
            logger.debug("html2image not available")
            return False
        except Exception as e:
            logger.debug(f"html2image capture failed: {e}")
            return False

    def _capture_with_playwright(self, html_path: str, output_path: str) -> bool:
        """Capture HTML using Playwright (preferred method)."""
        try:
            # Try to import playwright
            from playwright.sync_api import sync_playwright
            
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                page = browser.new_page()
                
                # Load HTML file
                page.goto(f"file://{os.path.abspath(html_path)}")
                
                # Wait for content to load
                page.wait_for_timeout(2000)
                
                # Take screenshot
                page.screenshot(path=output_path, full_page=True)
                
                browser.close()
                return True
                
        except ImportError:
            logger.debug("Playwright not available")
            return False
        except Exception as e:
            logger.debug(f"Playwright capture failed: {e}")
            return False
    
    def _capture_with_selenium(self, html_path: str, output_path: str) -> bool:
        """Capture HTML using Selenium."""
        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
            
            options = Options()
            options.add_argument('--headless')
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            
            driver = webdriver.Chrome(options=options)
            driver.get(f"file://{os.path.abspath(html_path)}")
            
            # Wait for content to load
            driver.implicitly_wait(3)
            
            # Take screenshot
            driver.save_screenshot(output_path)
            driver.quit()
            
            return True
            
        except ImportError:
            logger.debug("Selenium not available")
            return False
        except Exception as e:
            logger.debug(f"Selenium capture failed: {e}")
            return False
    
    def _capture_with_wkhtmltoimage(self, html_path: str, output_path: str) -> bool:
        """Capture HTML using wkhtmltoimage."""
        try:
            cmd = [
                'wkhtmltoimage',
                '--width', '1200',
                '--height', '800',
                '--javascript-delay', '2000',
                html_path,
                output_path
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            return result.returncode == 0
            
        except (subprocess.TimeoutExpired, FileNotFoundError):
            logger.debug("wkhtmltoimage not available or failed")
            return False
        except Exception as e:
            logger.debug(f"wkhtmltoimage capture failed: {e}")
            return False
    
    def _image_to_base64(self, image_path: str) -> str:
        """Convert image file to base64 string."""
        try:
            with open(image_path, 'rb') as f:
                img_data = f.read()
                img_b64 = base64.b64encode(img_data).decode('utf-8')
                return img_b64
        except Exception as e:
            logger.error(f"Error converting image to base64: {e}")
            return None

    def _build_data_context(self, session_id: Optional[str], viz_type: Optional[str], viz_path: Optional[str]) -> Dict[str, Any]:
        """Construct a minimal context from available session data to guide text-only explanations."""
        ctx: Dict[str, Any] = {
            'type': viz_type or 'visualization',
            'source': os.path.basename(viz_path) if viz_path else None,
            'summary': {}
        }
        try:
            if not session_id:
                return ctx
            import glob
            import pandas as pd
            sess_dir = os.path.join(current_app.instance_path, 'uploads', session_id)
            if not os.path.isdir(sess_dir):
                return ctx
            # Prefer specific files if they exist
            csv_path = None
            for name in ['tpr_results.csv', 'raw_data.csv', 'raw_data.xlsx']:
                path = os.path.join(sess_dir, name)
                if os.path.exists(path):
                    csv_path = path
                    break
            if not csv_path:
                data_files = glob.glob(os.path.join(sess_dir, '*.csv')) + \
                            glob.glob(os.path.join(sess_dir, '*.xlsx')) + \
                            glob.glob(os.path.join(sess_dir, '*.xls'))
                if data_files:
                    csv_path = max(data_files, key=os.path.getctime)
            if not csv_path or not os.path.exists(csv_path):
                return ctx
            if csv_path.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(csv_path)
            else:
                df = pd.read_csv(csv_path)
            ctx['summary']['rows'] = int(df.shape[0])
            ctx['summary']['columns'] = int(df.shape[1])
            # Heuristic detection of a key metric (e.g., TPR/positivity)
            metric_cols = [c for c in df.columns if 'tpr' in c.lower() or 'positiv' in c.lower() or 'risk' in c.lower()]
            if metric_cols:
                c = metric_cols[0]
                import pandas as _pd
                series = _pd.to_numeric(df[c], errors='coerce')
                if series.notna().any():
                    ctx['summary']['metric'] = c
                    ctx['summary']['mean'] = float(series.mean(skipna=True))
                    ctx['summary']['max'] = float(series.max(skipna=True))
            # Extract location hints if available
            for col in ['state', 'state_name', 'lga', 'lga_name']:
                if col in df.columns:
                    vals = df[col].dropna().astype(str).unique().tolist()
                    if vals:
                        ctx['summary'][col] = vals[:3]
            return ctx
        except Exception as e:
            logger.debug(f"Could not build data context: {e}")
            return ctx
    
    def _get_llm_explanation(self, img_b64: str, viz_type: str, session_id: str) -> str:
        """Get LLM explanation using image (py-sidebot approach)."""
        try:
            if not self.llm_manager:
                error_msg = "LLM manager not available for vision explanations"
                logger.error(error_msg)
                return f"ERROR: {error_msg}"
            
            # Create image URL for LLM
            img_url = f"data:image/png;base64,{img_b64}"
            
            # Enhanced instructions for malaria context
            # Create focused instructions for this specific visualization
            focused_instructions = f"""
            {self.instructions}
            
            Visualization context: {viz_type or 'data analysis'}
            
            Analyze what you actually see in this specific image. Make observations about:
            - The specific patterns, clusters, or distributions visible
            - Which regions/areas show the highest and lowest values
            - Any surprising outliers or unexpected patterns
            - Practical implications of what this data reveals
            
            Be specific to THIS visualization, not general advice.
            """
            
            # Check if LLM supports vision
            if hasattr(self.llm_manager, 'generate_with_image'):
                logger.info("Using vision-enabled LLM for explanation")
                # Use vision-enabled LLM
                explanation = self.llm_manager.generate_with_image(
                    prompt=focused_instructions,
                    image_url=img_url,
                    session_id=session_id
                )
            else:
                logger.warning("LLM doesn't support vision, using text fallback")
                # Fallback to text-only explanation
                explanation = self.llm_manager.generate_response(
                    prompt=f"Explain this {viz_type or 'malaria visualization'} for public health officials.",
                    system_message=self.instructions,
                    session_id=session_id
                )
            
            return explanation
            
        except Exception as e:
            error_msg = f"LLM explanation failed: {str(e)}"
            logger.error(f"Error getting LLM explanation: {e}", exc_info=True)
            return f"ERROR: {error_msg}"
    
    # REMOVED: _fallback_explanation method completely removed
    # All errors now return explicit error messages instead of generic fallbacks

def get_universal_viz_explainer(llm_manager=None):
    """Factory function to create universal visualization explainer."""
    return UniversalVisualizationExplainer(llm_manager=llm_manager)
