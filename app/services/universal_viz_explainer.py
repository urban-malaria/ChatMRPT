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
        try:
            # Convert visualization to base64 image
            img_b64 = self._convert_to_image(viz_path)
            
            if not img_b64:
                return self._fallback_explanation(viz_type)
            
            # Get LLM explanation using image
            explanation = self._get_llm_explanation(img_b64, viz_type, session_id)
            
            return explanation
            
        except Exception as e:
            logger.error(f"Error explaining visualization {viz_path}: {e}")
            return self._fallback_explanation(viz_type)
    
    def _convert_to_image(self, viz_path: str) -> Optional[str]:
        """Convert visualization to base64 image (py-sidebot approach)."""
        try:
            # Check if file exists
            if not os.path.exists(viz_path):
                logger.warning(f"Visualization file not found: {viz_path}")
                return None
            
            # Handle different file types
            if viz_path.endswith('.html'):
                return self._html_to_image(viz_path)
            elif viz_path.endswith(('.png', '.jpg', '.jpeg')):
                return self._image_to_base64(viz_path)
            else:
                logger.warning(f"Unsupported file type: {viz_path}")
                return None
                
        except Exception as e:
            logger.error(f"Error converting visualization to image: {e}")
            return None
    
    def _html_to_image(self, html_path: str) -> Optional[str]:
        """Convert HTML visualization to base64 image."""
        try:
            # Create temporary image file
            with tempfile.NamedTemporaryFile(suffix='.png', delete=False) as tmp_file:
                tmp_path = tmp_file.name
            
            # Use headless browser to capture HTML as image
            # Try different methods in order of preference
            methods = [
                self._capture_with_playwright,
                self._capture_with_selenium,
                self._capture_with_wkhtmltoimage
            ]
            
            for method in methods:
                try:
                    if method(html_path, tmp_path):
                        # Convert captured image to base64
                        return self._image_to_base64(tmp_path)
                except Exception as e:
                    logger.debug(f"Capture method failed: {e}")
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
    
    def _get_llm_explanation(self, img_b64: str, viz_type: str, session_id: str) -> str:
        """Get LLM explanation using image (py-sidebot approach)."""
        try:
            if not self.llm_manager:
                return self._fallback_explanation(viz_type)
            
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
                # Use vision-enabled LLM
                explanation = self.llm_manager.generate_with_image(
                    prompt=focused_instructions,
                    image_url=img_url,
                    session_id=session_id
                )
            else:
                # Fallback to text-only explanation
                explanation = self.llm_manager.generate_response(
                    prompt=f"Explain this {viz_type or 'malaria visualization'} for public health officials.",
                    system_message=malaria_instructions,
                    session_id=session_id
                )
            
            return explanation
            
        except Exception as e:
            logger.error(f"Error getting LLM explanation: {e}")
            return self._fallback_explanation(viz_type)
    
    def _fallback_explanation(self, viz_type: str) -> str:
        """Fallback explanation when image processing fails."""
        viz_name = viz_type.replace('_', ' ').title() if viz_type else 'Visualization'
        
        return f"""📊 **{viz_name} Generated**

This visualization shows malaria risk analysis results to guide public health decision-making.

**How to interpret:**
- Look for spatial patterns and risk hotspots
- Identify areas requiring immediate intervention
- Use color coding to understand risk levels
- Consider epidemiological context for your region

**Next steps:**
- Examine high-risk areas for targeted interventions
- Plan resource allocation based on risk patterns
- Consider local transmission factors
- Implement appropriate control measures"""

def get_universal_viz_explainer(llm_manager=None):
    """Factory function to create universal visualization explainer."""
    return UniversalVisualizationExplainer(llm_manager=llm_manager)