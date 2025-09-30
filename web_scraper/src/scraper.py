import os
import logging
import pandas as pd
from bs4 import BeautifulSoup
import requests
import json
import re
import time
from datetime import datetime
from typing import Optional, List, Dict, Any, Union
from urllib.parse import urlparse, urljoin
from playwright.sync_api import sync_playwright
import asyncio
from concurrent.futures import ThreadPoolExecutor

# Set up logging
log_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'logs')
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, 'scraper.log')

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class WebScraper:
    """
    A web scraper class that handles HTTP requests and data extraction.
    """
    
    def __init__(self, use_playwright: bool = True, use_scrapingbee: bool = False, scrapingbee_api_key: str = None):
        """
        Initialize the web scraper.
        
        Args:
            use_playwright: Whether to use Playwright for JavaScript rendering
            use_scrapingbee: Whether to use ScrapingBee API as a fallback
            scrapingbee_api_key: API key for ScrapingBee (required if use_scrapingbee is True)
        """
        self.use_playwright = use_playwright
        self.use_scrapingbee = use_scrapingbee
        self.scrapingbee_api_key = scrapingbee_api_key
        self.session = requests.Session()
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9,hi;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Referer': 'https://www.google.com/',
            'DNT': '1',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
        }
        
        # List of user agents to rotate
        self.user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/119.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 OPR/106.0.0.0'
        ]
        
        # Initialize Playwright if needed
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
        
        if self.use_playwright:
            try:
                from playwright.sync_api import sync_playwright
                self.playwright = sync_playwright().start()
                self.browser = self.playwright.chromium.launch(
                    headless=False,  # Set to True in production
                    args=[
                        '--disable-blink-features=AutomationControlled',
                        '--disable-infobars',
                        '--window-size=1920,1080',
                        '--start-maximized'
                    ]
                )
                
                # Create a new browser context with custom options
                self.context = self.browser.new_context(
                    user_agent=self.headers['User-Agent'],
                    viewport={'width': 1920, 'height': 1080},
                    locale='en-US',
                    timezone_id='America/New_York',
                    color_scheme='light',
                    permissions=['geolocation'],
                    geolocation={'longitude': -74.0060, 'latitude': 40.7128},
                    http_credentials=None,
                    ignore_https_errors=False,
                    java_script_enabled=True,
                    bypass_csp=False,
                    offline=False,
                    has_touch=False,
                    is_mobile=False,
                    device_scale_factor=1,
                    no_viewport=False,
                    record_har_path=None,
                    record_video_dir=None,
                    record_video_size=None,
                    screen={'width': 1920, 'height': 1080},
                    storage_state=None,
                    extra_http_headers={
                        'Accept-Language': 'en-US,en;q=0.9',
                        'Accept-Encoding': 'gzip, deflate, br',
                        'DNT': '1',
                        'Upgrade-Insecure-Requests': '1',
                    }
                )
                
                # Add init script to hide WebDriver
                self.context.add_init_script("""
                    Object.defineProperty(navigator, 'webdriver', {
                        get: () => undefined
                    });
                    
                    // Override the languages property to use a custom getter
                    Object.defineProperty(navigator, 'languages', {
                        get: () => ['en-US', 'en', 'hi'],
                    });
                    
                    // Override the plugins property to use a custom getter
                    Object.defineProperty(navigator, 'plugins', {
                        get: () => [1, 2, 3, 4, 5],
                    });
                    
                    // Override the platform to prevent detection
                    Object.defineProperty(navigator, 'platform', {
                        get: () => 'Win32',
                    });
                """)
                
                # Create a new page
                self.page = self.context.new_page()
                
                # Set extra HTTP headers
                self.page.set_extra_http_headers({
                    'Accept-Language': 'en-US,en;q=0.9',
                    'Accept-Encoding': 'gzip, deflate, br',
                    'DNT': '1',
                    'Upgrade-Insecure-Requests': '1',
                })
                
                logger.info("Playwright browser initialized with anti-detection measures")
                
            except Exception as e:
                logger.error(f"Failed to initialize Playwright: {str(e)}")
                self.use_playwright = False
    
    def get_page(self, url: str, max_retries: int = 3) -> Optional[BeautifulSoup]:
        """
        Fetch a web page with retry logic and proper headers.
        Tries methods in order: Playwright -> Requests -> ScrapingBee (if configured)
        
        Args:
            url (str): URL of the page to fetch
            max_retries (int): Maximum number of retry attempts
            
        Returns:
            BeautifulSoup: Parsed HTML content or None if all methods fail
        """
        # Try Playwright first if enabled
        if self.use_playwright and hasattr(self, 'page') and self.page:
            result = self._get_page_with_playwright(url, max_retries)
            if result:
                return result
        
        # Fall back to requests
        result = self._get_page_with_requests(url, max_retries)
        if result:
            return result
        
        # If still no success, try ScrapingBee if configured
        if self.use_scrapingbee and self.scrapingbee_api_key:
            return self._get_page_with_scrapingbee(url, max_retries)
        
        return None
            
    def _get_page_with_playwright(self, url: str, max_retries: int) -> Optional[BeautifulSoup]:
        """Fetch page using Playwright for JavaScript rendering."""
        if not self.use_playwright or not self.page:
            logger.warning("Playwright not available, falling back to requests")
            return self._get_page_with_requests(url, max_retries)
            
        for attempt in range(max_retries):
            try:
                logger.info(f"Fetching with Playwright: {url} (Attempt {attempt + 1}/{max_retries})")
                
                # Navigate to the page with a longer timeout
                self.page.goto(url, timeout=120000, wait_until='domcontentloaded')
                
                # Wait for content to load
                self.page.wait_for_load_state('networkidle')
                
                # Wait for main content to be visible
                self.page.wait_for_selector('main, article, .content, .article, [role="main"]', timeout=30000)
                
                # Scroll down the page to trigger lazy loading
                self.page.evaluate("""
                    window.scrollBy(0, window.innerHeight);
                    return new Promise(resolve => setTimeout(resolve, 1000));
                """)
                
                # Wait a bit more for dynamic content
                time.sleep(2)
                
                # Get the page content
                content = self.page.content()
                
                # Save the page source for debugging
                debug_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'debug')
                os.makedirs(debug_dir, exist_ok=True)
                html_path = os.path.join(debug_dir, 'page_source_playwright.html')
                with open(html_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                logger.info(f"Saved Playwright page source to {html_path}")
                
                if len(content) < 1000:
                    logger.warning(f"Received very little content ({len(content)} bytes)")
                    continue
                
                if any(term in content.lower() for term in ['captcha', 'access denied', 'cloudflare']):
                    logger.warning("Detected potential CAPTCHA or access restriction")
                    continue
                
                soup = BeautifulSoup(content, 'html.parser')
                
                # Additional check for CAPTCHA in the parsed content
                if soup.find(string=re.compile(r'(?i)captcha|access denied|cloudflare')):
                    logger.warning("Detected CAPTCHA in the page content")
                    continue
                
                return soup
                
            except Exception as e:
                logger.warning(f"Playwright attempt {attempt + 1} failed: {str(e)}")
                if attempt == max_retries - 1:
                    logger.error(f"Failed to fetch {url} with Playwright after {max_retries} attempts")
                    # Fall back to requests if Playwright fails
                    return self._get_page_with_requests(url, max_retries)
                
                # Add a delay before retry
                time.sleep(2 ** attempt)  # Exponential backoff
    
    def _get_page_with_requests(self, url: str, max_retries: int) -> Optional[BeautifulSoup]:
        """Fetch page using requests library with retry logic."""
        for attempt in range(max_retries):
            try:
                # Rotate user agent
                current_ua = self.user_agents[attempt % len(self.user_agents)]
                headers = self.headers.copy()
                headers['User-Agent'] = current_ua
                
                # Add a delay between retries
                if attempt > 0:
                    delay = 2 ** attempt  # Exponential backoff
                    logger.info(f"Waiting {delay} seconds before retry...")
                    time.sleep(delay)
                
                logger.info(f"Fetching with requests: {url} (Attempt {attempt + 1}/{max_retries}) with User-Agent: {current_ua[:50]}...")
                
                # Make the request
                response = self.session.get(
                    url,
                    headers=headers,
                    timeout=30,
                    allow_redirects=True,
                    verify=True
                )
                
                # Check response status
                response.raise_for_status()
                
                # Check content length
                if len(response.text) < 1000:
                    logger.warning(f"Received very little content ({len(response.text)} bytes)")
                    continue
                
                # Check for CAPTCHA or access denied
                if any(term in response.text.lower() for term in ['captcha', 'access denied', 'cloudflare']):
                    logger.warning("Detected potential CAPTCHA or access restriction")
                    continue
                
                # Parse with BeautifulSoup
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Additional check for CAPTCHA in the parsed content
                if soup.find(string=re.compile(r'(?i)captcha|access denied|cloudflare')):
                    logger.warning("Detected CAPTCHA in the page content")
                    continue
                
                # Save the page source for debugging
                debug_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'debug')
                os.makedirs(debug_dir, exist_ok=True)
                html_path = os.path.join(debug_dir, 'page_source_requests.html')
                with open(html_path, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                logger.info(f"Saved requests page source to {html_path}")
                
                return soup
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request attempt {attempt + 1} failed: {str(e)}")
                if attempt == max_retries - 1:
                    logger.error(f"Failed to fetch {url} after {max_retries} attempts")
                    return None
    
    def _get_page_with_requests(self, url: str, max_retries: int) -> Optional[BeautifulSoup]:
        """Fetch page using requests library with retry logic."""
        for attempt in range(max_retries):
            try:
                # Rotate user agent
                current_ua = self.user_agents[attempt % len(self.user_agents)]
                headers = self.headers.copy()
                headers['User-Agent'] = current_ua
                
                # Add a delay between retries
                if attempt > 0:
                    delay = 2 ** attempt  # Exponential backoff
                    logger.info(f"Waiting {delay} seconds before retry...")
                    time.sleep(delay)
                
                logger.info(f"Fetching with requests: {url} (Attempt {attempt + 1}/{max_retries}) with User-Agent: {current_ua[:50]}...")
                
                # Make the request
                response = self.session.get(
                    url,
                    headers=headers,
                    timeout=30,
                    allow_redirects=True,
                    verify=True
                )
                
                # Check response status
                response.raise_for_status()
                
                # Check content length
                if len(response.text) < 1000:
                    logger.warning(f"Received very little content ({len(response.text)} bytes)")
                    continue
                
                # Check for CAPTCHA or access denied
                if any(term in response.text.lower() for term in ['captcha', 'access denied', 'cloudflare']):
                    logger.warning("Detected potential CAPTCHA or access restriction")
                    continue
                
                # Parse with BeautifulSoup
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Additional check for CAPTCHA in the parsed content
                if soup.find(string=re.compile(r'(?i)captcha|access denied|cloudflare')):
                    logger.warning("Detected CAPTCHA in the page content")
                    continue
                
                # Save the page source for debugging
                debug_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'debug')
                os.makedirs(debug_dir, exist_ok=True)
                html_path = os.path.join(debug_dir, 'page_source_requests.html')
                with open(html_path, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                logger.info(f"Saved requests page source to {html_path}")
                
                return soup
                
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request attempt {attempt + 1} failed: {str(e)}")
                if attempt == max_retries - 1:
                    logger.error(f"Failed to fetch {url} after {max_retries} attempts")
                    return None
    
    def _get_page_with_scrapingbee(self, url: str, max_retries: int) -> Optional[BeautifulSoup]:
        """Fetch page using ScrapingBee API."""
        if not self.use_scrapingbee or not self.scrapingbee_api_key:
            return None
            
        for attempt in range(max_retries):
            try:
                logger.info(f"Fetching with ScrapingBee: {url} (Attempt {attempt + 1}/{max_retries})")
                
                # Add delay between retries
                if attempt > 0:
                    delay = 2 ** attempt
                    logger.info(f"Waiting {delay} seconds before retry...")
                    time.sleep(delay)
                
                # Make the request to ScrapingBee API
                params = {
                    'api_key': self.scrapingbee_api_key,
                    'url': url,
                    'render_js': 'true',
                    'premium_proxy': 'true',
                    'country_code': 'us',
                    'wait': '3000',  # Wait for 3 seconds for JS to render
                    'timeout': '30000'  # 30 seconds timeout
                }
                
                response = self.session.get(
                    'https://app.scrapingbee.com/api/v1/',
                    params=params,
                    timeout=45  # Slightly longer timeout for API calls
                )
                
                # Check response status
                response.raise_for_status()
                
                # Check content length
                if len(response.text) < 1000:
                    logger.warning(f"Received very little content from ScrapingBee ({len(response.text)} bytes)")
                    continue
                
                # Check for CAPTCHA or access denied
                if any(term in response.text.lower() for term in ['captcha', 'access denied', 'cloudflare']):
                    logger.warning("Detected potential CAPTCHA or access restriction in ScrapingBee response")
                    continue
                
                # Save the page source for debugging
                debug_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'debug')
                os.makedirs(debug_dir, exist_ok=True)
                html_path = os.path.join(debug_dir, 'page_source_scrapingbee.html')
                with open(html_path, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                logger.info(f"Saved ScrapingBee page source to {html_path}")
                
                # Parse with BeautifulSoup
                soup = BeautifulSoup(response.text, 'html.parser')
                return soup
                
            except Exception as e:
                logger.warning(f"ScrapingBee attempt {attempt + 1} failed: {str(e)}")
                if attempt == max_retries - 1:
                    logger.error(f"Failed to fetch {url} with ScrapingBee after {max_retries} attempts")
        
        return None
    
    def _get_page_with_playwright(self, url: str, max_retries: int) -> Optional[BeautifulSoup]:
        """Fetch page using Playwright for JavaScript rendering."""
        if not self.use_playwright or not hasattr(self, 'page') or not self.page:
            logger.warning("Playwright not available or not initialized, falling back to requests")
            return None
            
        for attempt in range(max_retries):
            try:
                logger.info(f"Fetching with Playwright: {url} (Attempt {attempt + 1}/{max_retries})")
                
                # Navigate to the page with a longer timeout
                self.page.goto(url, timeout=120000, wait_until='domcontentloaded')
                
                # Wait for content to load
                self.page.wait_for_load_state('networkidle')
                
                # Wait for main content to be visible
                try:
                    self.page.wait_for_selector('main, article, .content, .article, [role="main"]', timeout=30000)
                except Exception as e:
                    logger.warning(f"Timeout waiting for main content: {str(e)}")
                
                # Scroll down the page to trigger lazy loading
                self.page.evaluate("""
                    window.scrollBy(0, window.innerHeight);
                    return new Promise(resolve => setTimeout(resolve, 1000));
                """)
                
                # Wait a bit more for dynamic content
                time.sleep(2)
                
                # Get the page content
                content = self.page.content()
                
                # Save the page source for debugging
                debug_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'debug')
                os.makedirs(debug_dir, exist_ok=True)
                html_path = os.path.join(debug_dir, 'page_source_playwright.html')
                with open(html_path, 'w', encoding='utf-8') as f:
                    f.write(content)
                logger.info(f"Saved Playwright page source to {html_path}")
                
                if len(content) < 1000:
                    logger.warning(f"Received very little content ({len(content)} bytes)")
                    continue
                
                if any(term in content.lower() for term in ['captcha', 'access denied', 'cloudflare']):
                    logger.warning("Detected potential CAPTCHA or access restriction")
                    continue
                
                soup = BeautifulSoup(content, 'html.parser')
                
                # Additional check for CAPTCHA in the parsed content
                if soup.find(string=re.compile(r'(?i)captcha|access denied|cloudflare')):
                    logger.warning("Detected CAPTCHA in the page content")
                    continue
                
                return soup
                
            except Exception as e:
                logger.warning(f"Playwright attempt {attempt + 1} failed: {str(e)}")
                if attempt == max_retries - 1:
                    logger.error(f"Failed to fetch {url} with Playwright after {max_retries} attempts")
                    # Fall back to requests if Playwright fails
                    return self._get_page_with_requests(url, max_retries)
                
                # Add a delay before retry
                time.sleep(2 ** attempt)  # Exponential backoff
    
    def clean_text(self, text: str) -> str:
        """Clean and normalize text by removing extra whitespace and newlines."""
        if not text:
            return ''
        # Replace multiple spaces/newlines with a single space
        import re
        return ' '.join(re.sub(r'\s+', ' ', text, flags=re.UNICODE).strip().split())

    def extract_article_content(self, soup) -> Dict[str, Any]:
        """Extract main article content from the page with specific handling for stotram pages."""
        content = {
            'verses': [],
            'meanings': [],
            'paragraphs': [],
            'sections': {}
        }
        
        # First, try to find the main content area
        main_content = None
        
        # Try common content selectors
        selectors = [
            'article', 'main', 'div.entry-content', 'div.article-content', 
            'div.post-content', 'div.content', 'div.main-content', 'div#content'
        ]
        
        for selector in selectors:
            main_content = soup.select_one(selector)
            if main_content:
                break
        
        # If no specific content area found, use the body
        if not main_content:
            main_content = soup.find('body')
        
        if main_content:
            # Extract all text elements
            elements = main_content.find_all(['p', 'div', 'span', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6'])
            
            current_section = 'General'
            
            for elem in elements:
                if not elem.text.strip():
                    continue
                    
                text = self.clean_text(elem.get_text())
                
                # Section headers
                if elem.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                    current_section = text
                    if current_section not in content['sections']:
                        content['sections'][current_section] = []
                    continue
                
                # Check for verses (look for devanagari characters or specific patterns)
                devanagari_chars = ['॥', '।', 'ॐ', 'नमः', 'शिव', 'हर', 'राम', 'कृष्ण']
                if any(char in text for char in devanagari_chars) or len(text) > 50:
                    if text not in content['verses']:
                        content['verses'].append(text)
                        if current_section in content['sections']:
                            content['sections'][current_section].append(text)
                # Check for meanings/explanations
                elif any(word in text.lower() for word in ['meaning', 'अर्थ', 'भावार्थ', 'explanation', 'translation']):
                    if text not in content['meanings']:
                        content['meanings'].append(text)
                        if current_section in content['sections']:
                            content['sections'][current_section].append(text)
                # Regular paragraphs
                elif len(text) > 30:
                    if text not in content['paragraphs']:
                        content['paragraphs'].append(text)
                        if current_section in content['sections']:
                            content['sections'][current_section].append(text)
        
        # Format the full text with sections
        full_text_parts = []
        
        if content['verses']:
            full_text_parts.append('VERSES:')
            full_text_parts.extend(content['verses'])
            
        if content['meanings']:
            full_text_parts.append('\nMEANINGS:')
            full_text_parts.extend(content['meanings'])
            
        if content['sections']:
            full_text_parts.append('\nSECTIONS:')
            for section, items in content['sections'].items():
                full_text_parts.append(f'\n--- {section} ---')
                full_text_parts.extend(items)
        
        content['full_text'] = '\n\n'.join(full_text_parts)
        
        return content

    def extract_article_content(self, soup) -> Dict[str, Any]:
        """Extract main article content from the page."""
        content = {
            'verses': [],
            'meanings': [],
            'paragraphs': [],
            'sections': {}
        }
        
        # Special handling for the Shiva Tandav Stotram page
        if 'shiv-tandav-stotram' in str(soup):
            return self._extract_shiva_tandav_content(soup)
        
        # First, try to find the main content area
        main_content = None
        
        # Common content selectors - try multiple patterns
        content_selectors = [
            'article', 
            'main', 
            'div.entry-content', 
            'div.article-content',
            'div.post-content', 
            'div.content', 
            'div.main-content', 
            'div#content',
            'div.post',
            'div.entry',
            'div.page-content'
        ]
        
        # Try each selector until we find the main content
        for selector in content_selectors:
            main_content = soup.select_one(selector)
            if main_content:
                logger.info(f"Found content with selector: {selector}")
                break
        
        # If no specific content area found, use the body
        if not main_content:
            main_content = soup.find('body')
            logger.info("Using body as content container")
        
        if main_content:
            # Extract all text elements
            elements = main_content.find_all(['p', 'div', 'span', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'pre', 'blockquote'])
            
            current_section = 'General'
            
            for elem in elements:
                if not elem.text.strip():
                    continue
                    
                text = self.clean_text(elem.get_text())
                
                # Skip very short texts
                if len(text) < 10:
                    continue
                
                # Section headers
                if elem.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                    current_section = text
                    if current_section not in content['sections']:
                        content['sections'][current_section] = []
                    continue
                
                # Check for verses (look for devanagari characters or specific patterns)
                devanagari_chars = ['॥', '।', 'ॐ', 'नमः', 'शिव', 'हर', 'राम', 'कृष्ण']
                if any(char in text for char in devanagari_chars) or len(text) > 50:
                    if text not in content['verses']:
                        content['verses'].append(text)
                        if current_section in content['sections']:
                            content['sections'][current_section].append(f"VERSE: {text}")
                # Check for meanings/explanations
                elif any(word in text.lower() for word in ['meaning', 'अर्थ', 'भावार्थ', 'explanation', 'translation']):
                    if text not in content['meanings']:
                        content['meanings'].append(text)
                        if current_section in content['sections']:
                            content['sections'][current_section].append(f"MEANING: {text}")
                # Regular paragraphs
                elif len(text) > 30:
                    if text not in content['paragraphs']:
                        content['paragraphs'].append(text)
                        if current_section in content['sections']:
                            content['sections'][current_section].append(text)
        
        # Format the full text with sections
        full_text_parts = []
        
        if content['verses']:
            full_text_parts.append('VERSES:')
            full_text_parts.extend(content['verses'])
            
        if content['meanings']:
            full_text_parts.append('\nMEANINGS:')
            full_text_parts.extend(content['meanings'])
            
        if content['sections']:
            full_text_parts.append('\nSECTIONS:')
            for section, items in content['sections'].items():
                full_text_parts.append(f'\n--- {section} ---')
                full_text_parts.extend(items)
        
        content['full_text'] = '\n\n'.join(full_text_parts)
        
        return content
    
    def scrape_example(self, url: str) -> List[Dict[str, Any]]:
        """
        Scrape data from the provided URL with enhanced content extraction.
        
        Args:
            url (str): The URL to scrape
            
        Returns:
            list: List of dictionaries containing scraped data
        """
        logger.info(f"Starting web scraping for URL: {url}")
        
        try:
            # Get the page content
            soup = self.get_page(url)
            
            if not soup:
                logger.error(f"Failed to fetch content from {url}")
                return []
            
            # Save the raw HTML for debugging
            debug_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'debug')
            os.makedirs(debug_dir, exist_ok=True)
            
            # Save the HTML content
            html_path = os.path.join(debug_dir, 'page_source.html')
            with open(html_path, 'w', encoding='utf-8') as f:
                f.write(str(soup))
            logger.info(f"Saved page source to {html_path}")
            
            # Basic page information
            title = ''
            if soup.title and soup.title.string:
                title = self.clean_text(soup.title.string)
            
            page_data = {
                'url': url,
                'title': title or 'No title found',
                'scraped_at': datetime.now().isoformat()
            }
            
            # Extract main content
            content = self.extract_article_content(soup)
            page_data.update(content)
            
            # Add metadata
            meta_desc = soup.find('meta', attrs={'name': 'description'})
            if not meta_desc:
                meta_desc = soup.find('meta', attrs={'property': 'og:description'})
                
            if meta_desc and 'content' in meta_desc.attrs:
                page_data['meta_description'] = self.clean_text(meta_desc['content'])
            
            # Count elements
            page_data.update({
                'num_links': len(soup.find_all('a')),
                'num_images': len(soup.find_all('img')),
                'num_paragraphs': len(content.get('paragraphs', [])),
                'num_verses': len(content.get('verses', [])),
                'num_sections': len(content.get('sections', {})),
                'content_preview': content.get('full_text', '')[:1000] + ('...' if len(content.get('full_text', '')) > 1000 else '')
            })
            
            # Log extraction results
            logger.info(f"Extracted {page_data['num_verses']} verses, {page_data['num_paragraphs']} paragraphs, and {page_data['num_sections']} sections")
            
            return [page_data]
            
        except Exception as e:
            logger.error(f"Error scraping {url}: {str(e)}")
            return []
    
    def _extract_shiva_tandav_content(self, soup) -> Dict[str, Any]:
        """Specialized extraction for Shiva Tandav Stotram page."""
        content = {
            'verses': [],
            'meanings': [],
            'paragraphs': [],
            'sections': {}
        }
        
        try:
            # Try to find the main content container
            article = soup.find('article')
            if not article:
                article = soup.find('main') or soup.find('div', class_=lambda x: x and 'content' in x.lower()) or soup
            
            # Extract all text blocks
            text_blocks = []
            current_section = 'Shiva Tandav Stotram'
            
            # Find all paragraph elements and headers
            elements = article.find_all(['p', 'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'div'])
            
            for elem in elements:
                # Skip empty elements
                if not elem.text.strip():
                    continue
                
                # Clean the text
                text = self.clean_text(elem.text)
                
                # Skip very short texts
                if len(text) < 10:
                    continue
                
                # Handle section headers
                if elem.name in ['h1', 'h2', 'h3', 'h4', 'h5', 'h6']:
                    current_section = text
                    if current_section not in content['sections']:
                        content['sections'][current_section] = []
                    continue
                
                # Check for verses (look for devanagari characters or specific patterns)
                devanagari_chars = ['॥', '।', 'ॐ', 'नमः', 'शिव', 'हर', 'राम', 'कृष्ण', 'तांडव', 'ताण्डव']
                if any(char in text for char in devanagari_chars) or len(text) > 50:
                    if text not in content['verses']:
                        content['verses'].append(text)
                        if current_section in content['sections']:
                            content['sections'][current_section].append(f"VERSE: {text}")
                # Check for meanings/explanations
                elif any(word in text.lower() for word in ['meaning', 'अर्थ', 'भावार्थ', 'explanation', 'translation']):
                    if text not in content['meanings']:
                        content['meanings'].append(text)
                        if current_section in content['sections']:
                            content['sections'][current_section].append(f"MEANING: {text}")
                # Regular paragraphs
                elif len(text) > 30:
                    if text not in content['paragraphs']:
                        content['paragraphs'].append(text)
                        if current_section in content['sections']:
                            content['sections'][current_section].append(text)
            
            # If we didn't find any verses, try a more aggressive approach
            if not content['verses']:
                logger.warning("No verses found, trying aggressive extraction")
                # Look for any text that might be a verse
                for elem in article.find_all(True):  # All tags
                    if not elem.text.strip():
                        continue
                    
                    text = self.clean_text(elem.text)
                    if len(text) > 50 and any(char in text for char in ['॥', '।', 'ॐ']):
                        if text not in content['verses']:
                            content['verses'].append(text)
                            if current_section in content['sections']:
                                content['sections'][current_section].append(f"VERSE: {text}")
            
            # Format the full text with sections
            full_text_parts = []
            
            if content['verses']:
                full_text_parts.append('VERSES:')
                full_text_parts.extend(content['verses'])
                
            if content['meanings']:
                full_text_parts.append('\nMEANINGS:')
                full_text_parts.extend(content['meanings'])
                
            if content['sections']:
                full_text_parts.append('\nSECTIONS:')
                for section, items in content['sections'].items():
                    full_text_parts.append(f'\n--- {section} ---')
                    full_text_parts.extend(items)
            
            content['full_text'] = '\n\n'.join(full_text_parts)
            
        except Exception as e:
            logger.error(f"Error extracting Shiva Tandav content: {str(e)}")
        
        return content
    
    def __del__(self):
        """Cleanup resources."""
        try:
            if hasattr(self, 'page') and self.page:
                self.page.close()
            if hasattr(self, 'context') and self.context:
                self.context.close()
            if hasattr(self, 'browser') and self.browser:
                self.browser.close()
            if hasattr(self, 'playwright') and self.playwright:
                self.playwright.stop()
        except Exception as e:
            logger.warning(f"Error during cleanup: {str(e)}")
    
    def save_to_csv(self, data: List[Dict], filename: str) -> str:
        """
        Save scraped data to a CSV file with UTF-8 encoding.
        
        Args:
            data (list): List of dictionaries containing data to save
            filename (str): Output filename (without extension)
            
        Returns:
            str: Path to the saved file or empty string if failed
        """
        try:
            # Create data directory if it doesn't exist
            os.makedirs("../data", exist_ok=True)
            
            # Create filename with timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filepath = f"../data/{filename}_{timestamp}.csv"
            
            # Convert to DataFrame and save as CSV with UTF-8 encoding
            df = pd.DataFrame(data)
            
            # Handle any non-ASCII characters in the data
            def safe_str(obj):
                if isinstance(obj, (list, dict)):
                    return str(obj)
                return str(obj) if obj is not None else ''
                
            # Convert all data to strings safely
            df = df.applymap(safe_str)
            
            # Save with UTF-8 encoding with BOM for better Excel compatibility
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            logger.info(f"Data saved to {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Error saving to CSV: {e}")
            return ""
    
def parse_arguments():
    """Parse command line arguments."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Web Scraper - Extract data from websites')
    parser.add_argument('url', nargs='?', default='https://httpbin.org/get',
                      help='URL of the website to scrape (default: https://httpbin.org/get)')
    return parser.parse_args()

def main():
    # Parse command line arguments
    args = parse_arguments()
    target_url = args.url
    
    # Ensure URL has http:// or https:// prefix
    if not target_url.startswith(('http://', 'https://')):
        target_url = 'https://' + target_url
        
    print("\n" + "="*50)
    print(f"Web Scraper - Target: {target_url}")
    print("="*50)
    
    # Initialize the scraper
    scraper = WebScraper()
    
    # Scrape the provided URL
    logger.info(f"Starting web scraping for URL: {target_url}")
    results = scraper.scrape_example(target_url)
    
    if results:
        # Generate a filename based on the domain
        from urllib.parse import urlparse
        domain = urlparse(target_url).netloc.replace('.', '_')
        filename = f"scrape_results_{domain}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Save results to CSV
        saved_file = scraper.save_to_csv(results, filename)
        
        if saved_file:
            print("\n" + "="*50)
            print("Scraping Results")
            print("="*50)
            print(f"\nSuccessfully scraped data from: {target_url}")
            print(f"Results saved to: {os.path.abspath(saved_file)}")
            
            # Display a preview of the data with proper encoding
            print("\nPreview of scraped data:")
            print("-" * 80)
            for i, (key, value) in enumerate(results[0].items(), 1):
                if key != 'scraped_at':
                    try:
                        # Try to encode the value for console output
                        if isinstance(value, str):
                            value_preview = value[:100] + ('...' if len(value) > 100 else '')
                            print(f"{i}. {key}: {value_preview.encode('utf-8', 'replace').decode('utf-8')}")
                        else:
                            value_str = str(value)[:100] + ('...' if len(str(value)) > 100 else '')
                            print(f"{i}. {key}: {value_str.encode('utf-8', 'replace').decode('utf-8')}")
                    except Exception as e:
                        print(f"{i}. {key}: [Content contains characters that cannot be displayed]")
            # Print a note about the CSV file
            print("\nNote: Some characters may not display correctly in the console.")
            print("The complete data has been saved to the CSV file with proper encoding.")
            print("-" * 80)
    else:
        print("\nFailed to scrape the provided URL. Please check the URL and try again.")
        print("Make sure the website is accessible and doesn't block web scrapers.")
    
    logger.info("Web scraping completed")

if __name__ == "__main__":
    main()
