import os
import logging
import pandas as pd
from bs4 import BeautifulSoup
import requests
from fake_useragent import UserAgent
from datetime import datetime
from typing import Optional, List, Dict, Any
from requests_html import HTMLSession
import time

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
    
    def __init__(self):
        """Initialize the web scraper with a session."""
        self.session = HTMLSession()
        self.ua = UserAgent()
        self.session.headers.update({
            'User-Agent': self.ua.random,
            'Accept-Language': 'en-US,en;q=0.9,hi;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Connection': 'keep-alive',
            'DNT': '1',
            'Upgrade-Insecure-Requests': '1',
        })
    
    def get_page(self, url: str, use_js: bool = True) -> Optional[BeautifulSoup]:
        """
        Fetch a web page using requests-html with JavaScript rendering.
        
        Args:
            url (str): URL of the page to fetch
            use_js (bool): Whether to use JavaScript rendering
            
        Returns:
            BeautifulSoup: Parsed HTML content or None if failed
        """
        try:
            # First try without JavaScript
            logger.info(f"Fetching {url} {'with' if use_js else 'without'} JavaScript rendering")
            
            # Use requests-html to render the page
            response = self.session.get(url, timeout=30)
            
            # If JavaScript rendering is requested, render the page
            if use_js:
                try:
                    # Render the page (this will execute JavaScript)
                    response.html.render(timeout=20, sleep=5, wait=2)
                except Exception as e:
                    logger.warning(f"JavaScript rendering failed: {str(e)}")
                    # Continue with the non-rendered page
            
            # Parse with BeautifulSoup
            soup = BeautifulSoup(response.html.html, 'html.parser')
            
            # Check if we got meaningful content
            if len(response.text) < 1000:
                logger.warning("Received very little content, trying without JavaScript...")
                if use_js:  # If we already tried with JS, try without
                    return self.get_page(url, use_js=False)
                return None
                
            return soup
            
        except Exception as e:
            logger.error(f"Failed to fetch {url}: {str(e)}")
            if use_js:  # If we were using JS, try without it
                return self.get_page(url, use_js=False)
            return None
    
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

    def scrape_example(self, url: str) -> List[Dict[str, Any]]:
        """
        Scrape data from the provided URL with enhanced content extraction.
        
        Args:
            url (str): The URL to scrape
            
        Returns:
            list: List of dictionaries containing scraped data
        """
        logger.info(f"Scraping URL: {url}")
        
        try:
            # First try with JavaScript rendering
            soup = self.get_page(url, use_js=True)
            
            if not soup:
                logger.error(f"Failed to fetch content from {url}")
                return []
            
            # Save the raw HTML for debugging
            debug_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'debug')
            os.makedirs(debug_dir, exist_ok=True)
            with open(os.path.join(debug_dir, 'page_source.html'), 'w', encoding='utf-8') as f:
                f.write(str(soup))
            
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
            
            # For debugging
            logger.info(f"Extracted {page_data['num_verses']} verses and {page_data['num_paragraphs']} paragraphs")
            
            return [page_data]
            
        except Exception as e:
            logger.error(f"Error scraping {url}: {str(e)}")
            return []
    
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
