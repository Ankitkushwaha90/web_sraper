# Web Scraper Project

A robust and extensible web scraping project built with Python, designed to extract data from websites efficiently and responsibly.

## Features

- **Flexible Scraping**: Supports both static and dynamic content
- **User-Agent Rotation**: Uses `fake-useragent` to avoid detection
- **Error Handling**: Comprehensive error handling and logging
- **Data Export**: Save scraped data to CSV format
- **Modular Design**: Easy to extend with new scrapers

## Prerequisites

- Python 3.8+
- Conda (recommended) or pip

## Installation

1. Clone the repository:
   ```bash
   git clone <repository-url>
   cd web_scraper
   ```

2. Create and activate the conda environment (if not already created):
   ```bash
   conda create -n safetensor_new python=3.8
   conda activate safetensor_new
   ```

3. Install the required packages:
   ```bash
   pip install -r requirements.txt
   ```

## Project Structure

```
web_scraper/
├── data/               # Directory for storing scraped data
├── logs/               # Log files
├── src/                # Source code
│   └── scraper.py      # Main scraper implementation
├── .gitignore          # Git ignore file
└── requirements.txt    # Python dependencies
```

## Usage

1. Import the `WebScraper` class in your Python script:
   ```python
   from src.scraper import WebScraper
   
   # Initialize the scraper
   scraper = WebScraper()
   
   # Example: Search for something
   results = scraper.scrape_example("web scraping with Python")
   
   # Save results to CSV
   if results:
       scraper.save_to_csv(results, "search_results")
   ```

2. Run the example script:
   ```bash
   python -m src.scraper
   ```

## Best Practices

1. **Respect robots.txt**: Always check the website's `robots.txt` file before scraping.
2. **Rate Limiting**: Add delays between requests to avoid overloading servers.
3. **Error Handling**: The scraper includes basic error handling, but you may need to add more specific error handling for your use case.
4. **Legal Considerations**: Ensure you have the right to scrape the target website and comply with their terms of service.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## Support

For support, please open an issue in the GitHub repository.
