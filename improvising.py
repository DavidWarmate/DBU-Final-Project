import os
import csv
import logging
import time
import threading
from datetime import datetime
import pandas as pd
import numpy as np
import schedule
import matplotlib.pyplot as plt
import seaborn as sns

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
from webdriver_manager.chrome import ChromeDriverManager
from fake_useragent import UserAgent

class EcommerceProductTracker:
    def __init__(self):
        # Configuration
        self.BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        self.LOG_DIR = os.path.join(self.BASE_DIR, 'logs')
        self.PROCESSED_DATA_DIR = os.path.join(self.BASE_DIR, 'processed_data')
        self.ANALYSIS_OUTPUT_DIR = os.path.join(self.BASE_DIR, 'analysis_output')

        # Create directories
        for dir_path in [self.LOG_DIR, self.PROCESSED_DATA_DIR, self.ANALYSIS_OUTPUT_DIR]:
            os.makedirs(dir_path, exist_ok=True)

        # Logging setup
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s: %(message)s',
            handlers=[
                logging.FileHandler(os.path.join(self.LOG_DIR, 'scraper.log')),
                logging.StreamHandler()
            ]
        )

        # Scraping configuration
        self.WEBSITES = ["Amazon", "BestBuy"]
        self.PRODUCT_CATEGORIES = ["laptops", "smartphones", "headphones"]
        self.SCRAPE_INTERVAL = 24  # hours
        
        # User Agent setup
        self.ua = UserAgent()

    def init_driver(self):
        """Initialize Selenium WebDriver with advanced anti-detection"""
        options = webdriver.ChromeOptions()
        
        # Use random user agent
        user_agent = self.ua.random
        options.add_argument(f'user-agent={user_agent}')
        
        # Anti-detection options
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_argument("--disable-extensions")
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        # Headless mode (optional, comment out if you want to see browser)
        #options.add_argument("--headless")

        try:
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
            
            # Advanced anti-detection scripts
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            driver.execute_script("delete navigator.webdriver")
            
            return driver
        except Exception as e:
            logging.error(f"Driver initialization failed: {e}")
            return None

    def scrape_amazon(self, driver, category):
        """Enhanced Amazon scraping method"""
        try:
            # Navigate to Amazon
            driver.get("https://www.amazon.com")
            
            # Wait and search
            search_box = WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.ID, "twotabsearchtextbox"))
            )
            search_box.clear()
            search_box.send_keys(category)
            search_box.send_keys(Keys.RETURN)

            # Wait for search results
            WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div[data-component-type='s-search-result']"))
            )

            # Scroll to load more results
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

            products = []
            product_elements = driver.find_elements(By.CSS_SELECTOR, "div[data-component-type='s-search-result']")

            for product in product_elements[:20]:
                try:
                    # Name extraction
                    name_elements = product.find_elements(By.CSS_SELECTOR, "h2 a span")
                    name = name_elements[0].text if name_elements else "N/A"

                    # Price extraction with multiple approaches
                    price_selectors = [
                        ".a-price-whole",
                        ".a-price-fraction",
                        "span.a-price"
                    ]
                    price = "N/A"
                    for selector in price_selectors:
                        price_elements = product.find_elements(By.CSS_SELECTOR, selector)
                        if price_elements:
                            price = ' '.join([p.text for p in price_elements[:2]])
                            break

                    # Rating extraction
                    rating_elements = product.find_elements(By.CSS_SELECTOR, "span.a-icon-alt")
                    rating = rating_elements[0].text.split()[0] if rating_elements else "N/A"

                    products.append({
                        "name": name,
                        "price": price,
                        "rating": rating,
                        "category": category,
                        "website": "Amazon",
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
                except Exception as product_error:
                    logging.warning(f"Amazon product extraction error: {product_error}")

            return products
        except Exception as e:
            logging.error(f"Amazon scraping error for {category}: {e}")
            return []

    def scrape_bestbuy(self, driver, category):
        """Enhanced Best Buy scraping method"""
        try:
            # Navigate to Best Buy
            driver.get("https://www.bestbuy.com")
            
            # Wait and search
            search_box = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input.search-input"))
            )
            search_box.clear()
            search_box.send_keys(category)
            search_box.send_keys(Keys.RETURN)

            # Wait for search results
            WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li.sku-item"))
            )

            # Scroll to load more results
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)

            products = []
            product_elements = driver.find_elements(By.CSS_SELECTOR, "li.sku-item")

            for product in product_elements[:20]:
                try:
                    # Name extraction
                    name_elements = product.find_elements(By.CSS_SELECTOR, "h4.sku-title")
                    name = name_elements[0].text if name_elements else "N/A"

                    # Price extraction with multiple approaches
                    price_selectors = [
                        "div.priceView-hero-price.priceView-customer-price span",
                        "div.priceView-price span",
                        "div.price-block span"
                    ]
                    price = "N/A"
                    for selector in price_selectors:
                        price_elements = product.find_elements(By.CSS_SELECTOR, selector)
                        if price_elements:
                            price = price_elements[0].text
                            break

                    # Rating extraction
                    rating_elements = product.find_elements(By.CSS_SELECTOR, "span.c-rating")
                    rating = rating_elements[0].text.split()[0] if rating_elements else "N/A"

                    products.append({
                        "name": name,
                        "price": price,
                        "rating": rating,
                        "category": category,
                        "website": "BestBuy",
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
                except Exception as product_error:
                    logging.warning(f"Best Buy product extraction error: {product_error}")

            return products
        except Exception as e:
            logging.error(f"Best Buy scraping error for {category}: {e}")
            return []

    def save_to_csv(self, products):
        """Save scraped products to CSV with error handling"""
        if not products:
            logging.warning("No products to save")
            return None

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(self.LOG_DIR, f'products_{timestamp}.csv')

        try:
            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['name', 'price', 'rating', 'category', 'website', 'timestamp']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(products)

            logging.info(f"Saved {len(products)} products to {filename}")
            return filename
        except Exception as e:
            logging.error(f"Error saving to CSV: {e}")
            return None

    def clean_and_process_data(self):
        """Enhanced data cleaning and processing"""
        logging.info("Starting data processing...")
        
        # Find latest CSV file
        csv_files = [f for f in os.listdir(self.LOG_DIR) if f.startswith('products_') and f.endswith('.csv')]
        if not csv_files:
            logging.warning("No data to process.")
            return None

        latest_file = max(csv_files)
        df = pd.read_csv(os.path.join(self.LOG_DIR, latest_file))

        # Advanced price cleaning
        def parse_price(price):
            try:
                # Remove currency symbols, commas, and handle multiple price formats
                cleaned_price = str(price).replace('$', '').replace(',', '').split()[0]
                return float(cleaned_price)
            except:
                return np.nan

        # Advanced rating cleaning
        def parse_rating(rating):
            try:
                # Handle various rating formats
                if rating == 'N/A':
                    return np.nan
                return float(str(rating).split()[0])
            except:
                return np.nan

        df['price_cleaned'] = df['price'].apply(parse_price)
        df['rating_numeric'] = df['rating'].apply(parse_rating)
        
        # Remove duplicates and handle missing values
        df = df.drop_duplicates(subset=['name', 'timestamp'])
        df['rating_numeric'] = df['rating_numeric'].fillna(df['rating_numeric'].median())
        df['price_cleaned'] = df['price_cleaned'].fillna(df['price_cleaned'].median())

        # Save processed data
        processed_file = os.path.join(self.PROCESSED_DATA_DIR, f'processed_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
        df.to_csv(processed_file, index=False)
        logging.info(f"Processed data saved to {processed_file}")

        return df

    def analyze_product_data(self, df):
        """Comprehensive data analysis and visualization"""
        if df is None or df.empty:
            logging.warning("Cannot perform analysis without data.")
            return

        # Ensure output directory exists
        os.makedirs(self.ANALYSIS_OUTPUT_DIR, exist_ok=True)

        # Price Analysis by Category and Website
        price_analysis = df.groupby(['website', 'category'])['price_cleaned'].agg(['mean', 'median', 'min', 'max'])
        price_analysis.to_csv(os.path.join(self.ANALYSIS_OUTPUT_DIR, 'price_analysis.csv'))

        # Price Distribution Visualization
        plt.figure(figsize=(15, 8))
        sns.boxplot(x='category', y='price_cleaned', hue='website', data=df)
        plt.title('Price Distribution by Product Category and Website')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(os.path.join(self.ANALYSIS_OUTPUT_DIR, 'price_distribution.png'))
        plt.close()

        # Rating Analysis
        rating_analysis = df.groupby(['website', 'category'])['rating_numeric'].mean()
        rating_analysis.to_csv(os.path.join(self.ANALYSIS_OUTPUT_DIR, 'rating_analysis.csv'))

        logging.info("Data analysis completed. Results saved in analysis output directory.")

    def scrape_all_sources(self):
        """Robust scraping of all configured sources"""
        all_products = []
        driver = self.init_driver()

        if not driver:
            logging.error("Failed to initialize web driver")
            return

        try:
            for website in self.WEBSITES:
                for category in self.PRODUCT_CATEGORIES:
                    logging.info(f"Scraping {website} for {category}")
                    
                    try:
                        if website == "Amazon":
                            products = self.scrape_amazon(driver, category)
                        elif website == "BestBuy":
                            products = self.scrape_bestbuy(driver, category)
                        else:
                            logging.error(f"Unsupported website: {website}")
                            continue

                        all_products.extend(products)
                    except Exception as category_error:
                        logging.error(f"Error scraping {website} - {category}: {category_error}")
                    
                    # Pause between category scrapes to avoid rate limiting
                    time.sleep(5)

            # Save collected products
            self.save_to_csv(all_products)
        except Exception as e:
            logging.error(f"Comprehensive scraping error: {e}")
        finally:
            # Ensure driver is closed
            if driver:
                driver.quit()

    def run_scheduler(self):
        """Enhanced scheduler with error handling"""
        import signal
        import sys

        def signal_handler(sig, frame):
            """Handle graceful shutdown"""
            logging.info("Stopping E-commerce Product Tracker...")
            sys.exit(0)

        # Register signal handler
        signal.signal(signal.SIGINT, signal_handler)

        logging.info("E-commerce Product Tracker Starting...")
        
        try:
            # Initial scrape and analysis
            self.scrape_all_sources()
            processed_data = self.clean_and_process_data()
            if processed_data is not None:
                self.analyze_product_data(processed_data)
            
            # Schedule periodic scraping
            schedule.every(24).hours.do(self.scrape_all_sources)
            
            # Schedule periodic analysis
            schedule.every(24).hours.do(self._periodic_analysis)
            
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
        
        except Exception as e:
            logging.error(f"Fatal error in scheduler: {e}")
        finally:
            logging.info("E-commerce Product Tracker Shutting Down...")

    def _periodic_analysis(self):
        """Separate method for periodic data processing and analysis"""
        try:
            processed_data = self.clean_and_process_data()
            if processed_data is not None:
                self.analyze_product_data(processed_data)
        except Exception as e:
            logging.error(f"Error during periodic analysis: {e}")

def main():
    tracker = EcommerceProductTracker()
    tracker.run_scheduler()

if __name__ == "__main__":
    main()