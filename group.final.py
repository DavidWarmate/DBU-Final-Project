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
from webdriver_manager.chrome import ChromeDriverManager

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

    def init_driver(self):
        """Initialize Selenium WebDriver"""
        options = webdriver.ChromeOptions()
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        return driver

    def scrape_amazon(self, driver, category):
        """Scrape Amazon for a specific product category"""
        try:
            driver.get("https://www.amazon.com")
            search_box = WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.ID, "twotabsearchtextbox"))
            )
            search_box.clear()
            search_box.send_keys(category)
            search_box.send_keys(Keys.RETURN)

            products = []
            product_elements = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "div[data-component-type='s-search-result']"))
            )

            for product in product_elements[:20]:
                try:
                    name = product.find_element(By.CSS_SELECTOR, "h2 a span").text
                    try:
                        price = product.find_element(By.CSS_SELECTOR, ".a-price-whole").text
                    except:
                        price = "N/A"
                    try:
                        rating = product.find_element(By.CSS_SELECTOR, "span.a-icon-alt").text
                    except:
                        rating = "N/A"

                    products.append({
                        "name": name,
                        "price": price,
                        "rating": rating,
                        "category": category,
                        "website": "Amazon",
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
                except Exception as e:
                    logging.error(f"Error parsing Amazon product: {e}")

            return products
        except Exception as e:
            logging.error(f"Error scraping Amazon for {category}: {e}")
            return []

    def scrape_bestbuy(self, driver, category):
        """Scrape Best Buy for a specific product category"""
        try:
            driver.get("https://www.bestbuy.com")
            search_box = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "input.search-input"))
            )
            search_box.clear()
            search_box.send_keys(category)
            search_box.send_keys(Keys.RETURN)

            products = []
            product_elements = WebDriverWait(driver, 10).until(
                EC.presence_of_all_elements_located((By.CSS_SELECTOR, "li.sku-item"))
            )

            for product in product_elements[:20]:
                try:
                    name = product.find_element(By.CSS_SELECTOR, "h4.sku-title").text
                    try:
                        price = product.find_element(By.CSS_SELECTOR, "div.priceView-hero-price.priceView-customer-price span").text
                    except:
                        price = "N/A"
                    try:
                        rating = product.find_element(By.CSS_SELECTOR, "span.c-rating.v-small").text
                    except:
                        rating = "N/A"

                    products.append({
                        "name": name,
                        "price": price,
                        "rating": rating,
                        "category": category,
                        "website": "BestBuy",
                        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
                except Exception as e:
                    logging.error(f"Error parsing Best Buy product: {e}")

            return products
        except Exception as e:
            logging.error(f"Error scraping Best Buy for {category}: {e}")
            return []

    def save_to_csv(self, products):
        """Save scraped products to CSV"""
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
        """Clean and process scraped data"""
        logging.info("Starting data processing...")
        
        # Find latest CSV file
        csv_files = [f for f in os.listdir(self.LOG_DIR) if f.startswith('products_') and f.endswith('.csv')]
        if not csv_files:
            logging.warning("No data to process.")
            return None

        latest_file = max(csv_files)
        df = pd.read_csv(os.path.join(self.LOG_DIR, latest_file))

        # Clean price data
        def parse_price(price):
            try:
                return float(str(price).replace('$', '').replace(',', ''))
            except:
                return np.nan

        df['price_cleaned'] = df['price'].apply(parse_price)
        
        # Remove duplicates
        df = df.drop_duplicates(subset=['name', 'timestamp'])
        
        # Handle missing values
        df['rating'] = df['rating'].fillna('N/A')
        df['price_cleaned'] = df['price_cleaned'].fillna(df['price_cleaned'].median())

        # Save processed data
        processed_file = os.path.join(self.PROCESSED_DATA_DIR, f'processed_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
        df.to_csv(processed_file, index=False)
        logging.info(f"Processed data saved to {processed_file}")

        return df

    def analyze_product_data(self, df):
        """Perform data analysis and generate visualizations"""
        if df is None:
            logging.warning("Cannot perform analysis without data.")
            return

        # Price Analysis by Category
        price_by_category = df.groupby('category')['price_cleaned'].agg(['mean', 'median', 'min', 'max'])
        price_by_category.to_csv(os.path.join(self.ANALYSIS_OUTPUT_DIR, 'price_analysis.csv'))

        # Price Distribution Visualization
        plt.figure(figsize=(12, 6))
        sns.boxplot(x='category', y='price_cleaned', data=df)
        plt.title('Price Distribution by Product Category')
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(os.path.join(self.ANALYSIS_OUTPUT_DIR, 'price_distribution.png'))
        plt.close()

        # Rating Analysis
        def parse_rating(rating):
            try:
                return float(rating.split()[0])
            except:
                return 0

        df['rating_numeric'] = df['rating'].apply(parse_rating)
        rating_by_category = df.groupby('category')['rating_numeric'].mean()
        rating_by_category.to_csv(os.path.join(self.ANALYSIS_OUTPUT_DIR, 'rating_analysis.csv'))

        # Price Comparison Across Websites
        website_price_comparison = df.groupby(['website', 'category'])['price_cleaned'].mean().unstack()
        website_price_comparison.to_csv(os.path.join(self.ANALYSIS_OUTPUT_DIR, 'website_price_comparison.csv'))

        logging.info("Data analysis completed. Results saved in analysis output directory.")

    def scrape_all_sources(self):
        """Scrape all configured websites and categories"""
        all_products = []
        driver = self.init_driver()

        try:
            for website in self.WEBSITES:
                for category in self.PRODUCT_CATEGORIES:
                    logging.info(f"Scraping {website} for {category}")
                    
                    if website == "Amazon":
                        products = self.scrape_amazon(driver, category)
                    elif website == "BestBuy":
                        products = self.scrape_bestbuy(driver, category)
                    else:
                        logging.error(f"Unsupported website: {website}")
                        continue

                    all_products.extend(products)
                    time.sleep(5)  # Pause between scrapes

            self.save_to_csv(all_products)
        except Exception as e:
            logging.error(f"Error during scraping: {e}")
        finally:
            driver.quit()

    def run_scheduler(self):
        """Enhanced scheduler with controlled scraping and analysis"""
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