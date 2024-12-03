from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import json

# Initialize WebDriver
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))

# Navigate to Amazon's bestsellers page
driver.get('https://www.amazon.com/Best-Sellers/zgbs')  # Example page with less aggressive anti-scraping measures

# Initialize a list to store product data
products_data = []

# Create a WebDriverWait object with a maximum timeout of 10 seconds
wait = WebDriverWait(driver, 10)

# Wait for the products to load
wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, 'zg-item')))

# Locate the product elements
products = driver.find_elements(By.CLASS_NAME, 'zg-item')

# Extract data from the first 10 products
for product in products[:10]:
    try:
        # Extract the title
        title = product.find_element(By.CLASS_NAME, 'p13n-sc-truncate').text.strip()
        # Extract the price (not all items may have a price listed)
        try:
            price = product.find_element(By.CLASS_NAME, 'p13n-sc-price').text
        except Exception:
            price = "Price not listed"
        # Append data
        products_data.append({'title': title, 'price': price})
    except Exception as e:
        print(f"Error extracting product data: {e}")

    # Add a random delay between iterations to simulate human behavior
    time.sleep(random.uniform(1, 3))

# Save the scraped data to a JSON file
with open('amazon_top10.json', 'w', encoding='utf-8') as f:
    json.dump(products_data, f, indent=4, ensure_ascii=False)

# Print the extracted data for verification
print(products_data)

# Close the browser
driver.quit()
