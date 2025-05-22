
from kafka import KafkaProducer
import json
import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

# Set up Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Set up headless browser
options = Options()
options.add_argument('--headless')
options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome(options=options)

# Scrape PakWheels listings
all_data = []
for page in range(1, 3):  # Scrape first 2 pages
    url = f"https://www.pakwheels.com/used-cars/search/-/?page={page}"
    driver.get(url)
    time.sleep(3)
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(2)

    listings = driver.find_elements(By.CSS_SELECTOR, "li.classified-listing")
    for listing in listings:
        try:
            title = listing.find_element(By.CSS_SELECTOR, "a.car-name").text
            price = listing.find_element(By.CSS_SELECTOR, ".price-details").text
            location = listing.find_element(By.CSS_SELECTOR, "ul.search-vehicle-info li").text
            specs = listing.find_elements(By.CSS_SELECTOR, "ul.search-vehicle-info-2 li")
            year = specs[0].text if len(specs) > 0 else ""
            mileage = specs[1].text if len(specs) > 1 else ""
            engine = specs[3].text if len(specs) > 3 else ""
            url = listing.find_element(By.CSS_SELECTOR, "a.car-name").get_attribute("href")
            img = listing.find_element(By.CSS_SELECTOR, "li.total-pictures-bar-outer img").get_attribute("src")

            record = {
                "title": title,
                "price": price,
                "location": location,
                "mileage": mileage,
                "engine": engine,
                "year": year,
                "url": url,
                "image": img
            }

            producer.send("pakwheels_listings", value=record)
            print(f"✔ Sent to Kafka: {title}")

        except Exception as e:
            print(f"⚠️ Skipped a listing: {e}")

producer.flush()
driver.quit()
print("[✓] All listings sent to Kafka.")
