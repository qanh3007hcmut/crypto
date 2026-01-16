""" Post API Module. """
import os
import time
import pandas as pd
from datetime import datetime, timedelta

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.support import expected_conditions as EC

class Utils:
    """ Utils Class. """
    @staticmethod
    def get_coin_list():
        # Build the path dynamically
        base_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(base_dir, "database-source", 'coin.csv')
        df = pd.read_csv(file_path)
        coin_list = [{"symbol": row['symbol'], "coinID": row['coinID']} for _, row in df.iterrows()]
        return coin_list

    @staticmethod
    def check_valid_coin(coin_symbols):
        """ Get Valid Coin. """
        # Build the path dynamically
        base_dir = os.path.dirname(os.path.abspath(__file__))
        file_path = os.path.join(base_dir, "database-source", 'valid_pairs.txt')
        with open(file_path, 'r') as file:
            valid_pairs = {line.strip() for line in file.readlines() if line.strip()}

        valid_coin_pairs = []
        for coin in coin_symbols:
            pair1 = f"{coin}USDT"
            pair2 = f"USDT{coin}"

            if pair1 in valid_pairs:
                valid_coin_pairs.append(pair1)
            elif pair2 in valid_pairs:
                valid_coin_pairs.append(pair2)

        return valid_coin_pairs


# Options config Firefox
options = Options()
options.binary_location = "C:/Program Files/Mozilla Firefox/firefox.exe"  # Specify the correct path
options.add_argument("--headless") 


class PostAPI:
    """ Post API Class. """

    def __init__(self, limit: int = 1):
        """ Init. """
        self.limit = limit
    
    def scraping_config(self, coin: str):
        """ Scraping Config. """
        driver = webdriver.Firefox(options=options)

        try:
            base_url = f"https://cryptopanic.com/news/{coin}"
            driver.get(base_url)
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "div.news"))
            )
            urls = [
                link.get_attribute("href") 
                for link in driver.find_elements(
                    By.CSS_SELECTOR, 
                    "div.news-row.news-row-link a.click-area, div.news-row.news-row-reddit a.click-area"
                )
            ]
            news_data = self.scrape_news_data_from_cryptopanic(driver, urls, coin)
            return news_data

        finally:
            driver.quit()
    
    def scrape_news_data_from_cryptopanic(self, driver: WebDriver, urls: list, coin: str):
        """ Scraping News. """
        news_data = []
        num_of_urls = 0 

        for url in urls:
            if num_of_urls >= self.limit:
                break

            driver.get(url)
            time.sleep(2) 

            try:
                main_element = driver.find_element(By.CSS_SELECTOR, "h1.post-title")
                title_element = main_element.find_element(By.CSS_SELECTOR, "a span.text")
                title = title_element.text
                
                time_element = driver.find_element(By.CSS_SELECTOR, "span.post-source time")
                relative_time = time_element.text
                
                current_time = datetime.now()
                time_parts = relative_time.split()
                if "h" in time_parts[1]:
                    delta = timedelta(hours=int(time_parts[0]))
                elif "d" in time_parts[1]:
                    delta = timedelta(days=int(time_parts[0]))
                elif "m" in time_parts[1]:
                    delta = timedelta(minutes=int(time_parts[0]))
                else:
                    delta = timedelta(0)

                actual_time = current_time - delta
                
                source_element = driver.find_element(By.CSS_SELECTOR, "span.post-source span.text-nowrap span.text")
                source = source_element.text
                
                content = self.extract_content(driver)
                news_data.append({
                    "symbol": coin.upper(),
                    "title": title,
                    "url": url,
                    "content": content,
                    "time": actual_time.strftime('%Y-%m-%d %H:%M:%S'),
                    "source": source
                })

                num_of_urls += 1
                print(f"{num_of_urls}. Successfully scraped data from URL: {url}")

            except Exception as e:
                print(f"Error scraping {url}: {e}")
                continue

        return news_data
    
    def extract_content(self, driver: WebDriver):
        """ Extract Content. """
        css_selectors = [
            "div.description div.description-body p",
            "div.description div.description-body",
            "a.reddit-post div.post-body span.post-body-inner"
        ]
        for selector in css_selectors:
            try:
                content_element = driver.find_element(By.CSS_SELECTOR, selector)
                return content_element.text
            except:
                continue
        try:
            loader_element = driver.find_element(By.CSS_SELECTOR, "div.tweet-container span.loader.loader-sm")
            if loader_element:
                content_element = driver.find_element(By.XPATH, '//*[@id="detail_pane"]/div[1]/h1/a[2]')
                return content_element.get_attribute('href')
        except:
            pass
        return "Content not available"

    
    def fetch_all_posts_data(self):
        """ Fetch All Posts Data. """
        coin_list = [coin["symbol"] for coin in Utils.get_coin_list()]
        posts_data = []
        for coin in coin_list:
            news_data = self.scraping_config(coin)
            posts_data.append(news_data)
        return posts_data
    
    def fetch_yield_posts_data(self):
        """Fetch Yield Posts Data using a generator."""
        coin_list = [coin["symbol"] for coin in Utils.get_coin_list()]
        for coin in coin_list:
            news_data = self.scraping_config(coin)
            yield news_data


if __name__ == "__main__":
    """ Test Funcs. """
    post_api = PostAPI()

    # For fetch yield
    for post in post_api.fetch_yield_posts_data():
        print(post)