import os
import json
import undetected_chromedriver as uc
from selectorlib import Extractor
import csv
import urllib.parse
import logging
from tqdm import tqdm
from concurrent.futures import ThreadPoolExecutor

def setup_directories_and_files():
    # Define the directories and files
    config_dir = './project/config'
    urls_dir = './project/urls'
    templates_dir = './project/templates'
    results_dir = './project/results'
    uc_settings_file = os.path.join(config_dir, 'uc_settings.txt')
    script_settings_file = os.path.join(config_dir, 'script_settings.txt')
    urls_list_file = os.path.join(urls_dir, 'urls_list.txt')
    urls_completed_file = os.path.join(urls_dir, 'urls_completed.txt')
    urls_failed_file = os.path.join(urls_dir, 'urls_failed.txt')

    # Create the directories if they do not exist
    os.makedirs(config_dir, exist_ok=True)
    os.makedirs(urls_dir, exist_ok=True)
    os.makedirs(templates_dir, exist_ok=True)
    os.makedirs(results_dir, exist_ok=True)

    return config_dir, urls_dir, templates_dir, results_dir, uc_settings_file, script_settings_file, urls_list_file, urls_completed_file, urls_failed_file

config_dir, urls_dir, templates_dir, results_dir, uc_settings_file, script_settings_file, urls_list_file, urls_completed_file, urls_failed_file = setup_directories_and_files()

# Set up logging
logging.basicConfig(filename='scraping.log', level=logging.INFO, format='%(asctime)s %(levelname)s:%(message)s')
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)s:%(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)

error_logger = logging.getLogger('error_logger')
error_handler = logging.FileHandler('errors.log')
error_formatter = logging.Formatter('%(asctime)s %(levelname)s:%(message)s')
error_handler.setFormatter(error_formatter)
error_logger.addHandler(error_handler)
error_logger.setLevel(logging.ERROR)

# Create the files if they do not exist
for file in [uc_settings_file, script_settings_file, urls_list_file, urls_completed_file, urls_failed_file]:
    if not os.path.exists(file):
        open(file, 'w').close()

# Read the settings from the uc_settings.txt and script_settings.txt files
with open(uc_settings_file, 'r') as file:
    uc_settings = file.read().splitlines()

with open(script_settings_file, 'r') as file:
    script_settings = file.read().splitlines()

# Read the list of URLs from the urls_list.txt file
with open(urls_list_file, 'r', encoding='utf-8') as file:
    urls_list = file.read().splitlines()

def initialize_driver():
    # Initialize undetected-chromedriver
    options = uc.ChromeOptions()
    options.add_argument('--headless')
    options.add_argument('--window-size=800,600')
    options.add_argument('--window-position=0,0')
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-plugins-discovery")
    options.add_argument('--no-first-run')
    options.add_argument('--no-service-autorun')
    options.add_argument('--no-default-browser-check')
    options.add_argument('--window-position=0,0')
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-notifications")
    options.add_argument('--no-sandbox')
    options.add_argument('--ignore-certificate-errors')
    options.add_argument('--disable-dev-shm-usage')
    options.add_experimental_option(
        "prefs",
        {
            "credentials_enable_service": False,
            "profile.password_manager_enabled": False,
            "autofill.profile_enabled": False,
        },
    )
    for setting in uc_settings:
        options.add_argument(setting)
    import shutil

    driver = uc.Chrome(options=options)
    return driver

def load_template(domain):
    # Initialize selectorlib
    template_file = os.path.join(templates_dir, f'{domain}.yml')
    if os.path.exists(template_file):
        logging.info(f'Template exists for domain: {domain}')
        e = Extractor.from_yaml_file(template_file)
        return e
    else:
        logging.warning(f'Template does not exist for domain: {domain}. Skipping URL: {url}')
        return None

def scrape_data(driver, e, url):
    driver.get(url)
    data = e.extract(driver.page_source)
    logging.info(f'Extracted data: {data}')
    # Close the driver
    driver.quit()
    return data

def save_data(data, domain):
    # Save the extracted data to {domain}_results.txt
    with open(os.path.join(results_dir, f'{domain}_results.txt'), 'a') as file:
        file.write(json.dumps(data) + '\n')

def scrape_url(url):
    logging.info(f'Starting to scrape URL: {url}')
    domain = urllib.parse.urlparse(url).netloc.split('.')[-2:-1][0]
    driver = initialize_driver()
    e = load_template(domain)
    if e is not None:
        try:
            data = scrape_data(driver, e, url)
            if data is not None:
                save_data(data, domain)
                logging.info(f'Successfully scraped URL: {url}')
                return data
        except Exception as e:
            error_logger.error(f'Error occurred while scraping URL {url}: {e}')
    return None
# Scrape the URLs sequentially
logging.info('Starting to scrape URLs sequentially')
for url in urls_list:
    result = scrape_url(url)
    if result:
        with open(urls_completed_file, 'a', encoding='utf-8') as file:
            if url.isascii():
                file.write(url + '\n')
                logging.info(f'Moved URL to urls_completed.txt: {url}')
            else:
                logging.warning(f'Skipped non-ASCII URL: {url}')
        urls_list.remove(url)
    else:
        with open(urls_failed_file, 'a', encoding='utf-8') as file:
            if url.isascii():
                file.write(url + '\n')
                logging.info(f'Moved URL to urls_failed.txt: {url}')
            else:
                logging.warning(f'Skipped non-ASCII URL: {url}')
        urls_list.remove(url)
logging.info('Finished scraping URLs sequentially')

# Handle errors gracefully and continue with the next URL if an error occurs
import time
import random
for url in urls_list:
    result = scrape_url(url)
    # Add a delay between each request to avoid being blocked by the server
    try:
        min_delay, max_delay = map(int, script_settings)
        time.sleep(random.uniform(min_delay, max_delay))
    except ValueError:
        error_logger.error("Error: script_settings contains invalid values")
    except Exception as e:
        error_logger.error(f'Error occurred: {e}')
