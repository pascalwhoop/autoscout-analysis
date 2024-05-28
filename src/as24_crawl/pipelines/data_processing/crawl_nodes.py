import math
from typing import Any, Dict, List, Tuple
import pandas as pd
import requests
import itertools
import re
import traceback
from tenacity import retry, stop_after_attempt, wait_exponential, RetryError, retry_if_exception_type, retry_if_result
from bs4 import BeautifulSoup
import os
import json
from joblib import Memory
from datetime import datetime
import typer
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm
from loguru import logger
from urllib.parse import urlencode
import itertools
import logging
from multiprocessing import Pool
from datetime import datetime
from typing import List, Dict, Any
import pandas as pd

# Directory setup to store cached data
cache_dir = 'joblib_cache'
memory = Memory(cache_dir, verbose=0)

logger = logging.getLogger(__name__)

def scrape_job(args):
    url_template, country, brand_model, year, cache = args
    results = scrape_autoscout24(url_template, country, brand_model, year, cache=cache)
    # add brand, model, year, and country to each result
    for res in results:
        res['brand'] = brand_model.split("/")[0]
        res['model'] = brand_model.split("/")[1]
        res['year'] = year
        res['country'] = country

    return results

def crawl_node(base_url: str, year_range: List[int], url_params: Dict[str, Any], countries: List, brand_model_combinations: List):
    """
    Main function to crawl data from autoscout24 with multiprocessing.
    """
    all_results = []

    # prep URL
    years = [year for year in range(year_range[0], year_range[1] + 1)]

    params_str = "&".join([f"{key}={value}" for key, value in url_params.items()])
    url_template = f"{base_url}&{params_str}"
    # used to cache-bust
    today = datetime.now().strftime("%Y-%m-%d")

    # Create a list of all combinations of parameters
    tasks = [
        (url_template, country, brand_model, year, today)
        for country, brand_model, year in itertools.product(countries, brand_model_combinations, years)
    ]

    # Perform multiprocessing
    with Pool() as pool:
        for idx, results in enumerate(pool.imap_unordered(scrape_job, tasks)):
            country, brand_model, year = tasks[idx][1:4]
            all_results.extend(results)
            logger.info(f"Scraping completed for {brand_model} in {country} for year {year}. Pages: {math.ceil(len(results)/20)}")

    logger.info(f"Finished crawling {len(all_results)} records.")
    return pd.DataFrame(all_results)


def fetch_page(url):
    response =  fetch_with_retry(url)
    # response.raise_for_status()
    return response.text


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=4, max=60),
    retry=(retry_if_exception_type(requests.exceptions.RequestException) | 
           retry_if_result(lambda x: x.status_code == 429))
)
def fetch_with_retry(url):
    return requests.get(url)

def parse_listing(listing):
    try:
        data = {}
        try:
            url_element = listing.find_all('a', re.compile(r'ListItem_title__.*'))
            data['url'] = url_element[0]['href'] if url_element else None

            if len(url_element) >= 1:
                subtitle_element = url_element[0].find_all('span', re.compile(r'ListItem_version__.*'))
                data['subtitle'] = subtitle_element[0].get_text(strip=True) if subtitle_element else None

            price_element = listing.find_all('p', re.compile(r'Price_price__.*'))
            data['price'] = price_element[0].get_text(strip=True) if price_element else None

        except IndexError as e:
            logger.error(f"Error extracting key metadata from: {listing}")
            logger.error(f"HTML: {listing.prettify()}")
            logger.error(f"Exception: {e}")
            raise

        # Find all 'span' elements with class matching the pattern
        details = listing.find_all('span', class_=re.compile(r'VehicleDetailTable_item__.*'))

        for detail in details:
            # Extract the `data-testid` attribute
            testid = detail.get('data-testid', '')

            # Map each `data-testid` to the appropriate field in the dictionary
            # Based on the SVG icon it contains.
            if 'mileage_road' in testid:
                data['mileage'] = detail.get_text(strip=True)
            elif 'calendar' in testid:
                data['first_registration'] = detail.get_text(strip=True)
            elif 'gas_pump' in testid:
                data['fuel_type'] = detail.get_text(strip=True)
            elif 'transmission' in testid:
                data['transmission'] = detail.get_text(strip=True)
            elif 'speedometer' in testid:
                data['engine_power'] = detail.get_text(strip=True)
            elif 'leaf' in testid:
                data['co2_emission'] = detail.get_text(strip=True)
            elif 'water_drop' in testid:
                data['fuel_consumption'] = detail.get_text(strip=True)

        
        vat = listing.find('div', class_='Price_vat__iUxNT')
        if vat and 'inkl. MwSt' in vat.get_text(strip=True):
            data['vat_deductible'] = True
        else:
            data['vat_deductible'] = False
        
        # Fail early if required attributes are missing
        required_attrs = ['url', 'price', 'mileage', 'first_registration', 'fuel_type', 'transmission', 'engine_power']
        for attr in required_attrs:
            if attr not in data:
                raise ValueError(f'Missing {attr} in listing')
        
        data['html'] = str(listing)

        return data
    except Exception as e:
        logger.error(f"Parsing error: {e}")
        raise



@memory.cache
def scrape_autoscout24(url_template:str , country: str, brand_model: str, year: int, cache) -> List:

    results = []

    page  = 1 
    pagination_next = True
    seen_ads = set()
    threshold_seen_ad = 0.50  # Stop if more than 50% ads have already been seen
    

    while pagination_next:
        url = url_template.format(country=country, page=page, brand_model=brand_model, year=year)
        # logger.debug(f"Fetching URL: {url}")
        page_html = fetch_page(url)
        soup = BeautifulSoup(page_html, 'html.parser')

        # Parse listings
        listings = soup.find_all('article', class_='cldt-summary-full-item')
        if not listings:
            # logger.warning("No listings found. Check selectors or page structure.")
            # logger.warning(f"URL: {url}")
            pagination_next = False
            continue
    
        fetched_ads = 0
        for listing in listings:
            try:
                res = listing.find_all('a', re.compile(r'ListItem_title__.*'))
                assert len(res) == 1, f"Expected 1 title, found {len(res)}"
                ad_url = res[0]['href']
                ad_id = ad_url.split("/")[-1]  # Extracting the ad id from the URL

                if ad_id in seen_ads:
                    fetched_ads += 1
        
                result = parse_listing(listing)
                results.append(result)
        
        
                seen_ads.add(ad_id)
            except Exception as e:
                logger.error(f"Error processing ad: {e}")
                logger.error(f"Skipping ad: {listing}")
                logger.error(f"Error stack: {traceback.format_exc(limit=2)}")
        # Check if we should stop pagination
        if fetched_ads / len(listings) >= threshold_seen_ad:
            logger.info("Reached threshold of seen ads. Stopping pagination.")
            break
    
        # Find next page URL for pagination
        next_page = soup.find_all('li', class_='prev-next')[1]
        if next_page and 'pagination-item--disabled' not in next_page.get('class', []):
            page += 1
        else:
            pagination_next = False
    return results


