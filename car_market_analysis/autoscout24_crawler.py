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

# CLI initialization
cli = typer.Typer()

# Directory setup to store cached data
cache_dir = 'data_cache'
memory = Memory(cache_dir, verbose=0)

MIN_PRICE = 3000
MAX_PRICE = 12500

# Base URL
base_url_template = (
    'https://www.autoscout24.de/lst/{brand_model}/ft_benzin?atype=C&cy={country}&page={page}&damaged_listing=exclude&desc=1'
    'fuel=B'
    '&kmto=150000'
    '&ocs_listing=include'
    '&powertype=kw'
    '&search_id=n946uue9uo'
    '&sort=age&source=detailpage_back-to-list&ustate=N%2CU'
    '&fregfrom={year}&fregto={year}&'
    f"&pricefrom={MIN_PRICE}&priceto={MAX_PRICE}"
)
countries = ['D', 'NL']

years = [year for year in range(2010, 2024)]

brand_model_combinations = [
    "ford/fiesta",
    "ford/focus",
    "ford/mondeo",
    "volkswagen/golf",
    "volkswagen/passat",
    "volkswagen/polo",
    "audi/a3",
    "audi/a4",
    "audi/a6",
    "bmw/1er-(alle)",
    "bmw/3er-(alle)",
    "skoda/fabia",
    "skoda/octavia",
    "skoda/superb",
    "toyota/auris",
    "toyota/avensis",
    "mercedes/a-klasse",
    "mercedes/c-klasse",
    "mercedes/e-klasse",
    "honda/civic",
    "honda/accord",
    "mazda/3",
    "mazda/6",
    "peugeot/308",
    "peugeot/508",
    "renault/megane",
    "renault/talisman",
    "hyundai/i30",
    "hyundai/i40",
    "kia/ceed",
    "kia/optima",
    "nissan/qashqai",
    "nissan/juke",
]


@memory.cache
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

        return data
    except Exception as e:
        logger.error(f"Parsing error: {e}")
        raise


def store_data(country, id, raw_html, json_data):
    # Create directories if they do not exist
    os.makedirs(f'data/as24/{country}/raw/', exist_ok=True)
    os.makedirs(f'data/as24/{country}/json/', exist_ok=True)

    # Write raw HTML
    with open(f'data/as24/{country}/raw/{id}.html', 'w') as raw_file:
        raw_file.write(raw_html)

    # Write JSON
    with open(f'data/as24/{country}/json/{id}.json', 'w') as json_file:
        json.dump(json_data, json_file, indent=4)

def scrape_autoscout24(country: str, brand_model: str, year: int):
    page  = 1 
    pagination_next = True
    seen_ads = set()
    threshold_seen_ad = 0.50  # Stop if more than 50% ads have already been seen
    
    # Prepare progress bar
    pbar = tqdm(desc="Processing pages", unit="page")

    with logging_redirect_tqdm():
        while pagination_next:
            url = base_url_template.format(country=country, page=page, brand_model=brand_model, year=year)
            # logger.debug(f"Fetching URL: {url}")
            page_html = fetch_page(url)
            soup = BeautifulSoup(page_html, 'html.parser')

            # Parse listings
            listings = soup.find_all('article', class_='cldt-summary-full-item')
            if not listings:
                logger.warning("No listings found. Check selectors or page structure.")
                logger.warning(f"URL: {url}")
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
            
                    raw_html_chunk = str(listing)
                    json_data = parse_listing(listing)
            
                    # Store data
                    store_data(country, ad_id, raw_html_chunk, json_data)
            
                    seen_ads.add(ad_id)
                except Exception as e:
                    logger.error(f"Error processing ad: {e}")
                    logger.error(f"Skipping ad: {listing}")
                    logger.error(f"Error stack: {traceback.format_exc(limit=2)}")
            # Check if we should stop pagination
            if fetched_ads / len(listings) >= threshold_seen_ad:
                logger.info("Reached threshold of seen ads. Stopping pagination.")
                break
        
            # Update progress bar
            pbar.update(1)

            # Find next page URL for pagination
            next_page = soup.find_all('li', class_='prev-next')[1]
            if next_page and 'pagination-item--disabled' not in next_page.get('class', []):
                page += 1
            else:
                pagination_next = False


@cli.command()
def main(cache: bool = typer.Option(True, "--cache/--no-cache", help="Enable or disable caching")):
    """
    Scrapes autoscout24.de with the given filters for cars in the specified country.
    """
    for country, brand_model, year in itertools.product(countries, brand_model_combinations, years):
        logger.info(f"Scraping data for {brand_model} in {country} for year {year}")
        scrape_autoscout24(country, brand_model, year)

if __name__ == "__main__":
    typer.run(main)
