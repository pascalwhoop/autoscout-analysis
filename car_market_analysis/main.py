import requests
import pandas as pd
from joblib import Memory
import os
import typer
import logging
from tqdm import tqdm

# Initialize Typer app
app = typer.Typer()

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Step 2: Define Constants
GRAPHQL_ENDPOINT = 'https://api-customer.prod.retail.auto1.cloud/v1/retail-customer-gateway/graphql'
HEADERS = {
    'accept': '*/*',
    'accept-language': 'en-US,en;q=0.9',
    'content-type': 'application/json',
    'origin': 'https://www.autohero.com',
    'referer': 'https://www.autohero.com/',
    'user-agent': 'Mozilla/5.0',
}
BATCH_SIZE = 24

# Step 3: GraphQL Query Template
QUERY_TEMPLATE = {
    "operationName": "searchAdV9AdsV2",
    "variables": {
        "search": {
            "offset": 0, "limit": 24, "sort": "most_popular",
            "filter": {"field": "countryCode", "op": "eq", "value": "NL"},
            "aggs": [], "postFilter": None, "fields": ["registration"],
            "properties": {
                "firstPublishedDays": 30, "shuffleCategoryBResults": True,
                "resultsCombiner": "abbabbc", "filterByEligibleDate": True
            }
        }
    },
    "query": "query searchAdV9AdsV2($search: EsSearchRequestProjectionInput!, $tradeInId: UUID) {\n  searchAdV9AdsV2(search: $search, tradeInId: $tradeInId)\n}"
}

# Step 4: Setup Joblib Memory for Caching
MEMORY = Memory("./joblib_cache", verbose=0)

@MEMORY.cache
def fetch_data(query):
    response = requests.post(GRAPHQL_ENDPOINT, json=query, headers=HEADERS)
    return response.json()

# Step 5: Parse API Response Data
def parse_response(response):
    ads = response['data']['searchAdV9AdsV2']['data']
    return [ad for ad in ads]

# Step 6: Pagination Logic
def crawl(country):
    offset = 0
    all_data = []
    processed_ids = set()
    QUERY_TEMPLATE["variables"]["search"]["filter"]["value"] = country

    logger.info(f'Starting data fetch for country: {country}')

    while True:
        QUERY_TEMPLATE["variables"]["search"]["offset"] = offset
        response = fetch_data(QUERY_TEMPLATE)

        if 'errors' in response and len(response['errors']) > 0:
            logger.error(f'Error fetching data: {response["errors"]}')
            break

        ads_data = parse_response(response)
        if not ads_data:
            break

        for ad in ads_data:
            if ad['id'] not in processed_ids:
                all_data.append(ad)
                processed_ids.add(ad['id'])

        offset += BATCH_SIZE

        logger.info(f'Fetched {len(all_data)} records so far.')

    logger.info(f'Finished fetching data for country: {country}')
    return all_data

# Step 7: Saving Data Functionality
def save_data(data, country, format='json'):
    df = pd.DataFrame(data)
    if format == 'json':
        df.to_json(f'{country}_cars.json', orient='records', lines=True)
    elif format == 'parquet':
        df.to_parquet(f'{country}_cars.parquet')
    logger.info(f'Saved {len(data)} records to {country}_cars.{format}')

# Step 8: Main Function to Initiate Crawling and Saving Data
@app.command()
def main(countries: str = typer.Argument("NL,DE"), format: str = typer.Option('json', help="Format in which to save data (json or parquet)")):
    countries_list = countries.split(',')
    for country in tqdm(countries_list, desc="Countries Progress"):
        logger.info(f'Crawling data for country: {country}')
        data = crawl(country)
        logger.info(f'Total records fetched for {country}: {len(data)}')
        save_data(data, country, format=format)

if __name__ == "__main__":
    app()