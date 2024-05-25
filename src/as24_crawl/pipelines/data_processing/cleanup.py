import pandas as pd
import re

def clean_data(crawling_data: pd.DataFrame) -> pd.DataFrame:
    """
    Clean the given DataFrame by processing specific columns.

    Args:
        df (pandas.DataFrame): The DataFrame to be cleaned.

    Returns:
        pandas.DataFrame: The cleaned DataFrame.
    """

    crawling_data['price'] = crawling_data['price'].apply(process_price)
    crawling_data['mileage'] = crawling_data['mileage'].apply(process_mileage)
    crawling_data['first_registration'] = crawling_data['first_registration'].apply(process_first_registration)
    crawling_data['engine_power'] = crawling_data['engine_power'].apply(process_engine_power)
    crawling_data['co2_emission'] = crawling_data['co2_emission'].apply(process_co2_emission)

    return crawling_data

def process_price(price):
    """
    Process the 'price' column by removing currency symbols and formatting.

    Args:
        price (str): The price value to be processed.

    Returns:
        int or None: The processed price value as an integer, or None if the input is invalid.
    """
    if not price:  # Check for null values
        return None
    try:
        return int(price.strip('€ ,-').replace('.', ''))
    except ValueError:
        return None


def process_mileage(mileage):
    """
    Process the 'mileage' column by removing units and formatting.

    Args:
        mileage (str): The mileage value to be processed.

    Returns:
        int or None: The processed mileage value as an integer, or None if the input is invalid.
    """
    try:
        return int(mileage.strip(' km').replace('.', ''))
    except ValueError:
        return None
def process_first_registration(first_registration):
    """
    Convert the 'first_registration' column to datetime format.

    Args:
        first_registration (str): The first registration date to be processed.

    Returns:
        pandas.Timestamp or None: The processed first registration date as a pandas Timestamp,
        or None if the input is invalid.
    """
    try:
        return pd.to_datetime(first_registration, format='%m/%Y', errors='coerce')
    except ValueError:
        return None
def process_engine_power(engine_power):
    """
    Process the 'engine_power' column by extracting the power value.

    Args:
        engine_power (str): The engine power value to be processed.

    Returns:
        int or None: The processed engine power value as an integer, or None if the input is invalid.
    """
    match = re.search(r'(\d+) kW', engine_power)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    return None
def process_co2_emission(co2_emission):
    """
    Process the 'co2_emission' column by extracting the CO2 emission value.

    Args:
        co2_emission (str): The CO2 emission value to be processed.

    Returns:
        int or None: The processed CO2 emission value as an integer, or None if the input is invalid.
    """
    if co2_emission == '-' or co2_emission is None:
        return None
    match = re.search(r'(\d+)', co2_emission)
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    return None
