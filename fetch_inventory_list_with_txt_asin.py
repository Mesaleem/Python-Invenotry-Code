import time
import requests
import pandas as pd
import os
import urllib.parse
import math
import csv
from datetime import datetime, timedelta
import hashlib
import json
from urllib.parse import urlencode
import gzip


# Sellers to skip
SKIP_SELLERS = {"A1NP2F96NY5Z4E", "A2KSYAFJPV173T"}

# Target seller to check for
TARGET_SELLER = "AEPAL1VNONJKV"

# Start Here Shabanam Credential

# Your credentials
# Your credentials
CLIENT_ID = "amzn1.application-oa2-client.ce3da6023b9c4d10bb773e8da9530889"
CLIENT_SECRET = "amzn1.oa2-cs.v1.c609002f30e4e1ef51a7cfe4d3a9ce0a81d13ec49f1bc2e32337fda6b00e07aa"  # Verify this
REFRESH_TOKEN = "Atzr|IwEBIC75LrkF4nSRNH-CJgsgcUc9oahP1ROPj7JtsVwLU7bLsvedCiPx5XQOuhoB-x0etiy5dlHrDLrBqT_1mbcq43QEWxKJcCx5X-9teBi4Pn2Ow7KTSWuE2thUp22b8R8I6wGkjvuf5p2S3KuR3XIfYmVBgsmWBe5gZoEuB82vPMvXqehiyyH6vz6Y4j5ojKN6xoSjWp1xbOJGEQ1kIhQoaScVFduw5pbJF6Dr9nj3QH6jI4CWXZVIRm7vvsgdz0BdVIkdzJ0-uiD9w2t4oy8H94LD_z0c7PzMXQbYqV0KdbckRDtO22mdRS_lHNi7Xy7tdrX7KIczQO6DQ03JmqRGnO8e11q8V82-i8kKc7E5XiHZ4A"  # Replace with actual refresh token
SP_API_BASE_URL = "https://sellingpartnerapi-fe.amazon.com"  # Japan endpoint
MARKETPLACE_ID = "A1VC38T7YXB528"  # Japan marketplace ID

SELLER_ID = "A1NP2F96NY5Z4E"  # Replace with your seller ID

# End Here Shabanam Credential

access_token = None
token_expiry = 0

def get_access_token():
    global access_token, token_expiry
    token_url = "https://api.amazon.com/auth/o2/token"
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    try:
        response = requests.post(token_url, data=payload)
        response.raise_for_status()  # गैर-200 स्टेटस के लिए त्रुटि उठाएँ
        token_data = response.json()
        access_token = token_data["access_token"]
        token_expiry = int(time.time()) + 3300  # 55 मिनट
        print(f"New Access Token generated: {access_token}")
        return access_token
    except requests.exceptions.RequestException as e:
        print(f"Failed to get access token: {e}")
        print(f"Response: {response.text}")
        raise Exception(f"Failed to get access token: {response.text}")

# टोकन चेक और रिन्यू
def ensure_valid_token():
    global access_token, token_expiry
    current_time = int(time.time())
    if not access_token or current_time >= token_expiry:
        print("Token expired or not set, renewing...")
        return get_access_token()
    print("Using existing valid token.")
    return access_token


def get_offer_price(asin, condition, max_retries=3, retry_delay=2):
    access_token = ensure_valid_token()
    headers = {"x-amz-access-token": access_token, "content-type": "application/json"}
    endpoint = f"{SP_API_BASE_URL}/products/pricing/v0/items/{asin}/offers"
    params = {
        "MarketplaceId": MARKETPLACE_ID,
        "ItemCondition": condition,  # "Used" or "New"
        "OfferType": "B2C",
    }
    for attempt in range(max_retries):
        response = requests.get(endpoint, headers=headers, params=params)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:
            print(
                f"429 Error for ASIN {asin}. Retrying in {retry_delay} seconds... (Attempt {attempt + 1}/{max_retries})"
            )
            time.sleep(retry_delay)
            retry_delay *= 2
        else:
            print(f"Failed for ASIN {asin} : {response.status_code} - {response.text}")
            return None
    print(f"Max retries reached for ASIN {asin}. Giving up.")
    return None


def check_target_seller_and_get_lowest_price(asin):
    """
    Check if target seller is selling the ASIN in New condition only and get lowest price
    """
    skip_sellers = SKIP_SELLERS
    target_seller = TARGET_SELLER
    
    # Check only New condition for target seller
    offer_data = get_offer_price(asin, "New")
    if not offer_data:
        print(f"No New condition offers found for ASIN {asin}")
        return None
        
    offers = offer_data.get("payload", {}).get("Offers", [])
    
    # First check if target seller is selling this ASIN in New condition
    target_seller_found = False
    for offer in offers:
        seller_id = offer.get("SellerId", "N/A")
        if seller_id == target_seller:
            target_seller_found = True
            print(f"Target seller {target_seller} found selling ASIN {asin} in New condition")
            break
    
    if not target_seller_found:
        print(f"Target seller {target_seller} not found for ASIN {asin} in New condition. Skipping...")
        return None
    
    # Now get lowest price from all New condition offers
    lowest_price = float("inf")
    for offer in offers:
        seller_id = offer.get("SellerId", "N/A")
        if seller_id in skip_sellers:
            continue

        price = offer.get("ListingPrice", {}).get("Amount", 0) or 0
        shipping = offer.get("Shipping", {}).get("Amount", 0) or 0
        total = price + shipping

        if total < lowest_price:
            lowest_price = total

    if lowest_price == float("inf"):
        print(f"No valid New condition offers found for ASIN {asin}")
        return None

    # Apply price adjustment based on price ranges
    if 1200 <= lowest_price < 2000:
        adjusted_price = lowest_price - 300 
    elif 2000 <= lowest_price < 3000:
        adjusted_price = lowest_price - 500  
    elif 3000 <= lowest_price < 4000:
        adjusted_price = lowest_price - 800  
    elif 4000 <= lowest_price < 5000:
        adjusted_price = lowest_price - 1200  
    elif lowest_price >= 5000:
        adjusted_price = lowest_price - 1500
    else:
        adjusted_price = lowest_price  # No adjustment if < 1200
    
    # Make sure price doesn't go below a reasonable minimum
    if adjusted_price < 100:
        adjusted_price = 100
    
    print(f"ASIN {asin}: Lowest New price: {lowest_price}, Adjusted price: {adjusted_price}")
    return int(math.floor(adjusted_price / 10.0)) * 10


def get_asin_from_isbn13(isbn13):
    access_token = ensure_valid_token()
    path = "/catalog/2022-04-01/items"
    query = f"identifiers={isbn13}&identifiersType=ISBN&marketplaceIds={MARKETPLACE_ID}&includedData=identifiers"
    url = f"{SP_API_BASE_URL}{path}?{query}"

    headers = {
        "x-amz-access-token": access_token,
        "content-type": "application/json"
    }

    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        data = response.json()
        if "items" in data and data["items"]:
            asin = data["items"][0]["asin"]
            return asin
        else:
            return None
    else:
        return None


def read_asins_from_file(filename):
    """
    Read ASINs from txt file. Assumes one ASIN per line.
    """
    asins = []
    try:
        with open(filename, 'r', encoding='utf-8') as file:
            for line in file:
                asin = line.strip()
                if asin:  # Skip empty lines
                    asins.append(asin)
        print(f"Read {len(asins)} ASINs from {filename}")
        return asins
    except FileNotFoundError:
        print(f"File {filename} not found!")
        return []
    except Exception as e:
        print(f"Error reading file {filename}: {str(e)}")
        return []


def process_asins_from_txt_file(txt_filename):
    """
    Process ASINs from txt file and generate CSV output with individual entries for safety
    """
    # Read ASINs from txt file
    asins = read_asins_from_file(txt_filename)
    
    if not asins:
        print("No ASINs found in file. Exiting...")
        return
    
    # Create output CSV file with header
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    output_file = f"target_seller_price_check_{timestamp}.csv"
    
    # Write header only once
    with open(output_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "asin",
            "sku", 
            "price",
            "quantity",
            "currency",
            "sale-price",
            "sale-from-date",
            "sale-through-date",
            "restock-date",
            "minimum-seller-allowed-price",
            "maximum-seller-allowed-price",
            "fulfillment-channel",
            "handling-time",
        ])
    
    processed_count = 0
    successful_count = 0
    
    for asin in asins:
        processed_count += 1
        print(f"\n=== Processing ASIN {processed_count}/{len(asins)}: {asin} ===")
        
        try:
            # Handle ISBN13 to ASIN conversion if needed
            if len(asin) == 13 and not asin.startswith("890"):
                print(f"Converting ISBN-13 {asin} to ASIN...")
                converted_asin = get_asin_from_isbn13(asin)
                if converted_asin:
                    print(f"Converted to ASIN: {converted_asin}")
                    final_asin = converted_asin
                else:
                    print(f"Could not convert ISBN-13 {asin} to ASIN. Skipping...")
                    continue
            else:
                final_asin = asin
            
            # Check if target seller is selling this ASIN in New condition and get lowest price
            adjusted_price = check_target_seller_and_get_lowest_price(final_asin)
            
            if adjusted_price is not None:
                successful_count += 1
                min_price = adjusted_price - 100
                max_price = adjusted_price + 100
                
                # Generate SKU (you can modify this logic as needed)
                sku = f"SKU_{final_asin}_{int(time.time())}"
                
                print(f"✅ SUCCESS! Price for ASIN {final_asin}: {adjusted_price} (Min: {min_price}, Max: {max_price})")
                
                row = [
                    final_asin,  # ASIN
                    sku,
                    adjusted_price,
                    2,
                    "JPY",
                    "",
                    "",
                    "",
                    "",
                    min_price,
                    max_price,
                    "",
                    10,
                ]
                
                # Write individual entry immediately to CSV (safe approach)
                with open(output_file, "a", newline="", encoding="utf-8") as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(row)
                
                print(f"✅ Entry saved to CSV for ASIN {final_asin}")
            else:
                print(f"❌ SKIPPED ASIN {final_asin} - Target seller not found in New condition or no valid offers")
            
            # Add small delay to avoid rate limiting
            time.sleep(1)
            
        except Exception as e:
            print(f"❌ ERROR processing ASIN {asin}: {str(e)}")
            continue
        
        # Progress update every 10 ASINs
        if processed_count % 10 == 0:
            print(f"\n📊 Progress Update: {processed_count}/{len(asins)} ASINs processed, {successful_count} successful entries")
    
    print(f"\n🎉 === Processing Complete ===")
    print(f"Total ASINs processed: {processed_count}")
    print(f"Successful entries: {successful_count}")
    print(f"Success rate: {(successful_count/processed_count)*100:.1f}%")
    print(f"Output file: {output_file}")
    print(f"📁 CSV file location: {os.path.abspath(output_file)}")
    
    return output_file


# Main function
def main():
    try:
        print("=== Amazon Price Checker from TXT File ===")
        print("Fetching access token...")
        access_token = ensure_valid_token()
        
        # Specify your txt file name here
        txt_filename = "asins.txt"  # Change this to your actual file name
        
        print(f"Processing ASINs from {txt_filename}...")
        process_asins_from_txt_file(txt_filename)
        
    except Exception as e:
        print(f"Error: {str(e)}")
        print("Check CLIENT_SECRET, REFRESH_TOKEN, and IAM role permissions.")

if __name__ == "__main__":
    main()