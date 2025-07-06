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

# Your credentials
CLIENT_ID = "amzn1.application-oa2-client.ce3da6023b9c4d10bb773e8da9530889"
CLIENT_SECRET = "amzn1.oa2-cs.v1.c609002f30e4e1ef51a7cfe4d3a9ce0a81d13ec49f1bc2e32337fda6b00e07aa"  # Verify this
REFRESH_TOKEN = "Atzr|IwEBIC75LrkF4nSRNH-CJgsgcUc9oahP1ROPj7JtsVwLU7bLsvedCiPx5XQOuhoB-x0etiy5dlHrDLrBqT_1mbcq43QEWxKJcCx5X-9teBi4Pn2Ow7KTSWuE2thUp22b8R8I6wGkjvuf5p2S3KuR3XIfYmVBgsmWBe5gZoEuB82vPMvXqehiyyH6vz6Y4j5ojKN6xoSjWp1xbOJGEQ1kIhQoaScVFduw5pbJF6Dr9nj3QH6jI4CWXZVIRm7vvsgdz0BdVIkdzJ0-uiD9w2t4oy8H94LD_z0c7PzMXQbYqV0KdbckRDtO22mdRS_lHNi7Xy7tdrX7KIczQO6DQ03JmqRGnO8e11q8V82-i8kKc7E5XiHZ4A"  # Replace with actual refresh token
SP_API_BASE_URL = "https://sellingpartnerapi-fe.amazon.com"  # Japan endpoint
MARKETPLACE_ID = "A1VC38T7YXB528"  # Japan marketplace ID

SELLER_ID = "A1NP2F96NY5Z4E"  # Replace with your seller ID

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

# Create Report
def create_report(access_token):
    url = f"{SP_API_BASE_URL}/reports/2021-06-30/reports"
    payload = {
        "reportType": "GET_MERCHANT_LISTINGS_ALL_DATA",
        "marketplaceIds": [MARKETPLACE_ID],
        "reportOptions": {
            "language": "en" 
        }
    }
    headers = {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json"
    }
    response = requests.post(url, json=payload, headers=headers)
    
    if response.status_code != 202:
        raise Exception(f"Failed to create report: {response.status_code} - {response.text}")
    return response.json().get("reportId")

# Check Report Status
def check_report_status(access_token, report_id):
    url = f"{SP_API_BASE_URL}/reports/2021-06-30/reports/{report_id}"
    headers = {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json"
    }
    while True:
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise Exception(f"Failed to check report status: {response.status_code} - {response.text}")
        status = response.json().get("processingStatus")
        if status == "DONE":
            return response.json().get("reportDocumentId")
        elif status in ["CANCELLED", "FATAL"]:
            raise Exception(f"Report failed: {status}")
        print("Report processing... Waiting 10 seconds.")
        time.sleep(10)

# Get Report Document Metadata
def get_report_document(access_token, report_document_id):
    url = f"{SP_API_BASE_URL}/reports/2021-06-30/documents/{report_document_id}"
    headers = {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json"
    }
    response = requests.get(url, headers=headers)
    
    if response.status_code != 200:
        raise Exception(f"Failed to get report document: {response.status_code} - {response.text}")
    document_info = response.json()
    print(f"Report document metadata: {json.dumps(document_info, indent=2)}")
    return document_info.get("url"), document_info.get("compressionAlgorithm")


def get_offer_price(asin, condition, max_retries=3, retry_delay=2):
    access_token = ensure_valid_token()
    headers = {"x-amz-access-token": access_token, "content-type": "application/json"}
    endpoint = f"{SP_API_BASE_URL}/products/pricing/v0/items/{asin}/offers"
    params = {
        "MarketplaceId": MARKETPLACE_ID,
        "ItemCondition": condition,  # "Used",
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


def get_lowest_price(asin):
    skip_sellers = SKIP_SELLERS

    def get_prices(condition):
        offer_data = get_offer_price(asin, condition)
        offers = offer_data.get("payload", {}).get("Offers", []) if offer_data else []

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
            return None, None

        # Apply adjustment logic
        if condition == "Used":
            adjusted = lowest_price - 100
        else:  # New condition
            if 1500 <= lowest_price < 2000:
                adjusted = lowest_price - 300
            elif 2000 <= lowest_price < 3000:
                adjusted = lowest_price - 600
            elif 3000 <= lowest_price < 4000:
                adjusted = lowest_price - 900
            elif 4000 <= lowest_price < 5000:
                adjusted = lowest_price - 1400
            elif lowest_price >= 5000:
                adjusted = lowest_price - 2300
            else:
                adjusted = lowest_price  # No discount if < 1500

        return lowest_price, adjusted

    used_unadjusted, used_adjusted = get_prices("Used")
    new_unadjusted, new_adjusted = get_prices("New")

    if used_adjusted is None and new_adjusted is None:
        print(f"No valid offers found for ASIN {asin}")
        return None

    # New logic: Prioritize Used if its unadjusted price is lower
    if used_unadjusted is not None and new_unadjusted is not None:
        if used_unadjusted < new_unadjusted:
            final = used_adjusted  # Use adjusted Used price
        else:
            final = new_adjusted if new_adjusted is not None else used_adjusted
    elif used_adjusted is None:
        final = new_adjusted
    else:
        final = used_adjusted

    return int(math.floor(final / 10.0)) * 10


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


    
# Download and Print Condition, ASIN, SKU
def download_and_print_inventory(report_url, compression_algorithm):
    # Download report
    # Write header once at start
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    output_file = f"update_japan_inventory_offers_{timestamp}.csv"
    with open(output_file, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
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
            ]
        )
    response = requests.get(report_url)
    if response.status_code != 200:
        raise Exception(f"Failed to download report: {response.status_code} - {response.text}")
    
    # Handle compression and encoding
    encodings = ["utf-8", "shift-jis", "cp932", "iso-8859-1"]
    lines = None
    if compression_algorithm == "GZIP":
        print("Decompressing GZIP data...")
        try:
            decompressed_data = gzip.decompress(response.content)
            # Try different encodings
            for encoding in encodings:
                try:
                    lines = decompressed_data.decode(encoding).splitlines()
                    print(f"Successfully decoded with {encoding}")
                    break
                except UnicodeDecodeError as e:
                    print(f"Failed to decode with {encoding}: {str(e)}")
            if not lines:
                raise Exception("Failed to decode decompressed data with any encoding.")
        except Exception as e:
            print(f"Failed to decompress GZIP data: {str(e)}")
            raise
    else:
        print("No compression detected. Processing as plain text...")
        for encoding in encodings:
            try:
                lines = response.text.decode(encoding).splitlines()
                print(f"Successfully decoded with {encoding}")
                break
            except UnicodeDecodeError as e:
                print(f"Failed to decode with {encoding}: {str(e)}")
        if not lines:
            raise Exception("Failed to decode plain text data with any encoding.")

    if not lines:
        print("No inventory found.")
        return
    
    headers = lines[0].split("\t")
    print(f"Report headers: {headers}")
    
    # Find column indices
    condition_columns = ["condition-type", "ConditionType", "condition", "コンディション"]
    sku_columns = ["seller-sku", "sku", "SellerSKU", "出品者SKU"]
    asin_columns = ["product-id", "asin", "ProductID", "商品ID"]
    condition_index = -1
    sku_index = -1
    asin_index = -1
    for col in condition_columns:
        if col in headers:
            condition_index = headers.index(col)
            break
    for col in sku_columns:
        if col in headers:
            sku_index = headers.index(col)
            break
    for col in asin_columns:
        if col in headers:
            asin_index = headers.index(col)
            break
    
    if condition_index == -1 or sku_index == -1 or asin_index == -1:
        raise Exception(f"Condition, SKU, or ASIN column not found in report headers. Headers: {headers}")
    
    # Print Condition, ASIN, SKU for each item
    item_count = 0
    for line in lines[1:]:
        item_count += 1
        fields = line.split("\t")
        condition = fields[condition_index] if condition_index < len(fields) else "N/A"
        sku = fields[sku_index] if sku_index < len(fields) else "N/A"
        asin = fields[asin_index] if asin_index < len(fields) else "N/A"
        print(f"Item {item_count}: Condition={condition}, ASIN={asin}, SKU={sku}")
        
        if condition == "3":
            print(f"Condition 3 for ASIN {asin}")
            # break
            if len(asin) == 13:
                if not asin.startswith("890"):
                    newasin = get_asin_from_isbn13(asin)                
                    print(f"ASIN for ISBN-13 {newasin} found")
                else:
                    newasin = None
            else:
                newasin = asin
            if newasin is None:
                continue
            
            print(f"Fetching lowest price for New ASIN {newasin}... || ASIN ... {asin}")
            lowest_price = get_lowest_price(newasin)
            
            if lowest_price is not None:
                min_price = lowest_price - 100
                max_price = lowest_price + 100
                print(
                    f"Lowest price for ASIN {asin}: New Price {lowest_price} : Min Price {min_price} : Max Price {max_price}"
                )
                row = [
                    sku,
                    lowest_price,
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
                with open(
                    output_file, "a", newline="", encoding="utf-8"
                ) as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow(row)
            else:
                print(f"No valid offers found for ASIN {asin}")
    
    print(f"Total items processed: {item_count}")

# Main
def main():
    try:
        print("Fetching access token...")
        access_token = ensure_valid_token()
        print("Creating report...")
        report_id = create_report(access_token)
        print(f"Report created with ID: {report_id}")
        print("Checking report status...")
        report_document_id = check_report_status(access_token, report_id)
        print(f"Report ready with documenthem ID: {report_document_id}")
        print("Fetching report URL and metadata...")
        report_url, compression_algorithm = get_report_document(access_token, report_document_id)
        print(f"Report URL: {report_url}, Compression: {compression_algorithm}")
        print("Downloading and printing inventory...")
        download_and_print_inventory(report_url, compression_algorithm)
    except Exception as e:
        print(f"Error: {str(e)}")
        print("Check CLIENT_SECRET, REFRESH_TOKEN, and IAM role permissions.")

if __name__ == "__main__":
    main()