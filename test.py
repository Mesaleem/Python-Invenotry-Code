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

# Sellers to skip
SKIP_SELLERS = {"A1NP2F96NY5Z4E", "A2KSYAFJPV173T"}

# Your credentials
CLIENT_ID = "amzn1.application-oa2-client.ce3da6023b9c4d10bb773e8da9530889"
CLIENT_SECRET = "amzn1.oa2-cs.v1.c609002f30e4e1ef51a7cfe4d3a9ce0a81d13ec49f1bc2e32337fda6b00e07aa"  # Verify this
REFRESH_TOKEN = "Atzr|IwEBIC75LrkF4nSRNH-CJgsgcUc9oahP1ROPj7JtsVwLU7bLsvedCiPx5XQOuhoB-x0etiy5dlHrDLrBqT_1mbcq43QEWxKJcCx5X-9teBi4Pn2Ow7KTSWuE2thUp22b8R8I6wGkjvuf5p2S3KuR3XIfYmVBgsmWBe5gZoEuB82vPMvXqehiyyH6vz6Y4j5ojKN6xoSjWp1xbOJGEQ1kIhQoaScVFduw5pbJF6Dr9nj3QH6jI4CWXZVIRm7vvsgdz0BdVIkdzJ0-uiD9w2t4oy8H94LD_z0c7PzMXQbYqV0KdbckRDtO22mdRS_lHNi7Xy7tdrX7KIczQO6DQ03JmqRGnO8e11q8V82-i8kKc7E5XiHZ4A"  # Replace with actual refresh token
SP_API_BASE_URL = "https://sellingpartnerapi-fe.amazon.com"  # Japan endpoint
MARKETPLACE_ID = "A1VC38T7YXB528"  # Japan marketplace ID

SELLER_ID = "A1NP2F96NY5Z4E"  # Replace with your seller ID
# SP_API_BASE_URL

# North America (US, CA, MX): https://sellingpartnerapi-na.amazon.com
# Europe (UK, DE, FR, etc.): https://sellingpartnerapi-eu.amazon.com
# Far East (JP, AU, SG): https://sellingpartnerapi-fe.amazon.com
# For India : https://sellingpartnerapi-eu.amazon.com market / place id :A21TJRUUN4KGV


# Global variables for token management
access_token = None
token_expiry = 0  # Timestamp when token expires


def get_access_token():
    global access_token, token_expiry
    token_url = "https://api.amazon.com/auth/o2/token"
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
    }
    response = requests.post(token_url, data=payload)
    if response.status_code == 200:
        token_data = response.json()
        access_token = token_data["access_token"]
        # टोकन एक्सपायरी को 55 मिनट बाद सेट करें (सुरक्षा के लिए 5 मिनट पहले रिन्यू)
        token_expiry = int(time.time()) + 3300  # 55 मिनट = 3300 सेकंड
        return access_token
    else:
        raise Exception(f"Failed to get access token: {response.text}")


# टोकन चेक और रिन्यू
def ensure_valid_token():
    global access_token, token_expiry
    current_time = int(time.time())
    if not access_token or current_time >= token_expiry:
        print("Token expired or not set, renewing...")
        return get_access_token()
    return access_token


def get_offer_price(asin, condition, max_retries=3, retry_delay=2):
    access_token = get_access_token()
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


# def get_lowest_price(asin):
#     skip_sellers = SKIP_SELLERS
#     used = True

#     # Step 1: Try USED offers
#     offer_data = get_offer_price(asin, "Used")
#     offers = offer_data.get("payload", {}).get("Offers", []) if offer_data else []

#     valid_used_offers = []
#     for offer in offers:
#         seller_id = offer.get("SellerId", "N/A")
#         if seller_id not in skip_sellers:
#             valid_used_offers.append(offer)

#     if valid_used_offers:
#         # Process valid used offers
#         lowest_price = float("inf")
#         lowest_offer = None

#         for offer in valid_used_offers:
#             price = offer.get("ListingPrice", {}).get("Amount", 0) or 0
#             shipping = offer.get("Shipping", {}).get("Amount", 0) or 0
#             total = price + shipping

#             if total < lowest_price:
#                 lowest_price = total
#                 lowest_offer = offer

#         adjusted_price = lowest_price - 100
#         # print(f"Used Offer: Total={lowest_price}, Adjusted={adjusted_price}")
#         return int(math.floor(adjusted_price / 10.0)) * 10

#     # Step 2: If no valid used offers, check NEW offers
#     print(f"No valid USED offers (non-skipped) for ASIN {asin}. Checking NEW offers...")
#     offer_data = get_offer_price(asin, "New")
#     offers = offer_data.get("payload", {}).get("Offers", []) if offer_data else []
#     used = False

#     if not offers:
#         print(f"No valid NEW offers found for ASIN {asin}")
#         return None

#     # Process NEW offers (skip sellers if needed — optional)
#     lowest_price = float("inf")
#     lowest_offer = None

#     for offer in offers:
#         seller_id = offer.get("SellerId", "N/A")
#         if seller_id in skip_sellers:
#             continue

#         price = offer.get("ListingPrice", {}).get("Amount", 0) or 0
#         shipping = offer.get("Shipping", {}).get("Amount", 0) or 0
#         total = price + shipping

#         if total < lowest_price:
#             lowest_price = total
#             lowest_offer = offer

#     if not lowest_offer:
#         print(f"All NEW offers skipped for ASIN {asin}")
#         return None

#     # Apply pricing logic for NEW
#     if 1500 <= lowest_price < 2000:
#         adjusted_price = lowest_price - 400
#     elif 2000 <= lowest_price < 3000:
#         adjusted_price = lowest_price - 800
#     elif 3000 <= lowest_price < 4000:
#         adjusted_price = lowest_price - 1200
#     elif 4000 <= lowest_price < 5000:
#         adjusted_price = lowest_price - 1600
#     elif lowest_price >= 5000:
#         adjusted_price = lowest_price - 2300
#     else:
#         adjusted_price = lowest_price

#     # print(f"New Offer: Total={lowest_price}, Adjusted={adjusted_price}")
#     return int(math.floor(adjusted_price / 10.0)) * 10


def get_lowest_price(asin):
    skip_sellers = SKIP_SELLERS

    def get_adjusted_price(condition):
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
            return None

        # Apply adjustment logic
        if condition == "Used":
            return lowest_price - 100
        else:  # New condition
            if 1500 <= lowest_price < 2000:
                return lowest_price - 300
            elif 2000 <= lowest_price < 3000:
                return lowest_price - 600
            elif 3000 <= lowest_price < 4000:
                return lowest_price - 900
            elif 4000 <= lowest_price < 5000:
                return lowest_price - 1400
            elif lowest_price >= 5000:
                return lowest_price - 2300
            else:
                return lowest_price  # No discount if < 1500

    used_price = get_adjusted_price("Used")
    new_price = get_adjusted_price("New")

    if used_price is None and new_price is None:
        print(f"No valid offers found for ASIN {asin}")
        return None
    elif used_price is None:
        final = new_price
    elif new_price is None:
        final = used_price
    else:
        final = min(used_price, new_price)

    return int(math.floor(final / 10.0)) * 10


# # Step 2: Fetch Inventory
def fetch_inventory(next_token=None):
    access_token = get_access_token()  # Getting fresh access token
    headers = {
        "x-amz-access-token": access_token,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    url = f"{SP_API_BASE_URL}/listings/2021-08-01/items/{SELLER_ID}?marketplaceIds={MARKETPLACE_ID}&limit=10"  # Limit to 10 records per call
    if next_token:
        next_token_encoded = urllib.parse.quote(next_token)  # URL-encode the nextToken
        url += f"&nextToken={next_token_encoded}"

    # print(f"Fetching inventory from URL: {url}")
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        raise Exception(f"Failed to fetch inventory: {response.text}")




# Step 3: Main Logic to Fetch and Print Inventory
def get_all_inventory():
    try:
        next_token = None
        item_count = 0
        seen_hashes = set()
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        output_file = f"japan_inventory_offers_{timestamp}.csv"
        # output_file = "japan_inventory.csv"

        # Write header once at start
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

        while True:
            inventory_data = fetch_inventory(next_token)
            items = inventory_data.get("items", [])
            
            # Make a hash of all SKUs on this page to detect duplicate pages
            sku_list = [item.get("sku", "N/A") for item in items]
            page_hash = hashlib.md5("".join(sku_list).encode()).hexdigest()

            if page_hash in seen_hashes:
                print("⚠️ Repeated page detected. Breaking loop to avoid infinite pagination.")
                break
            else:
                seen_hashes.add(page_hash)

            for item in items:
                item_count += 1
                sku = item.get("sku", "N/A")
                asin = "N/A"
                condition = "N/A"
                title = "N/A"

                summaries = item.get("summaries", [])
                if summaries:
                    summary = summaries[0]
                    asin = summary.get("asin", "N/A")
                    condition = summary.get("conditionType", "N/A")
                    title = summary.get("itemName", "N/A")
                    if "used" in condition.lower():
                        lowest_price = get_lowest_price(
                            asin
                        )  # Fetch lowest price for the ASIN
                        if lowest_price is not None:
                            min_price = (
                                lowest_price - 100
                            )  # Example logic to set min price
                            max_price = (
                                lowest_price + 100
                            )  # Example logic to set min price
                            print(
                                f"Lowest price for ASIN {asin}: New Price {lowest_price} : Min Price {min_price} : Max Price {max_price}"
                            )
                            row = [
                                sku,
                                lowest_price,  # price
                                2,
                                "JPY",  # currency
                                "",  # sale-price
                                "",  # sale-from-date
                                "",  # sale-through-date
                                "",  # restock-date
                                min_price,
                                max_price,
                                "",  # or "AFN" if FBA
                                10,  # handling-time
                            ]
                            # Append row to CSV
                            with open(
                                output_file, "a", newline="", encoding="utf-8"
                            ) as csvfile:
                                writer = csv.writer(csvfile)
                                writer.writerow(row)
                        else:
                            print(f"No valid offers found for ASIN {asin}")
                    else:
                        print(
                            f"Condition is not used for ASIN {asin} :  condition : {condition}. Skipping..."
                        )

            # Check for nextToken to handle pagination
            next_token = inventory_data.get("pagination", {}).get("nextToken")

            if not next_token:
                print("No more pages available.")
                break
            else:
                print(f"Fetching next page with nextToken: {next_token}")

            time.sleep(1)  # Avoid rate limits

        print("Inventory fetching complete.")

    except Exception as e:
        print(f"Error: {str(e)}")


# Run the script
if __name__ == "__main__":
    get_all_inventory()
    # Uncomment the line below to fetch inventory for a specific seller
    # get_inventory_for_seller(SELLER_ID)
