import requests
import json
import time
import csv
import gzip
import io
import os
from urllib.parse import urlencode
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


# Get Access Token
def get_access_token():
    url = "https://api.amazon.com/auth/o2/token"
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": REFRESH_TOKEN,
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    response = requests.post(url, data=urlencode(payload), headers=headers)
    
    if response.status_code != 200:
        raise Exception(f"Failed to get access token: {response.status_code} - {response.text}")
    token = response.json().get("access_token")
    if not token:
        raise Exception("Access token is empty.")
    print("Access token obtained.")
    return token

# Create Report
def create_report(access_token):
    url = f"{SP_API_BASE_URL}/reports/2021-06-30/reports"
    payload = {
        "reportType": "GET_MERCHANT_LISTINGS_ALL_DATA",
        "marketplaceIds": [MARKETPLACE_ID]
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
    with open(f"report_document_metadata_{time.strftime('%Y%m%d_%H%M%S')}.json", "w", encoding="utf-8") as f:
        json.dump(document_info, f, indent=2)
    print(f"Report document metadata saved: {json.dumps(document_info, indent=2)}")
    return document_info.get("url"), document_info.get("compressionAlgorithm")

# Clean and Escape CSV Data
def clean_csv_field(field):
    if field is None:
        return ""
    field = str(field).replace('"', '""').replace('\n', ' ').replace('\r', ' ')
    return f'"{field}"'

# Download and Save Report to CSV
def download_and_save_report(report_url, compression_algorithm, chunk_size=5000):
    # Download report
    response = requests.get(report_url)
    if response.status_code != 200:
        raise Exception(f"Failed to download report: {response.status_code} - {response.text}")
    
    # Save raw response for debugging
    raw_file = f"raw_response_{time.strftime('%Y%m%d_%H%M%S')}.bin"
    with open(raw_file, "wb") as f:
        f.write(response.content)
    print(f"Raw response saved to {raw_file} (Size: {os.path.getsize(raw_file) / (1024 * 1024):.2f} MB)")

    # Handle compression and encoding
    encodings = ["utf-8", "shift-jis", "iso-8859-1"]  # Try multiple encodings
    lines = None
    if compression_algorithm == "GZIP":
        print("Decompressing GZIP data...")
        try:
            decompressed_data = gzip.decompress(response.content)
            # Save decompressed data for debugging
            decompressed_file = f"decompressed_data_{time.strftime('%Y%m%d_%H%M%S')}.txt"
            with open(decompressed_file, "wb") as f:
                f.write(decompressed_data)
            print(f"Decompressed data saved to {decompressed_file} (Size: {os.path.getsize(decompressed_file) / (1024 * 1024):.2f} MB)")

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
    
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    item_count = 0
    file_count = 1
    chunk = []
    
    headers = lines[0].split("\t")
    print(f"Report headers: {headers}")
    
    for line in lines[1:]:
        item_count += 1
        fields = line.split("\t")
        # Ensure fields match header length
        while len(fields) < len(headers):
            fields.append("")
        chunk.append([clean_csv_field(field) for field in fields])
        
        # Write chunk to file every chunk_size items
        if len(chunk) >= chunk_size or item_count == len(lines) - 1:
            output_file = f"japan_inventory_offers_{timestamp}_part{file_count}.csv"
            with open(output_file, mode="w", newline="", encoding="utf-8-sig") as f:
                writer = csv.writer(f)
                writer.writerow(headers)  # Write headers
                writer.writerows(chunk)
            
            file_size = os.path.getsize(output_file) / (1024 * 1024)  # Size in MB
            print(f"Saved {len(chunk)} items to {output_file} (Size: {file_size:.2f} MB)")
            chunk = []
            file_count += 1
    
    print(f"Total items saved: {item_count}")
    print(f"Inventory saved in {file_count-1} file(s).")

# Main
def main():
    try:
        print("Fetching access token...")
        access_token = get_access_token()
        print("Creating report...")
        report_id = create_report(access_token)
        print(f"Report created with ID: {report_id}")
        print("Checking report status...")
        report_document_id = check_report_status(access_token, report_id)
        print(f"Report ready with document ID: {report_document_id}")
        print("Fetching report URL and metadata...")
        report_url, compression_algorithm = get_report_document(access_token, report_document_id)
        print(f"Report URL: {report_url}, Compression: {compression_algorithm}")
        print("Downloading and saving inventory...")
        download_and_save_report(report_url, compression_algorithm, chunk_size=5000)
    except Exception as e:
        print(f"Error: {str(e)}")
        print("Check CLIENT_SECRET, REFRESH_TOKEN, and IAM role permissions.")

if __name__ == "__main__":
    main()