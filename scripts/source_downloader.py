import os
import requests
from typing import List
from concurrent.futures import ThreadPoolExecutor, as_completed


def download_single_file(url: str, output_location: str):
    parts = url.rstrip('/').split('/')
    # Use the second last and last parts of the URL for filename
    if len(parts) >= 2:
        filename = f"{parts[-2]}_{parts[-1]}"
    else:
        filename = parts[-1]
    if not filename.endswith('.json'):
        filename += '.json'
    output_path = os.path.join(output_location, filename)
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        with open(output_path, 'wb') as f:
            f.write(response.content)
        print(f"Downloaded: {url} -> {output_path}")
    except Exception as e:
        print(f"Failed to download {url}: {e}")


def download_json_files(urls: List[str], output_location: str, max_workers: int = 8):
    os.makedirs(output_location, exist_ok=True)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(
            download_single_file, url, output_location) for url in urls]
        for future in as_completed(futures):
            future.result()


if __name__ == "__main__":
    # Example usage
    urls = [
        # Add your JSON URLs here
        # "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/AE/1", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/AE/2", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/AE/3", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/IN/1", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/IN/2", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/IN/3", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/IN/4", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/IN/5", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/IN/6", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/IN/7", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/IN/8", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/IN/9", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/IN/10", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/IN/11", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/IN/12", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/IN/13", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/IN/14", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/US/1", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/US/2", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/US/3", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/US/4", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/US/5", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/US/6", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/US/7", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/US/8", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/US/9", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/US/10", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/US/11", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/US/12", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/US/13", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/US/14", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/US/15", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/US/16", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/US/17", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/US/18", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/US/19", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/US/20", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/US/21", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/US/22", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/US/23", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/US/24", "https://zh-content-data.s3.us-east-1.amazonaws.com/BookingCom/20251010/en-US/US/25"
        # "https://zh-content-data.s3.us-east-1.amazonaws.com/EAN/20251010/en-US/AE/1", "https://zh-content-data.s3.us-east-1.amazonaws.com/EAN/20251010/en-US/IN/1", "https://zh-content-data.s3.us-east-1.amazonaws.com/EAN/20251010/en-US/IN/2", "https://zh-content-data.s3.us-east-1.amazonaws.com/EAN/20251010/en-US/IN/3", "https://zh-content-data.s3.us-east-1.amazonaws.com/EAN/20251010/en-US/IN/4", "https://zh-content-data.s3.us-east-1.amazonaws.com/EAN/20251010/en-US/IN/5", "https://zh-content-data.s3.us-east-1.amazonaws.com/EAN/20251010/en-US/IN/6", "https://zh-content-data.s3.us-east-1.amazonaws.com/EAN/20251010/en-US/IN/7", "https://zh-content-data.s3.us-east-1.amazonaws.com/EAN/20251010/en-US/US/1", "https://zh-content-data.s3.us-east-1.amazonaws.com/EAN/20251010/en-US/US/2", "https://zh-content-data.s3.us-east-1.amazonaws.com/EAN/20251010/en-US/US/3", "https://zh-content-data.s3.us-east-1.amazonaws.com/EAN/20251010/en-US/US/4", "https://zh-content-data.s3.us-east-1.amazonaws.com/EAN/20251010/en-US/US/5", "https://zh-content-data.s3.us-east-1.amazonaws.com/EAN/20251010/en-US/US/6", "https://zh-content-data.s3.us-east-1.amazonaws.com/EAN/20251010/en-US/US/7", "https://zh-content-data.s3.us-east-1.amazonaws.com/EAN/20251010/en-US/US/8", "https://zh-content-data.s3.us-east-1.amazonaws.com/EAN/20251010/en-US/US/9", "https://zh-content-data.s3.us-east-1.amazonaws.com/EAN/20251010/en-US/US/10", "https://zh-content-data.s3.us-east-1.amazonaws.com/EAN/20251010/en-US/US/11", "https://zh-content-data.s3.us-east-1.amazonaws.com/EAN/20251010/en-US/US/12", "https://zh-content-data.s3.us-east-1.amazonaws.com/EAN/20251010/en-US/US/13", "https://zh-content-data.s3.us-east-1.amazonaws.com/EAN/20251010/en-US/US/14", "https://zh-content-data.s3.us-east-1.amazonaws.com/EAN/20251010/en-US/US/15", "https://zh-content-data.s3.us-east-1.amazonaws.com/EAN/20251010/en-US/US/16", "https://zh-content-data.s3.us-east-1.amazonaws.com/EAN/20251010/en-US/US/17", "https://zh-content-data.s3.us-east-1.amazonaws.com/EAN/20251010/en-US/US/18", "https://zh-content-data.s3.us-east-1.amazonaws.com/EAN/20251010/en-US/US/19", "https://zh-content-data.s3.us-east-1.amazonaws.com/EAN/20251010/en-US/US/20", "https://zh-content-data.s3.us-east-1.amazonaws.com/EAN/20251010/en-US/US/21", "https://zh-content-data.s3.us-east-1.amazonaws.com/EAN/20251010/en-US/US/22", "https://zh-content-data.s3.us-east-1.amazonaws.com/EAN/20251010/en-US/US/23", "https://zh-content-data.s3.us-east-1.amazonaws.com/EAN/20251010/en-US/US/24"
        "https://zh-content-data.s3.us-east-1.amazonaws.com/HotelBeds/20260305/en-US/US/1", "https://zh-content-data.s3.us-east-1.amazonaws.com/HotelBeds/20260305/en-US/US/2", "https://zh-content-data.s3.us-east-1.amazonaws.com/HotelBeds/20260305/en-US/US/3", "https://zh-content-data.s3.us-east-1.amazonaws.com/HotelBeds/20260305/en-US/US/4", "https://zh-content-data.s3.us-east-1.amazonaws.com/HotelBeds/20260305/en-US/US/5", "https://zh-content-data.s3.us-east-1.amazonaws.com/HotelBeds/20260305/en-US/US/6", "https://zh-content-data.s3.us-east-1.amazonaws.com/HotelBeds/20260305/en-US/US/7", "https://zh-content-data.s3.us-east-1.amazonaws.com/HotelBeds/20260305/en-US/US/8", "https://zh-content-data.s3.us-east-1.amazonaws.com/HotelBeds/20260305/en-US/US/9", "https://zh-content-data.s3.us-east-1.amazonaws.com/HotelBeds/20260305/en-US/US/10", "https://zh-content-data.s3.us-east-1.amazonaws.com/HotelBeds/20260305/en-US/IN/1", "https://zh-content-data.s3.us-east-1.amazonaws.com/HotelBeds/20260305/en-US/IN/2", "https://zh-content-data.s3.us-east-1.amazonaws.com/HotelBeds/20260305/en-US/AE/1"
    ]
    output_location = "/Users/nakul.patil/Documents/hotel-match/hotelbeds_input_data/"
    download_json_files(urls, output_location)
