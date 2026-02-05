# JSON to Parquet Mapping DAG

## Overview
This DAG reads raw JSON hotel data from MinIO based on country and supplier parameters, applies schema mapping, and writes to Parquet format for efficient querying.

## DAG Name
- **Production**: `map_raw_json_country_and_supplier`
- **Test**: `test_map_raw_json_country_and_supplier`

## Parameters

The DAG accepts two parameters from the Airflow UI:

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `country` | string | `india` | Country name (e.g., india, usa, uk) |
| `supplier_name` | string | `expedia` (prod) / `hobse` (test) | Supplier name (e.g., expedia, hobse, booking) |

## Data Flow

### Input
- **Path Pattern**: `s3a://data-lake/raw_input/{country}/{supplier_name}/*.json`
- **Format**: Nested JSON files with hotel data
- **Example**: `s3a://data-lake/raw_input/india/hobse/hobse_mumbai.json`

### Output
- **Path Pattern**: `s3a://data-lake/mapped_input/{country}/{supplier_name}/`
- **Format**: Parquet files (partitioned by country and supplier)
- **Example**: `s3a://data-lake/mapped_input/india/hobse/`

## JSON Schema

The DAG expects JSON files with the following structure:

```json
{
  "hotels": [
    {
      "id": "39624369",
      "name": "Sofitel Mumbai BKC",
      "relevanceScore": "0",
      "providerId": "Hobse",
      "providerHotelId": "778a630e4021a782",
      "providerName": "Hobse",
      "language": "en-US",
      "geoCode": {
        "lat": "19.05281",
        "long": "72.85478"
      },
      "contact": {
        "address": {
          "line1": "C 57, G Block BKC",
          "city": {"name": "Mumbai"},
          "state": {"name": "Maharashtra", "code": "MH"},
          "country": {"code": "IN", "name": "India"},
          "postalCode": "400051"
        },
        "phones": ["9987711842"],
        "fax": [],
        "emails": ["divya.bhagat@accor.com"]
      },
      "type": "Unknown",
      "category": "Unknown",
      "starRating": "5",
      "distance": "0",
      "attributes": [],
      "imageCount": "0",
      "availableSuppliers": ["Hobse"],
      "website": "www.example.com"
    }
  ],
  "curatedHotels": []
}
```

## Mapped Parquet Schema

The output Parquet files contain the following flattened schema:

| Column | Type | Description |
|--------|------|-------------|
| `hotel_id` | string | Unique hotel identifier |
| `hotel_name` | string | Hotel name |
| `relevance_score` | string | Relevance score from source |
| `provider_id` | string | Provider identifier |
| `provider_hotel_id` | string | Provider's hotel ID |
| `provider_name` | string | Provider name |
| `latitude` | double | Hotel latitude |
| `longitude` | double | Hotel longitude |
| `address_line1` | string | Address line 1 |
| `address_line2` | string | Address line 2 |
| `city` | string | City name |
| `state` | string | State name |
| `state_code` | string | State code |
| `country_code` | string | Country code (e.g., IN) |
| `country_name` | string | Country name |
| `postal_code` | string | Postal/ZIP code |
| `phone_numbers` | string | Comma-separated phone numbers |
| `fax_numbers` | string | Comma-separated fax numbers |
| `email_addresses` | string | Comma-separated email addresses |
| `hotel_type` | string | Hotel type |
| `hotel_category` | string | Hotel category |
| `star_rating` | double | Star rating (0-5) |
| `distance` | double | Distance metric |
| `image_count` | int | Number of images |
| `website` | string | Hotel website URL |
| `attributes` | array<string> | Hotel attributes |
| `available_suppliers` | array<string> | Available suppliers |
| `language` | string | Language code |
| `source_country` | string | Source country parameter |
| `source_supplier` | string | Source supplier parameter |
| `processed_timestamp` | timestamp | Processing timestamp |

## Tasks

1. **map_json_to_parquet**: Reads JSON files, applies schema mapping, and writes Parquet files
2. **log_completion**: Logs completion details and statistics

## Running the DAG

### Using Airflow UI

1. Navigate to the Airflow UI
2. Find the DAG: `map_raw_json_country_and_supplier`
3. Click the "Play" button to trigger with parameters
4. Enter parameters:
   - `country`: e.g., `india`
   - `supplier_name`: e.g., `hobse`
5. Click "Trigger"

### Testing with Sample Data

1. **Upload sample data to MinIO**:
   ```bash
   # From project root
   python scripts/upload_sample_json.py
   ```

2. **Trigger the test DAG**:
   - Use DAG: `test_map_raw_json_country_and_supplier`
   - Parameters: `country=india`, `supplier_name=hobse`

## Files

### DAG Files
- **Production DAG**: `dags/map_raw_json_country_and_supplier.py`
- **Test DAG**: `dags/test/test_map_raw_json_country_and_supplier.py`

### Spark Jobs
- **Production Job**: `spark/jobs/map_json_to_parquet.py`
- **Test Job**: `spark/test/jobs/test_map_json_to_parquet.py`

### Helper Scripts
- **Upload Script**: `scripts/upload_sample_json.py`

## Output Statistics

After successful execution, the DAG logs:
- Total hotels mapped
- Hotels by city
- Hotels by star rating
- Hotels by type
- Output location

## Error Handling

The Spark job includes comprehensive error handling:
- Schema validation
- File existence checks
- Data quality checks
- Detailed error messages with stack traces

## Performance

- **Partitioning**: Output is partitioned by `source_country` and `source_supplier` for efficient querying
- **Format**: Parquet format provides columnar storage and compression
- **Resource Allocation**: 
  - Executor Memory: 1GB
  - Driver Memory: 1GB
  - Executor Cores: 1

## Dependencies

- Apache Spark 3.x
- PySpark
- MinIO (S3-compatible storage)
- Airflow 2.x

## Next Steps

After mapping to Parquet:
1. Data can be queried efficiently using Spark SQL
2. Further transformations can be applied
3. Delta Lake format can be used for ACID transactions
4. Data can be used for analytics and reporting
