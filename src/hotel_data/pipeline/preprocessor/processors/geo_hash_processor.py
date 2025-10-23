from pyspark.sql import functions as F
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf, struct
from pyspark.sql.types import ArrayType, StringType, DoubleType,StructType
import h3
from typing import List

from hotel_data.pipeline.preprocessor.core.base_processor import BaseProcessor

# Assuming BaseProcessor and hotel_data are available
# from hotel_data.pipeline.preprocessor.core.base_processor import BaseProcessor 


# --- Configuration ---
H3_RESOLUTION = 8
K_RING_DISTANCE = 1 # k=1 is the central cell + 6 immediate neighbors


class GeoHashProcessor(BaseProcessor):

    def __init__(self, resolution: int = H3_RESOLUTION, k_distance: int = K_RING_DISTANCE):
        self.resolution = resolution
        self.k_distance = k_distance
        
        # Register the H3 logic as a PySpark UDF
        self.geohash_udf = F.udf(
            self._h3_k_ring_geohashes, 
            ArrayType(StringType())
        )

    # ----------------------------------------------------
    # PySpark Processing Method
    # ----------------------------------------------------
    

    def process(self, df: DataFrame) -> DataFrame:
        """
        Applies the H3 geohash UDF and writes the result directly to the 
        top-level 'geohash' column in the flat schema.
        All other columns are preserved automatically.
        """
        print(f"Starting H3 GeoHash calculation at Resolution={self.resolution} with K-ring={self.k_distance}...")
        
        # Ensure the input columns are cast to the correct type for the UDF
        # UDF inputs must be numeric types (Double/Float)
        lat_col = col('geoCode_lat').cast('double')
        long_col = col('geoCode_long').cast('double')
        res_col = F.lit(self.resolution)
        k_col = F.lit(self.k_distance)

        # Calculate the hexagonal geoHashes array and write it directly 
        # to the 'geohash' column. This will overwrite any existing data 
        # in that column, which is what you intended.
        final_df = df.withColumn(
            'geohash', # Target column name
            self.geohash_udf(
                lat_col, 
                long_col, 
                res_col, 
                k_col
            )
        )
        
        print("GeoHash mapping complete. Data written to 'geohash' column.")
        return final_df

    # ----------------------------------------------------
    # H3 Logic (Static Helper for UDF)
    # ----------------------------------------------------
    @staticmethod
    def _h3_k_ring_geohashes(lat: float, lon: float, resolution: int, k_distance: int) -> List[str]:
        """
        Calculates the central H3 cell and its neighbors within k_distance.
        This function runs inside the Spark executor, not the driver.
        """
        # Note: Spark UDFs handle nulls in a standard way, so pd.isna check is optional
        if lat is None or lon is None:
            return []

        # 1. Get the central H3 cell index (the 'geohash')
        center_h3 = h3.latlng_to_cell(lat, lon, resolution)

        # 2. Get the cell itself and its neighbors (k-ring)
        neighbor_cells = h3.grid_disk(center_h3, k=k_distance)
        
        # Convert the set of cells to a list of hexadecimal strings
        return list(neighbor_cells)

    