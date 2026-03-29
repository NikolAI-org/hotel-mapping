from pyspark.sql import DataFrame
from typing import List

from hotel_data.pipeline.preprocessor.core.base_processor import BaseProcessor
from hotel_data.pipeline.preprocessor.processors.hotel_flattener_processor import HotelFlattenerProcessor
from hotel_data.pipeline.preprocessor.processors.mandatory_fields_processor import MandatoryFieldsFilterProcessor
from hotel_data.pipeline.preprocessor.processors.default_value_processor import DefaultValueProcessor
from hotel_data.pipeline.preprocessor.processors.address_combiner_processor import AddressCombinerProcessor
from hotel_data.pipeline.preprocessor.processors.name_formatter_processor import NameFormatterProcessor
from hotel_data.pipeline.preprocessor.processors.text_preprocessor_processor import TextPreprocessorProcessor
from hotel_data.pipeline.preprocessor.processors.geo_hash_processor import GeoHashProcessor
#from hotel_data.pipeline.preprocessor.processors.sbert_vectorizer import compute_all_embeddings
from hotel_data.pipeline.preprocessor.processors.timestamp_appender_processor import TimestampAppenderProcessor


class PreprocessingPipeline:
    """
    Orchestrates the Raw JSON -> Bronze Delta transformation.
    """

    def __init__(self):
        # 1. Flatten JSON
        self.flattener = HotelFlattenerProcessor(explode_arrays=True)

        # 2. Filter invalid rows (must have ID and Name)
        self.validator = MandatoryFieldsFilterProcessor(critical_fields=["id", "name"])

        # 3. Fill defaults for safety
        self.defaulter = DefaultValueProcessor(critical_fields=["id", "name"])

        # 4. Create Combined Address (for UID and Embedding)
        self.address_combiner = AddressCombinerProcessor(
            address_fields=["contact_address_line1", "contact_address_city_name", "contact_address_country_name"]
        )

        self.text_preprocessor = TextPreprocessorProcessor()

        # 5. Normalize Name (Remove stop words/address parts)
        self.name_normalizer = NameFormatterProcessor(
            address_fields=["contact_address_city_name", "contact_address_line1"],
            output_col="normalized_name"
        )

        # 6. Generate GeoHash (Blocking Key)
        self.geohasher = GeoHashProcessor(resolution=7)  # Resolution 7 is good for ~1-2km blocking

        # 7. Generate Embeddings (Heavy Compute)
        #self.vectorizer = SbertVectorizer()

        # 8. Add Processing Time
        self.timestamper = TimestampAppenderProcessor()

    def run(self, raw_df: DataFrame) -> DataFrame:
        """Execute the pipeline steps in order"""

        print(">> [1/8] Flattening JSON...")
        df = self.flattener.process(raw_df)

        print(">> [2/8] Validating Mandatory Fields...")
        df, invalid_df = self.validator.process(df)
        if invalid_df.count() > 0:
            print(f"   WARNING: Dropped {invalid_df.count()} invalid records.")

        print(">> [3/8] Applying Default Values...")
        df = self.defaulter.process(df)

        print(">> [4/8] Generating Combined Address...")
        df = self.address_combiner.process(df)

        print(">> [5/8] Normalizing Hotel Names...")
        df = self.name_normalizer.process(df)

        df = self.text_preprocessor.process(df)

        print(">> [6/8] Calculating GeoHashes...")
        df = self.geohasher.process(df)

        print(">> [7/8] Generating SBERT Embeddings (This may take time)...")
        # Ensure we repartition before vectorization to handle memory
        df = df.repartition(100)
        df = self.vectorizer.process(df)

        print(">> [8/8] Finalizing...")
        df = self.timestamper.process(df)

        return df