import sys
import types
import unittest

if "pyspark" not in sys.modules:
    sys.modules["pyspark"] = types.ModuleType("pyspark")

if "pyspark.sql" not in sys.modules:
    sql_mod = types.ModuleType("pyspark.sql")

    class _DataFrame:
        pass

    class _Column:
        pass

    sql_mod.DataFrame = _DataFrame
    sql_mod.Column = _Column
    sys.modules["pyspark.sql"] = sql_mod

if "pyspark.sql.functions" not in sys.modules:
    functions_mod = types.ModuleType("pyspark.sql.functions")

    def _identity(value, *_args, **_kwargs):
        return value

    functions_mod.col = _identity
    functions_mod.trim = _identity
    functions_mod.regexp_replace = _identity
    functions_mod.udf = lambda fn, *_args, **_kwargs: fn
    sys.modules["pyspark.sql.functions"] = functions_mod

if "pyspark.sql.types" not in sys.modules:
    types_mod = types.ModuleType("pyspark.sql.types")

    class _StringType:
        pass

    types_mod.StringType = _StringType
    sys.modules["pyspark.sql.types"] = types_mod


from hotel_data.pipeline.preprocessor.processors.name_formatter_processor import (
    _strip_trailing_landmark_clause,
)


class TestStripTrailingLandmarkClause(unittest.TestCase):
    def test_strips_separator_introduced_landmark_clause(self):
        text = "carlton hotel mumbai - behind taj mahal palace colaba mumbai"
        self.assertEqual(_strip_trailing_landmark_clause(text), "carlton hotel mumbai")

    def test_strips_comma_introduced_near_clause(self):
        text = "hotel bkc crown, near trade centre visa consulate"
        self.assertEqual(_strip_trailing_landmark_clause(text), "hotel bkc crown")

    def test_strips_abbreviated_opp_clause(self):
        text = "city lodge mumbai / opp cst station"
        self.assertEqual(_strip_trailing_landmark_clause(text), "city lodge mumbai")

    def test_strips_abbreviated_nr_clause(self):
        text = "grand stay colaba : nr gateway of india"
        self.assertEqual(_strip_trailing_landmark_clause(text), "grand stay colaba")

    def test_keeps_core_name_when_near_is_part_of_name(self):
        text = "hotel near riverfront mumbai"
        self.assertEqual(_strip_trailing_landmark_clause(text), text)

    def test_keeps_directional_text_without_separator(self):
        text = "the opposite house mumbai"
        self.assertEqual(_strip_trailing_landmark_clause(text), text)


if __name__ == "__main__":
    unittest.main()
