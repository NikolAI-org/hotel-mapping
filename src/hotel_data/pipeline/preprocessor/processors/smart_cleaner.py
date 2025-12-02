from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import re

# Words that indicate the core entity type. We should STOP stripping if we hit these.
STRUCTURE_WORDS = {
    'hotel', 'inn', 'resort', 'motel', 'suites', 'suite', 'apartments', 'villas',
    'lodge', 'hostel', 'residency', 'palace', 'plaza', 'square', 'grand', 'royal',
    'stay', 'house', 'home', 'club', 'cottage', 'camp'
}


def smart_suffix_remover(name, address_line):
    if not name or not address_line:
        return name

    # Normalize inputs for comparison
    name_tokens = name.lower().strip().split()
    # Create a set of address tokens for fast lookup (remove punctuation)
    address_tokens = set(re.findall(r'\w+', address_line.lower()))

    # Iterate from the END of the name backwards
    # We use an index to know where to slice the name
    cut_off_index = len(name_tokens)

    for i in range(len(name_tokens) - 1, -1, -1):
        token = name_tokens[i]

        # SAFETY 1: Never strip the very first word (e.g. "The", "Grand")
        if i == 0:
            break

        # SAFETY 2: Stop if we hit a Structure Word (e.g. "Hotel")
        # This protects "Orchid Hotel" from becoming "Orchid" even if "Hotel" is in address
        if token in STRUCTURE_WORDS:
            break

        # ACTION: If token is in address, we mark it for removal
        if token in address_tokens:
            cut_off_index = i
        else:
            # If we find a word NOT in the address, we usually stop
            # (e.g. "The Orchid Vile Parle" -> Vile/Parle match, Orchid doesn't -> Stop)
            break

    return " ".join(name_tokens[:cut_off_index])


# Register UDF
smart_suffix_udf = udf(smart_suffix_remover, StringType())

# --- Usage Example in your Pipeline ---
# Use this AFTER your current cleaning steps
# df = df.withColumn(
#     "normalized_name",
#     smart_suffix_udf(col("normalized_name"), col("contact_address_line1"))
# )