from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
import re
from hotel_data.config.scoring_config import ScoringConstants

# Words that indicate the core entity type. We should STOP stripping if we hit these.
#STRUCTURE_WORDS = {
#    'hotel', 'inn', 'resort', 'motel', 'suites', 'suite', 'apartments', 'villas',
#    'lodge', 'hostel', 'residency', 'palace', 'plaza', 'square', 'grand', 'royal',
#    'stay', 'house', 'home', 'club', 'cottage', 'camp'
#}

def normalize_real_estate_terms(text: str) -> str:
    """
    Injects spaces into squashed real-estate terminology to assist downstream tokenization.
    """
    if not text: 
        return text
    
    text = text.lower()
    
    # FIX FOR CASE 2: "2bhk" -> "2 bhk"
    # Safely splits digits from specific real estate words.
    text = re.sub(r'(\d+)(bhk|bed|bedroom|bath)\b', r'\1 \2', text)
    
    # FIX FOR CASE 3: "bedroomiibath" -> "bedroom ii bath"
    # Looks for a prefix term, a roman numeral (I, II, III, IV, V), and an optional suffix term.
    text = re.sub(r'(bedroom|bed|bath|bhk)(i{1,3}|iv|v)(bath|bedroom|bhk)?\b', r'\1 \2 \3', text)
    
    # Clean up any accidental double spaces we just created
    return re.sub(r'\s+', ' ', text).strip()

normalize_real_estate_udf = udf(normalize_real_estate_terms, StringType())

def smart_suffix_remover(name, address_line):
    if not name or not address_line:
        return name

    name_tokens = name.lower().strip().split()
    address_tokens = set(re.findall(r'\w+', address_line.lower()))

    cut_off_index = len(name_tokens)

    # Iterate from the END of the name backwards
    for i in range(len(name_tokens) - 1, -1, -1):
        token = name_tokens[i]

        # 1. If the word is NOT in the address, stop stripping immediately.
        # (e.g., if we hit "Hoxton" and it wasn't in the address, we keep it)
        if token not in address_tokens:
            break

        # 2. THE NEW GUARDRAIL: Look at what will be left if we delete this token.
        leftover_tokens = name_tokens[:i]
        
        # If the leftover string is empty OR consists ONLY of generic words (like "hotel"),
        # we MUST stop stripping to protect the core identity.
        is_only_generic_left = True
        for leftover in leftover_tokens:
            if leftover not in ScoringConstants.LOW_INFO_TERMS:
                is_only_generic_left = False
                break
                
        if not leftover_tokens or is_only_generic_left:
            break # Stop! Don't delete the core identifier!

        # 3. If it passed the guardrails, mark the index for deletion
        cut_off_index = i

    return " ".join(name_tokens[:cut_off_index])


# Register UDF
smart_suffix_udf = udf(smart_suffix_remover, StringType())

# --- Usage Example in your Pipeline ---
# Use this AFTER your current cleaning steps
# df = df.withColumn(
#     "normalized_name",
#     smart_suffix_udf(col("normalized_name"), col("contact_address_line1"))
# )