# hotel_data/pipeline/preprocessor/utils/constants.py

# NEW: Ordered list of phrases to remove BEFORE tokenization
# Logic: Longer phrases must come first to prevent partial removal.
# e.g. Remove "by oyo rooms" first. If not found, try "oyo rooms", etc.
STOP_PHRASES = [
    "by oyo rooms",
    "oyo rooms",
    "oyo"
]


# Common stop words/terms often found in hotel names and addresses
# Kept in LOWERCASE to match tokenization logic
STOP_WORDS = {
    'hotel', 'inn', 'lodge', 'motel', 'resort', 'spa', 'hostal', 'guesthouse', 'casa', 'auberge',
    'apart-hotel', 'suites', 'executive suites', 'residences', 'b&b', 'bed and breakfast', 'hostel', 'at',
    'near', 'by', 'the', 'on', 'of', 'in', 'next to', 'close to', 'opposite', 'near to', 'located at',
    'located in', 'east', 'west', 'north', 'south', 'central', 'downtown', 'city', 'town', 'village',
    'metropolitan', 'airport', 'beach', 'waterfront', 'harbor', 'view', 'views', 'vista', 'garden',
    'gardens', 'park', 'plaza', 'square', 'terrace', 'court', 'quarters', 'grand', 'royal', 'king',
    'queen', 'palace', 'imperial', 'crown', 'premier', 'prestige', 'deluxe', 'luxury', 'superior',
    'exclusive', 'small', 'boutique', 'extended stay', 'budget', 'microtel', 'old', 'new', 'historic',
    'vintage', 'modern', 'a', 'an', 'and', 'for', 'with', 'to', 'or', 'via', 'from', 'is', 'just', 'plus',
    'el', 'la', 'los', 'las', 'del', 'de', 'y', 'le', 'les', 'du', 'des', 'et', 'chez', 'der', 'die',
    'das', 'und', 'il', 'i', 'gli', 'e', 'di'
}