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
    # Prepositions & Articles (TRASH)
    'at', 'by', 'the', 'on', 'of', 'in', 'to', 'from', 'for', 'with', 'and', 'or', 'via', 'is', 'just', 'plus',
    'a', 'an', 'as',
    'near', 'next to', 'close to', 'opposite', 'near to', 'located at', 'located in',

    # Directions (Usually Noise in loose matching, but debatable. Safer to remove)
    'east', 'west', 'north', 'south', 'central', 'downtown',

    # Foreign Articles
    'el', 'la', 'los', 'las', 'del', 'de', 'y', 'le', 'les', 'du', 'des', 'et', 'chez',
    'der', 'die', 'das', 'und', 'il', 'i', 'gli', 'e', 'di'
}