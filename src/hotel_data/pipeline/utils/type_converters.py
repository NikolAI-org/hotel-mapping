def to_float(value, default=0.0) -> float:
    if value in (None, "", "null"):
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

def to_int(value, default=0) -> int:
    if value in (None, "", "null"):
        return default
    try:
        return int(value)
    except (ValueError, TypeError):
        return default    
    
def to_bool(value, default=False) -> bool:
    if value in (None, "", "null"):
        return default
    try:
        return bool(value)
    except (ValueError, TypeError):
        return default    