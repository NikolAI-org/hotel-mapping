class HotelClusteringException(Exception):
    """Base exception"""
    pass

class ScoringException(HotelClusteringException):
    """Scoring specific exception"""
    pass
