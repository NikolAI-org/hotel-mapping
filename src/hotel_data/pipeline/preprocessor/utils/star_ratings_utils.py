def star_rating_score(r1, r2):
    """
    Bucket-based star rating similarity.
    Uses absolute difference on integer star ratings.

    STAR_SCORE[i] = score when |r1 - r2| == i
    """

    # Return 0 if missing value
    if r1 is None or r2 is None:
        return 0.0

    # Force them to integers if providers supply "4.0" etc.
    try:
        r1 = int(float(r1))
        r2 = int(float(r2))
    except:
        return 0.0

    diff = abs(r1 - r2)

    STAR_SCORE = [1.0, 0.8, 0.5, 0.2, 0.0]  # You can tune this anytime

    # If difference is larger than supported array range
    if diff >= len(STAR_SCORE):
        return 0.0

    return STAR_SCORE[diff]
