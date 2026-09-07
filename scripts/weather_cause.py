import re
from mta_constants import keywords

_NEEDS_PLURAL_BOUNDARY = {'ice', 'icing', 'icy', 'rain', 'wind'}


def extract_alert_reason(header_text, description_text):
    """Pick the most specific weather term mentioned in an alert, trying `keywords`
    in priority order (specific causes like "snow" before the generic "weather"
    fallback) rather than whichever appears first left-to-right.

    Searches both header_text and description_text: many alerts - especially the
    days-later "service is being restored" follow-ups after a storm - only name
    the weather cause in the header, leaving description_text purely operational."""
    combined = " ".join(t for t in (header_text, description_text) if isinstance(t, str))
    if not combined:
        return None
    text = combined.lower()
    for kw in keywords:
        if kw in _NEEDS_PLURAL_BOUNDARY:
            pattern = r'\b' + re.escape(kw) + r's?\b'
        else:
            pattern = re.escape(kw)
        if re.search(pattern, text):
            return kw
    return None
