import re
from mta_constants import keywords

_NEEDS_PLURAL_BOUNDARY = {'ice', 'icing', 'icy', 'rain', 'wind'}

# Named storm types get spelled/punctuated inconsistently ("nor'easter", "noreaster",
# "nor-easter", "northeaster") - normalize all of them to one label and check first,
# since it's a specific, high-confidence cause name in its own right.
_NAMED_STORMS = {
    "nor'easter": re.compile(r"nor[-'\s]?easters?|northeasters?", re.IGNORECASE),
}


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

    for label, storm_pattern in _NAMED_STORMS.items():
        if storm_pattern.search(combined):
            return label

    text = combined.lower()
    for kw in keywords:
        if kw in _NEEDS_PLURAL_BOUNDARY:
            pattern = r'\b' + re.escape(kw) + r's?\b'
        elif kw == 'flood':
            # "Flood protection" is MTA's name for a long-running Coney Island Yard
            # construction project ("What's happening? Flood protection"), not a
            # real-time flooding event - don't let that phrase count as a match.
            pattern = r'flood(?!\s+protection)'
        else:
            pattern = re.escape(kw)
        if re.search(pattern, text):
            return kw
    return None
