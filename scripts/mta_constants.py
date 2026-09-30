# Ordered most-specific-first: when an alert's description mentions more than one of
# these, we want the specific cause (e.g. "snow") over the generic fallback ("weather").
keywords = [
    'blizzard', 'snow', 'sleet', 'hail', 'heavy rain', 'hurricane',
    'storm surge', 'tidal flooding', 'high tide', 'storm', 'flood',
    'icing', 'icy', 'ice', 'rain', 'wind', 'fog', 'heat', 'weather',
]