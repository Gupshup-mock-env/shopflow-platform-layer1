"""Stream records read off the raw clickstream."""

import faust


class ClickEvent(faust.Record, serializer="json"):
    """A single storefront interaction recorded by the web tier."""

    user_id: str
    page: str
    action: str
    timestamp: float
