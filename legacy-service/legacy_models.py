"""Order record used by the legacy fulfilment sync.

This module is mirrored verbatim in legacy-service and processor-service. The
sync payloads go on the wire as pickles, so both sides have to resolve
``legacy_models.LegacyOrder`` to the same dotted name. Keep the two copies in
step and do not rename the module or the class (SHOP-2984).
"""


class LegacyOrder:
    def __init__(self, order_id, items, total):
        self.order_id = order_id
        self.items = items
        self.total = total

    def item_count(self):
        return len(self.items)

    def __repr__(self):
        return "LegacyOrder(order_id=%r, items=%d, total=%r)" % (
            self.order_id,
            len(self.items),
            self.total,
        )
