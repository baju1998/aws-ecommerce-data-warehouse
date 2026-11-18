import pandas as pd
import pytest

from src.validate import validate_data


def test_duplicate_order_ids():

    orders = pd.DataFrame({
        "order_id": ["1001", "1001"],
        "customer_id": ["C1", "C2"],
        "order_status": ["delivered", "delivered"],
        "order_purchase_timestamp": [
            "2026-01-01",
            "2026-01-02"
        ]
    })

    with pytest.raises(ValueError, match="duplicate primary keys"):
        validate_data({"orders": orders})