from unittest.mock import Mock

from models.example.high_value_customers_with_no_orders import model
from unit_test_snowpark_session import unit_test_session_fixture  # noqa: F401


def test_high_value_customers_with_no_orders(unit_test_session):
    input_df = unit_test_session.create_dataframe(
        [
            [1, "Customer With Many Orders", 1234.12, 1001, "O", 100.00],
            [1, "Customer With Many Orders", 4542.12, 1002, "F", 200.00],
            [2, "Low Value Customer With No Orders", 4999.99, None, None, None],
            [3, "Customer With No Orders At Threshold", 5000.00, None, None, None],
            [4, "Customer With No Orders Above Threshold", 5000.01, None, None, None],
            [5, "Customer With One Order", 123.45, 1003, "O", 300.00],
        ],
        schema=[
            "c_custkey",
            "c_name",
            "c_acctbal",
            "o_orderkey",
            "o_orderstatus",
            "o_totalprice",
        ],
    )

    mock_dbt = Mock()
    mock_dbt.ref = Mock(return_value=input_df)
    result_df = model(mock_dbt, unit_test_session)

    expected_df = unit_test_session.create_dataframe(
        [
            [3, "Customer With No Orders At Threshold", 5000.00],
            [4, "Customer With No Orders Above Threshold", 5000.01],
        ],
        schema=[
            "c_custkey",
            "c_name",
            "c_acctbal",
        ],
    )
    assert result_df.collect() == expected_df.collect()
