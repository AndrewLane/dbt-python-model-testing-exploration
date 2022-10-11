from unittest.mock import Mock

from models.example.customer_total_order_price import model
from unit_test_snowpark_session import unit_test_session_fixture  # noqa: F401


def test_customer_total_order_price(unit_test_session):
    input_df = unit_test_session.create_dataframe(
        [
            [1, "Customer With Many Orders", 1234.12, 1001, "O", 100.00],
            [1, "Customer With Many Orders", 4542.12, 1002, "F", 200.00],
            [2, "Customer With No Orders", 4999.99, None, None, None],
            [3, "Customer With One Order", 123.45, 1003, "O", 600.00],
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
    result_pandas_df = model(mock_dbt, unit_test_session)
    result_df = unit_test_session.create_dataframe(result_pandas_df)

    expected_df = unit_test_session.create_dataframe(
        [[1, 300.00, 322.50], [3, 600.00, 645.00]],
        schema=["c_custkey", "o_totalprice", "o_totalprice_with_tax"],
    )
    assert result_df.collect() == expected_df.collect()
