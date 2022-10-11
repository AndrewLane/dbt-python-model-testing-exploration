from unittest.mock import Mock

from models.example.customer_orders import model
from unit_test_snowpark_session import unit_test_session_fixture  # noqa: F401


def test_customer_orders(unit_test_session):
    def mock_source(_, table_name):
        if table_name == "customer":
            return unit_test_session.create_dataframe(
                [
                    [1, "Customer 1", "123 Anywhere St", 123.45],
                    [2, "Customer 2", "456 Nowhere St", 0.0],
                    [3, "Customer 3 with no orders", "789 Everywhere Dr", 100.00],
                ],
                schema=["C_CUSTKEY", "C_NAME", "C_ADDRESS", "C_ACCTBAL"],
            )
        if table_name == "orders":
            return unit_test_session.create_dataframe(
                [
                    [1, 1, "P", 100.00, "1981-10-31"],
                    [2, 2, "F", 1.00, "1981-10-31"],
                    [3, 2, "O", 10.00, "1981-10-31"],
                ],
                schema=[
                    "O_ORDERKEY",
                    "O_CUSTKEY",
                    "O_ORDERSTATUS",
                    "O_TOTALPRICE",
                    "O_ORDERDATE",
                ],
            )

        raise ValueError(f"Unknown table_name {table_name}")

    mock_dbt = Mock()
    mock_dbt.source = mock_source
    result_df = model(mock_dbt, unit_test_session)

    expected_df = unit_test_session.create_dataframe(
        [
            [1, "Customer 1", 123.45, 1, "P", 100.00, 107.50],
            [2, "Customer 2", 0.0, 2, "F", 1.00, 1.08],
            [2, "Customer 2", 0.0, 3, "O", 10.00, 10.75],
            [3, "Customer 3 with no orders", 100.00, None, None, None, None],
        ],
        schema=[
            "C_CUSTKEY",
            "C_NAME",
            "C_ACCTBAL",
            "O_ORDERKEY",
            "O_ORDERSTATUS",
            "O_TOTALPRICE",
            "O_TOTALPRICE_WITH_TAX",
        ],
    )
    assert result_df.collect() == expected_df.collect()
