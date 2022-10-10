from unittest.mock import Mock

from models.example.random_customer_value import model
from unit_test_snowpark_session import unit_test_session_fixture  # noqa: F401


def test_random_customer_value(unit_test_session):
    input_df = unit_test_session.create_dataframe(
        [[1, "a"], [2, "b"], [3, "c"], [1_000_000, "d"]],
        schema=["C_CUSTKEY", "C_NAME"],
    )

    mock_dbt = Mock()
    mock_dbt.source = Mock(return_value=input_df)
    result_df = model(mock_dbt, unit_test_session)

    assert result_df.count() == input_df.count()

    result_rows = result_df.collect()
    for result_row in result_rows:
        variation = result_row["C_CUSTKEY"] - result_row["RANDOM_CUSTOMER_VALUE"]
        assert (
            -7 < variation < 7
        )  # anything beyond 7 standard deviations would be extremely unlikely from numpy.random.normal()
