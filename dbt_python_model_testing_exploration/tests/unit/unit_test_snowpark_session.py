import json
import os

import pytest
from snowflake.snowpark import Session


@pytest.fixture(name="unit_test_session", scope="session")
def unit_test_session_fixture():
    env_var = "SNOWFLAKE_CONN_STR"
    unit_test_snowflake_connection_config = os.getenv(env_var)
    assert (
        unit_test_snowflake_connection_config is not None
    ), f"Expecting {env_var} env var to be set"
    connection_config = json.loads(unit_test_snowflake_connection_config)
    return Session.builder.configs(connection_config).create()
