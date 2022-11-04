import json
import os

import pytest
from snowflake.snowpark import Session


@pytest.fixture(name="unit_test_session", scope="session")
def unit_test_session_fixture():
    env_var = "SNOWFLAKE_CONN_STR"  # See README for info on configuring this
    unit_test_snowflake_connection_config = os.getenv(env_var)
    assert (
        unit_test_snowflake_connection_config is not None
        and len(unit_test_snowflake_connection_config) > 0
    ), f"Expecting {env_var} env var to be set"
    connection_config = json.loads(unit_test_snowflake_connection_config)
    session = Session.builder.configs(connection_config).create()
    session.add_packages("numpy")
    return session


def get_target_schema():
    # gets the schema where all the functions are found
    env_var = "SNOWFLAKE_TARGET_SCHEMA"  # See README for info on configuring this
    target_schema = os.getenv(env_var)
    assert (
        target_schema is not None and len(target_schema) > 0
    ), f"Expecting {target_schema} env var to be set"
    return target_schema
