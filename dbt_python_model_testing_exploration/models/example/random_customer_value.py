import numpy
from snowflake.snowpark.types import FloatType


def register_udf_add_random(session):
    add_random = session.udf.register(
        lambda x: x + numpy.random.normal(),
        return_type=FloatType(),
        input_types=[FloatType()],
    )
    return add_random


def model(dbt, session):
    dbt.config(materialized="table", packages=["numpy"])

    customer_df = dbt.source("snowflakesampledata", "customer")

    add_random_udf = register_udf_add_random(session)
    return customer_df.select(["c_custkey"]).withColumn(
        "random_customer_value", add_random_udf("c_custkey")
    )
