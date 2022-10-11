from snowflake.snowpark.functions import round as snowparkround


def include_sales_tax(amount):
    # assumes 7.5% tax
    return snowparkround(amount * 1.075, 2)


def model(dbt, session):
    dbt.config(materialized="table")

    customer_df = dbt.source("snowflakesampledata", "customer")
    orders_df = dbt.source("snowflakesampledata", "orders")

    joined_df = customer_df.join(
        orders_df,
        customer_df.col("c_custkey") == orders_df.col("o_custkey"),
        join_type="left",
    )
    joined_df = joined_df.withColumn(
        "o_totalprice_with_tax", include_sales_tax(joined_df.col("o_totalprice"))
    )
    return joined_df.select(
        [
            "c_custkey",
            "c_name",
            "c_acctbal",
            "o_orderkey",
            "o_orderstatus",
            "o_totalprice",
            "o_totalprice_with_tax",
        ]
    )
