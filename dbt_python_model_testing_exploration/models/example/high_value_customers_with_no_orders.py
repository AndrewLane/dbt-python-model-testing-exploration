def model(dbt, session):
    dbt.config(materialized="table")
    customer_orders_df = dbt.ref("customer_orders")
    return customer_orders_df.filter("o_orderkey is null and c_acctbal > 5000").select(
        ["c_custkey", "c_name", "c_acctbal"]
    )
