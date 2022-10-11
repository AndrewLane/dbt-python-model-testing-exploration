from models.example.customer_orders import include_sales_tax


def model(dbt, session):
    dbt.config(materialized="table")
    customer_orders_df = dbt.ref("customer_orders")
    pandas_df = (
        customer_orders_df.filter("O_TOTALPRICE is not null")
        .select(["C_CUSTKEY", "O_TOTALPRICE"])
        .withColumn(
            "O_TOTALPRICE_WITH_TAX",
            include_sales_tax(customer_orders_df.col("O_TOTALPRICE")),
        )
        .to_pandas()
    )
    # I don't know pandas enough to know why I have to do reset_index
    return pandas_df.groupby(["C_CUSTKEY"]).sum().reset_index()
