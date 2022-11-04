# dbt-python-model-testing-exploration
Exploration of how to test DBT python models effectively

## Method

The tests executed via pytest are not true "unit" tests, but they do serve to demonstrate that a given model's logic executes exactly the way we expect...even if the transform itself is happening on Snowflake.

## Requirements

We must have access to a live Snowflake installation.  For CI in GitHub Actions, we configure this via

```
Settings -> (Security) Secrets -> Actions -> Repository Secrets
```

We then set the `SNOWFLAKE_CONN_STR` variable with content similar to:
```
{"account":"accountid.us-east-1","user":"transform_user","password":"password_here","warehouse":"warehouse_id","database":"database_id","schema":"schema_id","role":"role_id"}
```

Since this repo has UDFs being created via macro in on-run-start, the unit tests rely on those UDFs already existing.
We expect them to be in the schema pointed to by the `SNOWFLAKE_TARGET_SCHEMA` variable, which should have content similar to:

```
database_id.schema_id
```

For local testing, a `pytest.ini` file can be added with content similar to:

```
[pytest]
env =
    SNOWFLAKE_CONN_STR={{"account":"accountid.us-east-1","user":"transform_user","password":"password_here","warehouse":"warehouse_id","database":"database_id","schema":"schema_id","role":"role_id"}}
    SNOWFLAKE_TARGET_SCHEMA=database_id.schema_id
```