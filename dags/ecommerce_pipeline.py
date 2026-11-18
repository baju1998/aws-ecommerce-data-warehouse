from datetime import datetime
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException # lets us deliberately fail the pipeline when a data quality check fails.
from airflow.providers.amazon.aws.hooks.redshift_data import RedshiftDataHook # lets Python communicate with Redshift through the AWS Data API.
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
# ^ RedshiftDataOperator to execute SQL against your Redshift Serverless warehouse directly from Airflow

@dag(
    dag_id="ecommerce_pipeline",
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    tags=["ecommerce", "data-engineering"],
)
def ecommerce_pipeline():

    @task
    def start_pipeline():
        print("Starting AWS E-Commerce Data Pipeline")

    run_glue_etl = GlueJobOperator(
        task_id="run_glue_etl",
        job_name="ecommerce-etl-transform",
        aws_conn_id="aws_default",
        region_name="us-east-1",
        wait_for_completion=True,
        update_config=False,
    )

    load_customer_staging = RedshiftDataOperator(
        task_id="load_customer_staging",
        workgroup_name="ecommerce-workgroup",
        database="dev",
        aws_conn_id="aws_default",
        region="us-east-1",
        sql=[
            "DELETE FROM ecommerce_dw.stg_dim_customer",
            """
            COPY ecommerce_dw.stg_dim_customer
            FROM 's3://aws-ecommerce-data-warehouse/processed/dim_customer/'
            IAM_ROLE 'arn:aws:iam::218014903951:role/RedshiftEcommerceProcessedRead'
            FORMAT AS PARQUET
            """
        ],
        wait_for_completion=True,
    )
    staging_tables = [
        "dim_product",
        "dim_seller",
        "dim_date",
        "fact_sales",
    ]

    load_remaining_staging = RedshiftDataOperator(
        task_id="load_remaining_staging",
        workgroup_name="ecommerce-workgroup",
        database="dev",
        aws_conn_id="aws_default",
        region="us-east-1",
        sql=[
            statement
            for table in staging_tables
            for statement in [
                f"DELETE FROM ecommerce_dw.stg_{table}",
                f"""
                COPY ecommerce_dw.stg_{table}
                FROM 's3://aws-ecommerce-data-warehouse/processed/{table}/'
                IAM_ROLE 'arn:aws:iam::218014903951:role/RedshiftEcommerceProcessedRead'
                FORMAT AS PARQUET
                """,
            ]
        ],
        wait_for_completion=True,
    )

    @task
    def validate_staging_data():
        hook = RedshiftDataHook(
            aws_conn_id="aws_default",
            region_name="us-east-1",
        )

        expected_counts = {
            "dim_customer": 99441,
            "dim_product": 32951,
            "dim_seller": 3095,
            "dim_date": 634,
            "fact_sales": 112650,
        }

        for table, expected in expected_counts.items():
            sql = (
                f"SELECT COUNT(*) "
                f"FROM ecommerce_dw.stg_{table}"
            )

            statement_id = hook.execute_query(
                database="dev",
                sql=sql,
                workgroup_name="ecommerce-workgroup",
                wait_for_completion=True,
            )

            result = hook.conn.get_statement_result(
                Id=statement_id
            )

            actual = int(result["Records"][0][0]["longValue"])

            print(f"{table}: expected={expected}, actual={actual}")

            if actual != expected:
                raise AirflowException(
                    f"Data quality check failed for {table}: "
                    f"expected {expected}, got {actual}"
                )

        print("All staging row-count checks passed!")

        unique_keys = {
            "dim_customer": "customer_key",
            "dim_product": "product_key",
            "dim_seller": "seller_key",
            "dim_date": "date_key",
        }

        for table, key in unique_keys.items():
            sql = f"""
                SELECT COUNT(*) - COUNT(DISTINCT {key})
                FROM ecommerce_dw.stg_{table}
            """

            statement_id = hook.execute_query(
                database="dev",
                sql=sql,
                workgroup_name="ecommerce-workgroup",
                wait_for_completion=True,
            )

            result = hook.conn.get_statement_result(
                Id=statement_id
            )

            duplicates = int(result["Records"][0][0]["longValue"])

            print(f"{table}: duplicate keys={duplicates}")

            if duplicates > 0:
                raise AirflowException(
                    f"Duplicate keys detected in {table}: {duplicates}"
                )

        print("All dimension duplicate-key checks passed!")


        sql = """
            SELECT COUNT(*)
            FROM (
                SELECT order_id, order_item_id
                FROM ecommerce_dw.stg_fact_sales
                GROUP BY order_id, order_item_id
                HAVING COUNT(*) > 1
            ) AS duplicates
        """

        statement_id = hook.execute_query(
            database="dev",
            sql=sql,
            workgroup_name="ecommerce-workgroup",
            wait_for_completion=True,
        )

        result = hook.conn.get_statement_result(Id=statement_id)
        duplicate_groups = int(result["Records"][0][0]["longValue"])

        print(f"fact_sales: duplicate order-item keys={duplicate_groups}")

        if duplicate_groups > 0:
            raise AirflowException(
                f"Duplicate order-item keys detected: {duplicate_groups}"
            )

        print("Sales transaction uniqueness check passed!")

        relationships = {
            "customer": ("customer_key", "dim_customer"),
            "product": ("product_key", "dim_product"),
            "seller": ("seller_key", "dim_seller"),
            "date": ("date_key", "dim_date"),
        }

        for name, (key, dimension) in relationships.items():
            sql = f"""
                SELECT COUNT(*)
                FROM ecommerce_dw.stg_fact_sales f
                LEFT JOIN ecommerce_dw.stg_{dimension} d
                    ON f.{key} = d.{key}
                WHERE d.{key} IS NULL
            """

            statement_id = hook.execute_query(
                database="dev",
                sql=sql,
                workgroup_name="ecommerce-workgroup",
                wait_for_completion=True,
            )

            result = hook.conn.get_statement_result(
                Id=statement_id
            )

            orphan_count = int(
                result["Records"][0][0]["longValue"]
            )

            print(f"{name}: orphan records={orphan_count}")

            if orphan_count > 0:
                raise AirflowException(
                    f"Foreign-key validation failed: "
                    f"{name} has {orphan_count} orphan records"
                )

        print("All foreign-key checks passed!")

    refresh_tables = [
        "dim_customer",
        "dim_product",
        "dim_seller",
        "dim_date",
        "fact_sales",
    ]

    refresh_production = RedshiftDataOperator(
        task_id="refresh_production_tables",
        workgroup_name="ecommerce-workgroup",
        database="dev",
        aws_conn_id="aws_default",
        region="us-east-1",
        sql=[
            # Delete fact records before dimensions
            "DELETE FROM ecommerce_dw.fact_sales",
            *[
                f"DELETE FROM ecommerce_dw.{table}"
                for table in refresh_tables[:-1]
            ],

            # Insert dimensions before fact records
            *[
                f"""
                INSERT INTO ecommerce_dw.{table}
                SELECT * FROM ecommerce_dw.stg_{table}
                """
                for table in refresh_tables
            ],
        ],
        wait_for_completion=True,
    )

    @task
    def end_pipeline():
        print("E-Commerce Data Pipeline completed")

    start = start_pipeline()
    end = end_pipeline()

    validation = validate_staging_data()

    start >> run_glue_etl >> load_customer_staging >> load_remaining_staging >> validation >> refresh_production >> end
ecommerce_pipeline()