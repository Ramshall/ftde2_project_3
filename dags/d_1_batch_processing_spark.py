from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable

from pyspark.sql import SparkSession
from sqlalchemy import create_engine
import pandas as pd

from datetime import datetime

def get_spark_session():
    spark_jars_packages = Variable.get("spark_jars_packages")
    return SparkSession.builder \
        .config("spark.jars.packages", spark_jars_packages) \
        .master("local") \
        .appName("PySpark_Postgres") \
        .getOrCreate()

def read_postgres_table(spark, table_name):
    postgres_conn = BaseHook.get_connection("postgres_conn")
    return spark.read.format("jdbc") \
        .option("url", f"jdbc:postgresql://{postgres_conn.host}:{postgres_conn.port}/{postgres_conn.schema}") \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", table_name) \
        .option("user", postgres_conn.login) \
        .option("password", postgres_conn.password) \
        .load()

def get_tidb_engine():
    tidb_conn = BaseHook.get_connection("tidb_conn")
    return create_engine(
        f'mysql+mysqlconnector://{tidb_conn.login}:{tidb_conn.password}@{tidb_conn.host}:{tidb_conn.port}/{tidb_conn.schema}',
        echo=False
    )

def fun_top_countries_get_data(**kwargs):
    spark = get_spark_session()
    
    df_country = read_postgres_table(spark, "country")
    df_city = read_postgres_table(spark, "city")

    df_country.createOrReplaceTempView("country")
    df_city.createOrReplaceTempView("city")
    
    df_result_1 = spark.sql('''
    SELECT
      country,
      COUNT(country) AS total,
      current_date() AS date
    FROM country AS co
    INNER JOIN city AS ci
      ON ci.country_id = co.country_id
    GROUP BY 1
    ''')
    
    df_result_1.write.mode("append").partitionBy("date") \
        .option('compression', 'snappy') \
        .save('data_result_task_1')

def fun_top_countries_load_data(**kwargs):
    df = pd.read_parquet('data_result_task_1')
    engine = get_tidb_engine()
    df.to_sql(name='top_country_rama', con=engine, if_exists='append', index=False)

def fun_total_film_get_data(**kwargs):
    spark = get_spark_session()
    
    df_film = read_postgres_table(spark, "film")
    df_film_category = read_postgres_table(spark, "film_category")
    df_category = read_postgres_table(spark, "category")

    df_film.createOrReplaceTempView("film")
    df_film_category.createOrReplaceTempView("film_category")
    df_category.createOrReplaceTempView("category")
    
    df_result_2 = spark.sql('''
      SELECT
        name,
        COUNT(name) as total,
        current_date() as date
      FROM (
        SELECT * FROM film AS f
        JOIN film_category as fc
        ON f.film_id = fc.film_id) AS j
      JOIN category AS c ON j.category_id = c.category_id
      GROUP BY name ORDER BY COUNT(name) DESC
    ''')
    
    df_result_2.write.mode("append").partitionBy("date") \
        .option('compression', 'snappy') \
        .save('data_result_task_2')
    
def fun_total_film_load_data(**kwargs):
    df = pd.read_parquet('data_result_task_2')
    engine = get_tidb_engine()
    df.to_sql(name='total_film_category_rama', con=engine, if_exists='append', index=False)

with DAG(
    dag_id='d_1_batch_processing_spark',
    start_date=datetime(2022, 5, 28),
    schedule_interval='00 23 * * *',
    catchup=False
) as dag:

    start_task = EmptyOperator(
        task_id='start'
    )

    op_top_countries_get_data = PythonOperator(
        task_id='top_countries_get_data',
        python_callable=fun_top_countries_get_data
    )

    op_top_countries_load_data = PythonOperator(
        task_id='top_countries_load_data',
        python_callable=fun_top_countries_load_data
    )
    
    op_total_film_get_data = PythonOperator(
        task_id='total_film_get_data',
        python_callable=fun_total_film_get_data
    )
    
    op_total_film_load_data = PythonOperator(
        task_id='total_film_load_data',
        python_callable=fun_total_film_load_data
    )
    
    end_task = EmptyOperator(
        task_id='end'
    )
    
    start_task >> op_top_countries_get_data >> op_top_countries_load_data >> end_task
    start_task >> op_total_film_get_data >> op_total_film_load_data >> end_task