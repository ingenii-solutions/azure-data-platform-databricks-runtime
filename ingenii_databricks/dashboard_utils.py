from pyspark.dbutils import DBUtils
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col
from pyspark.sql.session import SparkSession


def create_widgets(spark: SparkSession, dbutils: DBUtils) -> None:
    """
    Create the widgets for the dashboard

    Parameters
    ----------
    spark : SparkSession
        Object for interacting with Delta tables
    dbutils : DBUtils
        Object for interacting with many things, in this case the widgets
    """
    orch_df = spark.table("orchestration.import_file")
    dbutils.widgets.dropdown(
        name="entry_type",
        defaultValue="Incomplete",
        choices=["All", "Incomplete", "Issues", "Complete"],
        label="Entry Type")
    dbutils.widgets.dropdown(
        name="source",
        defaultValue="--",
        choices=["--"] + [
            s.source for s in orch_df.select("source").distinct().collect()
        ],
        label="Source")
    dbutils.widgets.dropdown(
        name="table",
        defaultValue="--",
        choices=["--"] + [
            t.table for t in orch_df.select("table").distinct().collect()
        ],
        label="Table")


def filtered_import_table(spark: SparkSession, dbutils: DBUtils) -> DataFrame:
    """
    Use the widget values to produce a filtered table

    Parameters
    ----------
    spark : SparkSession
        Object for interacting with Delta tables
    dbutils : DBUtils
        Object for interacting with many things, in this case the widgets

    Returns
    -------
    DataFrame
        The dataframe containing the final data
    """
    table_query = spark.table("orchestration.import_file")

    entry_type = dbutils.widgets.get("entry_type")
    source = dbutils.widgets.get("source")
    table = dbutils.widgets.get("table")
    if entry_type == "Incomplete":
        table_query = table_query.filter(col("date_completed").isNull())
    elif entry_type == "Issues":
        table_query = table_query.filter(
            (col("increment") > 0) & col("date_completed").isNull())
    elif entry_type == "Complete":
        table_query = table_query.filter(col("date_completed").isNotNull())

    if source != "--":
        table_query = table_query.filter(col("source") == source)
    if table != "--":
        table_query = table_query.filter(col("table") == table)

    return table_query
