import os
from pyspark.sql import SparkSession
from datetime import datetime

def init_feature_store_path():
    from feast import FeatureStore
    with open("feature_store.yaml", "w") as yaml_file:
        yaml_file.write(os.getenv("FEATURE_STORE_YAML"))
    return FeatureStore(repo_path=".")

def prepare_dataset(store, batch,**kwargs):

    feature_service = store.get_feature_service(mtp_loan_features)
    enriched_job = store.get_historical_features(
        entity_df=batch,
        features=feature_service,
    )
    return enriched_job.to_spark_df()

if __name__ == "__main__":
    store = init_feature_store_path()
    conf = SparkConf().set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .set("spark.sql.catalogImplementatio "hive")\
    .set("spark.hadoop.hive.metastore.uris", os.environ.get("METASTORE_URI"))\
    .set("spark.sql.warehouse.dir", os.environ.get("METASTORE_WAREHOUSE_DIR"))\
    .set("spark.hadoop.fs.s3a.access.key", os.environ.get("METASTORE_AWS_ACCESS_KEY_ID"))\
    .set("spark.hadoop.fs.s3a.secret.key", os.environ.get("METASTORE_AWS_SECRET_ACCESS_KEY"))\
    .set("spark.hadoop.fs.s3a.endpoint", os.environ.get("FEAST_S3_ENDPOINT_URL"))\
    .set("spark.sql.debug.maxToStringFields", "100")\
    .set("spark.hadoop.fs.s3a.path.style.access", "true")\
    .set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")\
    .set("spark.driver.extraJavaOptions", "-Divy.cache.dir=/tmp -Divy.home=/tmp")\
    .set("spark.executor.extraJavaOptions", "-Divy.cache.dir=/tmp -Divy.home=/tmp")\

    sc = SparkContext.getOrCreate(conf=conf)
    spark = SparkSession(sc).builder.appName("testsokolov_predict").enableHiveSupport().getOrCreate()
    df = f"select * from entities_db_test.entity_ds_guide"

    enriched_df = prepare_dataset(store, df)
    import mlflow

    # Load model as a Spark UDF.
    loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri="models:/guide_model_reg/1")
    input_df = enriched_df[enriched_df.columns.drop("event_timestamp")]
    # Predict on a Spark DataFrame.
    columns = list(input_df.columns)
    df = loaded_model(*columns)
    df = df.withColumn('date_part', datetime.now().strftime('%Y-%m-%d'))
    if spark._jsparkSession.catalog().tableExists('entities_db_test', 'output'):
        df.write.partitionBy("date_part").format("parquet").mode("append").insertInto(f"entities_db_test.output")
    else:
        df.write.partitionBy("date_part").format("parquet").mode("append").saveAsTable(f"entities_db_test.output")
    spark.stop()