import pyspark.pandas as ps
from pyspark.sql import SparkSession
import os

def init_feature_store_path():
    from feast import FeatureStore
    with open("feature_store.yaml", "w") as yaml_file:
        yaml_file.write(os.getenv("FEATURE_STORE_YAML"))
    return FeatureStore(repo_path=".")

def prepare_dataset(store, batch,**kwargs):

    feature_service = store.get_feature_service(feature_service_1)
    enriched_job = store.get_historical_features(
        entity_df=batch,
        features=feature_service,
    )
    return enriched_job.to_spark_df()

if __name__ == "__main__":
    store = init_feature_store_path()
    conf = SparkConf().set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
    .set("spark.hadoop.fs.s3a.access.key", access_key)\
    .set("spark.hadoop.fs.s3a.secret.key", secret_key)\
    .set("spark.hadoop.fs.s3a.endpoint", s3_endpoint)\
    .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .set("spark.hadoop.fs.s3a.path.style.access","true")\
    .set("spark.driver.extraJavaOptions", "-Divy.cache.dir=/tmp -Divy.home=/tmp")\
    .set("spark.executor.extraJavaOptions", "-Divy.cache.dir=/tmp -Divy.home=/tmp")
    sc = SparkContext.getOrCreate(conf=conf)
    spark = SparkSession(sc).builder.appName("test_predict").getOrCreate()
    df = spark.sql(f"select * from cdm.nvg_data_gr_cre_dlq_t3_r2")

    enriched_df = prepare_dataset(store, df)
    import mlflow

    # Load model as a Spark UDF.
    loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri="models:/breast_cancer/2")

    # Predict on a Spark DataFrame.
    columns = list(df.columns)
    df.withColumn('predictions', loaded_model(*columns)).collect()
    if spark._jsparkSession.catalog().tableExists('cdm', 'output_storage'):
        df.write.format("parquet").mode("overwrite").insertInto(f"cdm.output_storage")
    else:
        df.write.format("parquet").mode("overwrite").saveAsTable(f"cdm.output_storage")
    spark.stop()