
import sys
from random import random
from operator import add
import pyspark.pandas as ps
from pyspark.sql import SparkSession
import os

def init_feature_store_path():
    from feast import FeatureStore
    with open("feature_store.yaml", "w") as yaml_file:
        yaml_file.write(os.getenv("FEATURE_STORE_YAML"))
    return FeatureStore(repo_path=".")

def prepare_dataset(store, batch,**kwargs):
    date= datetime.utcnow() - datetime(1970, 1, 1)
    seconds =(date.total_seconds())
    milliseconds = round(seconds*1000)
    dataset_name = "bms5_dataset_" + milliseconds

    from feast.infra.offline_stores.contrib.postgres_offline_store.postgres_source import SavedDatasetPostgreSQLStorage

    feature_service = store.get_feature_service(feature_service_1)
    enriched_job = store.get_historical_features(
        entity_df=batch,
        features=feature_service,
    )
    dataset = store.create_saved_dataset(
        from_=enriched_job,
        name=dataset_name,
        storage=SavedDatasetPostgreSQLStorage(table_ref=dataset_name),
    )
    return enriched_job.to_df()

if __name__ == "__main__":
    store = init_feature_store_path()
    bucket_name = os.environ.get('KUBERNETES_S3_BUCKET') 
    s3_endpoint = os.environ.get('KUBERNETES_MLFLOW_S3_ENDPOINT_URL') 
    access_key = os.environ.get('AWS_ACCESS_KEY_ID') 
    secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')
    conf = SparkConf().set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")    .set("spark.hadoop.fs.s3a.access.key", access_key)    .set("spark.hadoop.fs.s3a.secret.key", secret_key)    .set("spark.hadoop.fs.s3a.endpoint", s3_endpoint)    .set("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")     .set("spark.hadoop.fs.s3a.path.style.access","true")    .set("spark.driver.extraJavaOptions", "-Divy.cache.dir=/tmp -Divy.home=/tmp")    .set("spark.executor.extraJavaOptions", "-Divy.cache.dir=/tmp -Divy.home=/tmp")
    sc = SparkContext.getOrCreate(conf=conf)
    spark = SparkSession(sc).builder.appName("bms5_predict").getOrCreate()
    df = spark.sql(f"select * from dbo.entity_df")
     
    enriched_df = spark.createDataFrame(prepare_dataset(store, df.to_pandas()))
    import mlflow

    # Load model as a Spark UDF.
    loaded_model = mlflow.pyfunc.spark_udf(spark, model_uri="models:/breast_cancer/2")
    
    # Predict on a Spark DataFrame.
    columns = list(enriched_df.columns)
    df.withColumn('predictions', loaded_model(*columns)).collect()
    if spark._jsparkSession.catalog().tableExists('dbo', 'output_storage'):
        df.write.format("parquet").mode("overwrite").insertInto(f"dbo.output_storage")
    else:    
        df.write.format("parquet").mode("overwrite").saveAsTable(f"dbo.output_storage")
    spark.stop()
    