# Databricks notebook source
# COMMAND ----------
from pyspark.ml.feature import Tokenizer, StopWordsRemover, HashingTF
from pyspark.sql.functions import col, concat_ws, lower, regexp_replace, udf, lit
from pyspark.sql.types import ArrayType, FloatType
import numpy as np

# Load Data
df_source = spark.read.table("silver_ceres_metadata")

# Feature Engineering: Combine text fields
df_ml = df_source.withColumn("text_soup", 
    lower(concat_ws(" ", col("title"), col("title"), col("tags"), col("description")))
).withColumn("text_soup", regexp_replace(col("text_soup"), "[^a-zA-Z0-9\\s]", ""))

# COMMAND ----------
# ML Pipeline Definition (Stateless Hashing for Serverless compatibility)
tokenizer = Tokenizer(inputCol="text_soup", outputCol="words_raw")
remover = StopWordsRemover(inputCol="words_raw", outputCol="words_filtered")
hashingTF = HashingTF(inputCol="words_filtered", outputCol="features", numFeatures=1024)

# Apply Transformations
df_tokenized = tokenizer.transform(df_ml)
df_filtered = remover.transform(df_tokenized)
df_features = hashingTF.transform(df_filtered)

# UDF to convert ML SparseVector to standard Float Array (Delta Lake compatibility fix)
@udf(ArrayType(FloatType()))
def vector_to_array_udf(v):
    return v.toArray().tolist()

# Save Features to Delta
df_final = df_features.withColumn("features_array", vector_to_array_udf(col("features"))) \
                      .select("title", "portal", "url", "features_array")

df_final.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("gold_ml_features")

# COMMAND ----------
# Search Logic Implementation

@udf(FloatType())
def array_dot_product(arr1, arr2):
    try:
        return float(np.dot(arr1, arr2))
    except:
        return 0.0

def search_engine(query_text, top_k=10):
    # 1. Vectorize Query
    query_df = spark.createDataFrame([(query_text,)], ["text_soup"])
    q_token = tokenizer.transform(query_df)
    q_filter = remover.transform(q_token)
    q_hashed = hashingTF.transform(q_filter)
    
    # Extract query vector as array
    query_array = q_hashed.head()["features"].toArray().tolist()
    
    # 2. Compute Similarity against persisted Gold Table
    df_base = spark.read.table("gold_ml_features")
    
    results = df_base \
        .withColumn("score", array_dot_product(col("features_array"), lit(query_array))) \
        .filter(col("score") > 0) \
        .orderBy(col("score").desc()) \
        .limit(top_k) \
        .select("title", "portal", "score", "url")
    
    return results

# COMMAND ----------
# Interactive Demo Widget
dbutils.widgets.text("search_query", "population health", "Search Dataset:")
user_query = dbutils.widgets.get("search_query")

if user_query:
    print(f"Results for: {user_query}")
    display(search_engine(user_query))