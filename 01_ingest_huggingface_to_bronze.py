# COMMAND ----------
# Dependency pinning is required to ensure compatibility with Databricks Runtime and HF FileSystem
%pip install "huggingface_hub<0.20" "datasets==2.15.0"

# COMMAND ----------
dbutils.library.restartPython()

# COMMAND ----------
from datasets import load_dataset
import pandas as pd
from pyspark.sql.functions import current_timestamp, lit

# Configuration
dataset_name = "AndreaBozzo/ceres-open-data-index"
table_name = "bronze_ceres_metadata"

# 1. Load Dataset from Hugging Face
ds = load_dataset(dataset_name, split="train")

# 2. Convert to Pandas to handle data types before Spark conversion
pdf = ds.to_pandas()

# Force object columns to string to prevent schema inference issues in Bronze
for col in pdf.columns:
    if pdf[col].dtype == 'object':
        pdf[col] = pdf[col].astype(str)

# 3. Create Spark DataFrame
df_raw = spark.createDataFrame(pdf)

# 4. Add Audit Columns
df_bronze = df_raw.withColumn("ingestion_ts", current_timestamp()) \
                  .withColumn("source_system", lit("HuggingFace"))

# 5. Write to Delta Lake (Managed Table)
df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(table_name)

print(f"Success: Table {table_name} created with {df_bronze.count()} records.")