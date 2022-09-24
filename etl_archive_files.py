# Databricks notebook source
import datetime
import pytz
from pyspark.sql.functions import *

# COMMAND ----------

source_folder_loc = "/mnt/datalake/UKG/Adf"
archive_folder_loc = "/mnt/datalake/UKG/Adf_Archived"

# COMMAND ----------

def transform_load_files(source_path):
  df = spark.read\
            .format("csv")\
            .option("header",True)\
            .load(source_path)
  
  select_cols = ["Location","EPIC"]
  table_name = source_path[source_path.rfind("/")+1:]
  
  transformed_df = df.select(*select_cols)\
                     .withColumn("row_insert_tsp",lit(datetime.datetime.now()))
    
  transformed_df.write\
                .format("delta")\
                .saveAsTable(table_name)
  

# COMMAND ----------

import datetime
import pytz

def archive_files(source_folder_loc,archive_folder_loc):
  to_archive_files = dbutils.fs.ls(source_folder_loc)
  num_files = 0
  files_transfered = []
  for file in to_archive_files:
    source_path = file.path
    archive_path = file.path.replace(source_folder_loc,archive_folder_loc)
    dbutils.fs.mv(source_path,archive_path)
    num_files += 1
    files_transfered += [source_path[source_path.rfind("/")+1:]]
  print("Archived {} files at time : {}".format(num_files,datetime.datetime.now() \
                                                .astimezone(tz=pytz.timezone('US/Central'))))
  return files_transfered

# COMMAND ----------

transform_load_files(source_folder_loc)

# COMMAND ----------

archive_files(source_folder_loc,archive_folder_loc)
