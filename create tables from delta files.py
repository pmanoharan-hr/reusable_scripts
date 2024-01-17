"""
use this block to move delta tables from Databricks PROD to Databricks Test
Beware! Delta Table in Test will be replaced.
"""
from pyspark.sql.functions import col

schema_name = ["silverpop"]
location = spark.sql(f"describe schema {schema_name}").where(col("database_description_item") == "Location").collect()[0][1].replace('test','prod')
print('Schema Location: ',location)
files = dbutils.fs.ls(location)
for file in files:
    table_name = file.name.replace("/",'')

    result = spark.sql(f"create or replace table {schema_name}.{table_name} as select * from delta.`{file.path}`")
    display(result)