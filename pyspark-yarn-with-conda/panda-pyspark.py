import pandas as pd
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('pandaspark').getOrCreate()
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

details = { 
    'Name' : ['John', 'Mark', 'Ashley', 'Shiv'], 
    'Age' : [24, 54, 34, 46], 
    'Department' : ['Finance', 'HR', 'IT', 'HR'], 
} 
  
# creating a Dataframe object  
pdf = pd.DataFrame(details) 
df = spark.createDataFrame(pdf)
result_pdf = df.select("*").toPandas()
print("Printing Spark dataframes")
df.show()
print("Printing Panda dataframes")
print(result_pdf)
