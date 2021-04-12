library(sparklyr)
library(xgboost)
library(dplyr)
library(yaml)

# Configuring runtime variables for Spark
Sys.setenv(HADOOP_HOME = "/opt/cloudera/parcels/CDH/lib/hadoop")
Sys.setenv(SPARK_HOME =  "/opt/cloudera/parcels/CDH/lib/spark/")

# Read Config file & Launch jobs
config <- yaml.load_file('spark-config.yml')
sc <- spark_connect(master = "yarn", config = config)
print(invoke(spark_context(sc), "applicationId"))

# Fetching SQL from file and loading to spark dataframe
fileName <- "scoring.sql"
sqlText <- readChar(fileName, file.info(fileName)$size)
sdf_sql(sc,"use datalake")
data <- sdf_sql(sc,sqlText)


# Define Function
func_scoring <- function(p){
        #library(arrow)
        p
}

# Define schema
schema <- list(
"id_key" = "string"
, "score" = "double"
, "bins" = "string"
)

# Applying derived functions
appliedDF <- spark_apply(data, func_scoring, schema)

# Write an external table
spark_write_table(appliedDF, "datalake.scoring_results", mode="overwrite")
