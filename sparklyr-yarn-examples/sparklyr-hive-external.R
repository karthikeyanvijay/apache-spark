library(sparklyr)
library(dplyr)

# Configuring runtime variables for Spark
Sys.setenv(HADOOP_HOME = "/opt/cloudera/parcels/CDH/lib/hadoop")
Sys.setenv(SPARK_HOME =  "/opt/cloudera/parcels/CDH/lib/spark/")

# Spark Configuration
config <- spark_config()
config$spark.app.name <- "SparklyR-External-Table"
# Distribute files
config$spark.yarn.dist.archives <- '/hadoopfs/fs1/code/r_env.tar.gz#r_env'
config$spark.yarn.dist.files<- 'scoring.Rdata#scoring.Rdata'
# Specify the R command to use, as well as various R locations on the workers
config$spark.r.command <- "./r_env/bin/Rscript"
config$sparklyr.apply.env.R_HOME <- "./r_env/lib/R"
config$sparklyr.apply.env.RHOME <- "./r_env"
config$sparklyr.apply.env.R_SHARE_DIR <- "./r_env/lib/R/share"
config$sparklyr.apply.env.R_INCLUDE_DIR <- "./r_env/lib/R/include"
# Set Driver & Executor settings
config$spark.executor.instances <- "8"
configspark.executor.cores <- "5"
config$spark.yarn.executor.memory <- "2G"
config$spark.yarn.executor.memoryOverhead <- "6G"
config$spark.yarn.queue <- "default"
# Set Cluster specific settings
config$spark.network.timeout <- "10000001"
config$spark.executor.heartbeatInterval <- "10000000"

# Launch Spark Job
sc <- spark_connect(master = "yarn", config = config)
print(invoke(spark_context(sc), "applicationId"))

# Fetching SQL from file and loading to spark dataframe
fileName <- "scoring.sql"
sqlText <- readChar(fileName, file.info(fileName)$size)
# Query External table
data <- sdf_sql(sc,sqlText)
#print(collect(data))

# Define Function
func_scoring <- function(p){
        # Add code to return predictions
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

