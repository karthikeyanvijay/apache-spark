library(sparklyr)
library(xgboost)
#library(SparklyrHWC)
#library(SparklyrHWC,lib.loc=c(file.path("/opt/cloudera/parcels/CDH/lib/hive_warehouse_connector/SparklyrHWC-1.0.0.7.2.7.2-1")))
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

# # Below approach crashes the driver at full volume
# sdf_sql(sc,"use datalake")
# intDf <- sparklyr::spark_read_table(sc, "scoring_results_acid") # Collect issued here
# data <- intDf %>% select(id_key,col1,col2,col3,col4) %>% rename_all(toupper) %>% rename(id_key = ID_KEY)

# # Below approach not tested
# hs <- build(HiveWarehouseBuilder.session(sc))
# hwcDf <- SparklyrHWC::executeQuery(hs,sqlText)
# data <- sparklyr::sdf_copy_to(sc, hwcDf)
# SparklyrHWC::commitTxn(hs)
# print(collect(data))

# Define Function
func_scoring <- function(p){
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

#detach("package:SparklyrHWC", unload=TRUE)
# Write an external table
spark_write_table(appliedDF, "datalake.scoring_results", mode="overwrite")
