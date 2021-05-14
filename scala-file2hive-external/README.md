# file2hive Spark job

## Building the jar
- This repo includes two Maven config files -
    * `pom.xml.hdp` - Configuration to be used for building the JAR for legacy clusters. Rename this file to `pom.xml` before building the jar for HDP clusters.
    * `pom.xml.cdp` - Configuration to used for building the JAR for CDP clusters. Rename this file to `pom.xml` before building the jar for CDP clusters.
- To build the jar for this job, execute `mvn clean install`. The jar file is present under the `target` directory. 

## Dataset
Dataset used in this example can be downloaded from [here](https://www.kaggle.com/dhruvildave/wikibooks-dataset).

## Job Execution
The job can be submitted to YARN with the following command
```
spark-submit  \
  --class com.cloudera.spark.examples.loadfile2hive \
  --master=yarn  \
  --deploy-mode=cluster \
  --executor-memory 2G  \
  --driver-memory 2G  \
  --num-executors 6 \
  --conf spark.yarn.submit.waitAppCompletion=true \
  file2hive-1.0.jar <input_file_path> <input_file_type> <target_table_name> <target_output_format>
```
  
The wrapper script has been included - `submit-file2hive.sh`. Sample execution - 
```
./submit-file2hive.sh hdfs:///${dataDir}/wikibooks-input csv default.wikibooks_data orc
```