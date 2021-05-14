 #!/bin/bash
 spark-submit  \
  --class com.cloudera.examples.SparkKafkaSample \
  --master=yarn  \
  --deploy-mode=cluster \
  --executor-memory 4G  \
  --driver-memory 4G  \
  --num-executors 3 \
  --files spark-client.properties,adminuser1.keytab,jaas.conf,truststore.jks  \
  --conf spark.yarn.submit.waitAppCompletion=false \
  --driver-java-options "-Djava.security.auth.login.config=./jaas.conf"  \
  --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=./jaas.conf"  \
  ./SparkKafkaSample-1.0.jar \
  ./spark-client.properties