# PySpark with Conda Environment for Python packages 

## Setup Conda & Python environment on Gateway/Edge host
* Create the required directory for installation & set it up with permissions.
    ```
    mkdir /hadoopfs/fs1/anaconda3
    chown root:cdp-admins /hadoopfs/fs1/anaconda3
    chmod 775 /hadoopfs/fs1/anaconda3
    setfacl -d -m group:cdp-admins:rwx /hadoopfs/fs1/anaconda3
    setfacl -d -m group:cdp-users:rwx /hadoopfs/fs1/anaconda3
    ```
* Install Anaconda from [website](https://www.anaconda.com/products/individual) & point the install to the existing directory `/hadoopfs/fs1/anaconda3`
    ```
    bash Anaconda3-2020.11-Linux-x86_64.sh -u
    ```
* To create environment, run the following on the gateway node.
    ```
    conda create -y -n spark_python37_env python=3.7 -c pandas conda-pack
    conda install -y -c conda-forge pyarrow 
    conda pack -f -o spark_python37_env.tar.gz
    ```
* Users should now be able to run the following access the Conda environment.
    ```
    conda spark_python37_env
    conda deactivate
    ```

## Submitting PySpark job
Submit the PySpark job with the following command
```
 PYSPARK_PYTHON=./spark_python37_env/bin/python spark-submit \
    --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./spark_python37_env/bin/python \
    --master yarn \
    --deploy-mode cluster \
    --archives spark_python37_env.tar.gz#spark_python37_env panda-pyspark.py
```