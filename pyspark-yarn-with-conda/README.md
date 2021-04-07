# PySpark Examples

## Setup Conda & Python environment on Gateway/Edge host
* Directory creation
    ```
    mkdir /hadoopfs/fs1/anaconda3
    chown root:cdp-admins /hadoopfs/fs1/anaconda3
    chmod 775 /hadoopfs/fs1/anaconda3
    setfacl -d -m group:cdp-admins:rwx /hadoopfs/fs1/anaconda3
    ```
* Install Anaconda from [website](https://www.anaconda.com/products/individual) & point the install to the existing directory `/hadoopfs/fs1/anaconda3`
    ```
    bash Anaconda3-2020.11-Linux-x86_64.sh -u
    ```
* Create Environment - Run the following on the gateway node. `cdp-users` group will need to provide the entire conda path - `sudo /hadoopfs/fs1/anaconda3/bin/conda`
    ```
    conda create -y -n spark_python37_env python=3.7 -c pandas conda-pack
    conda install -c conda-forge pyarrow 
    conda pack -f -o spark_python37_env.tar.gz
    ```
* All users should now be able to run the following access. Use sudo only if you are running in to issues with installing additional packages. `cdp-users` group will need to provide the entire conda path - `sudo /hadoopfs/fs1/anaconda3/bin/conda`
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