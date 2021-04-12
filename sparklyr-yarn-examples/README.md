# SparklyR Examples

## Setup R environment on Gateway/Edge host
* Directory creation
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
* To create the R environment, run the following on the gateway node.
    ```
    conda create -y -n r_env r r-essentials r-base r-sparklyr conda-pack
    conda activate r_env
    conda install -y -c conda-forge r-xgboost r-arrow
    ```
* To package the R environment for run the following command
    ```
    cd /hadoopfs/fs1/anaconda3/envs/
    conda pack -o /hadoopfs/fs1/r_env.tar.gz -d ./r_env
    ```
* Users should now be able to run the following access the Conda environment.
    ```
    conda activate r_env
    conda deactivate
    ```

## Submit SparklyR jobs on YARN
- Review the `spark-config.yml`. Use the Hive ACID properties if required.
- Create a Bash script to source the environment before kicking off the job.
```
#!/bin/bash
source /hadoopfs/fs1/anaconda3/etc/profile.d/conda.sh
conda activate r_env
Rscript sparklyr-sample.R
```