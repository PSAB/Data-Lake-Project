## Project Description

This project contains one script: ```etl.py```. 
This script's tasks consist of the following:
* Extract data from S3.
* Use Spark to process the data into a Star Schema parquet format.
* Load processed data back into S3.

```dl.cfg``` is a config file that contains critical access information to remote AWS services

Data for this project can be found in multiple places:
* Within the local ```data``` directory (smaller subset of dataset)
* On the provided S3 bucket ```s3a://udacity-dend/```


## How to use

Type in command line:
```python etl.py```
<br>
This should run the tasks that would load datasets into Spark dataframes, process those dataframes, and write them back as parquet files back into S3.