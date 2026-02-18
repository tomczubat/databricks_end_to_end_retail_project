# databricks_end_to_end_retail_project


Project Overview
Spark Structured Streaming


Project Components

Project Components
<img width="3420" height="973" alt="image" src="https://github.com/user-attachments/assets/9f17865b-0e68-483e-924c-f56c4788b99e" />


to give the databricks connector permission to the ADLS Gen 2 continaers by granting read/write access to the resource
<img width="2064" height="936" alt="image" src="https://github.com/user-attachments/assets/c7b3ad91-5513-4b2c-885d-e5ea21a314a4" />



<img width="3109" height="1625" alt="image" src="https://github.com/user-attachments/assets/05fffff9-252b-4ab6-b523-3469592e10ee" />

assign access to unity catalog as well
<img width="3013" height="1407" alt="image" src="https://github.com/user-attachments/assets/6d37f4d6-5c67-4a94-b5e9-6c2354a8b989" />


maybe also add this role

Storage Blob Data Owner
<img width="3061" height="1617" alt="image" src="https://github.com/user-attachments/assets/3ee1cca1-0efb-4cd6-b78d-5384ac0b8771" />


After the netastire us created, assugb it to our workspace and enable unityt catalog from the data ingest side panel
<img width="3358" height="1793" alt="image" src="https://github.com/user-attachments/assets/42347ebf-25b0-4155-ba58-2117e8625803" />
<img width="1727" height="1017" alt="image" src="https://github.com/user-attachments/assets/f59d5036-4922-4bb4-83a4-8ab53ab649ed" />


Create new catalog
<img width="3366" height="1812" alt="image" src="https://github.com/user-attachments/assets/8ef2c970-8492-467a-bd2b-d411f75d413d" />


set up the external location connection to each container to the unity catalog
<img width="3366" height="1192" alt="image" src="https://github.com/user-attachments/assets/144f9372-0882-4e50-8aca-1eb632231f05" />

ingest the parquet file from the regions folder in the source container container
<img width="3422" height="1828" alt="image" src="https://github.com/user-attachments/assets/9e8181e2-1268-4310-87fa-cb27f858b55e" />

Preview the table that will be created and make sure to select the bronze schema.

.trigger(once=True) = run one micro‑batch, process everything available, then stop.

<img width="3432" height="1804" alt="image" src="https://github.com/user-attachments/assets/36c589ab-b286-425a-bed8-0beb2897f04d" />




Create notebook to load data incrementally

AUTOLOADER read and write stream
.trigger(once=True) = run one micro‑batch, process everything available, then stop.

read and write the first customers parquet file from the source container to the bronzse container


```python
df =  spark.readStream.format("cloudFiles") \
        .option("cloudFiles.format", "parquet") \
        .option("cloudFiles.schemaLocation", f"abfss://bronze@adlsdatabricksprojectcz.dfs.core.windows.net/checkpoint_{folder_name}") \
        .load(f"abfss://source@adlsdatabricksprojectcz.dfs.core.windows.net/{folder_name}") 
    

```

```python
df.writeStream \
    .format("parquet")  \
    .outputMode("append") \
    .option("checkpointLocation", f"abfss://bronze@adlsdatabricksprojectcz.dfs.core.windows.net/checkpoint_{folder_name}") \
    .trigger(once=True) \
    .start(f"abfss://bronze@adlsdatabricksprojectcz.dfs.core.windows.net/{folder_name}")


```

The ".trigger(once=True)" will ensure that data is onoly processed once from the source container. The stream will also stop once the data is written to the bronzse lauer.





Check the record count of cumsters i nthe bronze container


<img width="2579" height="528" alt="image" src="https://github.com/user-attachments/assets/7f497e57-d51b-4df0-bdfe-dbb3e014786b" />



Add the second parquet files to the customers folder ion tyhe source container(This file containes 10 rows)


<img width="1967" height="681" alt="image" src="https://github.com/user-attachments/assets/552298aa-0b66-4f25-ab81-3df835f3bdc5" />

Run the notebook again and check how many records are on the customer folder of the bronze container, TR

<img width="2549" height="486" alt="image" src="https://github.com/user-attachments/assets/0db898fb-e7b3-433d-ab5d-12980e9aaa92" />

We now see that the 10 additional customer rows have been ingested into the bronze layer. This means our incremental load is working as indented as only new files were processed.



## Make the notebook dynamic 
oarameterize the notebook with loops
param can be changed at runtime

### create the parameters notebook
The array of dictionaries will store the folder names from the source container

```python
# Define an array of dictionaries to store the folder names

datasets = [
  {"folder_name": "orders"},
  {"folder_name": "customers"},
  {"folder_name": "products"},
]
```

### Create a  Databricks Job Task Values Store that will be able to be used in other taks in our job

```python
folder_name = dbutils.widgets.get("folder_name")
```

### create a job and add a the parameters notebook as a task
<img width="2860" height="987" alt="image" src="https://github.com/user-attachments/assets/181c297f-ee25-4995-a711-050ca372fc56" />

### Create a bronze_autoloader task that runs the bronze_layer notebook. This task is dependent on receiving the paramters info from the paramters notebook

<img width="1603" height="1026" alt="image" src="https://github.com/user-attachments/assets/4073bfc2-41b5-4d86-a605-a6d45ef31ce6" />

Add the source_folder array  and create the task
<img width="1623" height="345" alt="image" src="https://github.com/user-attachments/assets/8f401ab2-ce9a-4f9d-a3a9-b2fadb3f244d" />

Enable looping over the task
<img width="669" height="504" alt="image" src="https://github.com/user-attachments/assets/d87970e3-ac73-4d9e-b403-f53289580302" />






Set the For each loop with a dynamic input of the parameter output name

<img width="1573" height="550" alt="image" src="https://github.com/user-attachments/assets/1b871988-a89a-466e-ac7c-3f5b71ddcb47" />




SSet the value of the key in the bronze_autoloader to the name of the dictionary

<img width="1407" height="188" alt="image" src="https://github.com/user-attachments/assets/48a5df74-d088-4ce1-89ee-e92362820c98" />



### The job for bronze_incremental is now ready to be tested
<img width="1702" height="796" alt="image" src="https://github.com/user-attachments/assets/e549b118-b93b-4190-8465-a898b00bfd42" />

The first run was successful
<img width="1554" height="185" alt="image" src="https://github.com/user-attachments/assets/6ef28c5c-b29b-43af-9578-726b58b5f49f" />

### Check the row count in the bronze container for all three folders
<img width="2703" height="1238" alt="image" src="https://github.com/user-attachments/assets/3889612f-0a5a-479a-8a37-21c2fc29b74e" />

### Add the part 2 parquet file for each folder inteh source container

<img width="1769" height="717" alt="image" src="https://github.com/user-attachments/assets/9a2c264b-e780-48d7-8dbe-38b60aeed79a" />
<img width="1582" height="708" alt="image" src="https://github.com/user-attachments/assets/ba2ee5f9-1345-4b8c-82bc-d290bc948057" />
<img width="1591" height="744" alt="image" src="https://github.com/user-attachments/assets/2e4ec460-d2e0-4185-9564-f5b268d65ed7" />

###Run the bronze autoloader again. This should ingest only tyhe part 2 file from each folder
<img width="2900" height="1177" alt="image" src="https://github.com/user-attachments/assets/f6c968d1-3bca-4695-bd4c-498dc4205852" />

### Check thge row count in the bronze container to ensure only the new parquet files were added
<img width="2720" height="1578" alt="image" src="https://github.com/user-attachments/assets/58e79367-4bfb-4a63-9606-ba2408620871" />

## Silver layer
### The purpose of the silver layer is to ingest data from the bronze layer, perfrorm aggregation, and move the data to the gold layer.

@## Create a new notebook for the orders data and attached a compute cluster
Import out neccesary libraries and import the order data from the bronze container into our data frame

```python
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
```

```python
df = spark.read.format("parquet") \
    .load("abfss://bronze@adlsdatabricksprojectcz.dfs.core.windows.net/orders")
```

### Drop the _rescued_date column which will not be used
```python
df = df.drop('_rescued_data')
```

Current dataframe
<img width="2456" height="823" alt="image" src="https://github.com/user-attachments/assets/7cfafee7-0fbe-4aba-bacc-4369210f6ecd" />

### Add a year column based on the order_date column
```python
df = df.withColumn("year", year(col("order_date")))
```

df with additional year column
<img width="3448" height="1101" alt="image" src="https://github.com/user-attachments/assets/dd28b708-7eb7-411c-8159-0f22e9ccef1f" />

## Creating a class with window functions to make our code more reusable
```python
class WindowFunction():

    def dense_rank(self, df):
        df_dense_rank =  df.withColumn("flag", dense_rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
        return df_dense_rank
    
    def rank(self, df):
        df_rank = df.withColumn("flag", dense_rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
        return df_rank
    
    def row_number(self, df):
        df_row_number = df.withColumn("flag", dense_rank().over(Window.partitionBy("year").orderBy(desc("total_amount"))))
        return df_row_number
    
```

### Create an open from the FunctionFunction Class
```python
obj = WindowFunction()
```

### Call dense rank function while passing in our original dataframe and assign this to a new dataframe
```python
df_result = obj.dense_rank(df)
```

### Check the new dataframe to ensure our dense_rank flag was added
<img width="2450" height="851" alt="image" src="https://github.com/user-attachments/assets/2c2eb4d0-31aa-455c-85ea-f4b594636b26" />

