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
        .option("cloudFiles.schemaLocation", f"abfss://bronze@adlsdatabricksprojectcz.dfs.core.windows.net/checkpoint_customers") \
        .load(f"abfss://source@adlsdatabricksprojectcz.dfs.core.windows.net/customers") \
```

```python
df.writeStream \
    .format("parquet")  \
    .outputMode("append") \
    .option("checkpointLocation", f"abfss://bronze@adlsdatabricksprojectcz.dfs.core.windows.net/checkpoint_customers") \
    .trigger(once=True) \
    .start(f"abfss://bronze@adlsdatabricksprojectcz.dfs.core.windows.net/customers")

```

The ".trigger(once=True)" will ensure that data is onoly processed once from the source container. The stream will also stop once the data is written to the bronzse lauer.





Check the record count of cumsters i nthe bronze container


<img width="2579" height="528" alt="image" src="https://github.com/user-attachments/assets/7f497e57-d51b-4df0-bdfe-dbb3e014786b" />



Add the second parquet files to the customers folder ion tyhe source container(This file containes 10 rows)


<img width="1957" height="714" alt="image" src="https://github.com/user-attachments/assets/64bd8165-9373-4b28-957e-5dbeb0ee715b" />

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

source_folders = [
  {"folder_name": "orders"},
  {"folder_name": "customers"},
  {"folder_name": "products"},
  {"name": "silver"}
]
```

### Create a  Databricks Job Task Values Store that will be able to be used in other taks in our job

```python
dbutils.jobs.taskValues.set("source_folders", source_folders)
```

### create a job and add a the parameters notebook as a task
<img width="2860" height="987" alt="image" src="https://github.com/user-attachments/assets/181c297f-ee25-4995-a711-050ca372fc56" />

### Create a bronze_autoloader task that runs the bronze_layer notebook. This task is dependent on receiving the paramters info from the paramters notebook

<img width="1603" height="1026" alt="image" src="https://github.com/user-attachments/assets/4073bfc2-41b5-4d86-a605-a6d45ef31ce6" />

Add the source_folder array  and create the task
<img width="1623" height="345" alt="image" src="https://github.com/user-attachments/assets/8f401ab2-ce9a-4f9d-a3a9-b2fadb3f244d" />

We will need to enable looping over the task
<img width="669" height="504" alt="image" src="https://github.com/user-attachments/assets/d87970e3-ac73-4d9e-b403-f53289580302" />




<img width="1508" height="405" alt="image" src="https://github.com/user-attachments/assets/83d07349-63d6-4b22-ab4d-6e68b16ea59d" />


Set the For each look with a dynamic input of the parameter output name
<img width="1523" height="618" alt="image" src="https://github.com/user-attachments/assets/235afdad-18a0-4f2c-87e4-15e4519576e8" />


Set the value of the key in the bronze_autoloader to the name of the dictionary
<img width="1545" height="333" alt="image" src="https://github.com/user-attachments/assets/1c217874-45b7-47ab-9ba4-62e160e0f2cb" />


### The job for bronze_incremental is now ready to be tested
<img width="1702" height="796" alt="image" src="https://github.com/user-attachments/assets/e549b118-b93b-4190-8465-a898b00bfd42" />

