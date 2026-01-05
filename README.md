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
        .option("cloudFiles.schemaLocation", f"abfss://bronze@adlsdatabricksprojectcz.dfs.core.windows.net/checkpoint_cuastomers") \
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



<img width="2596" height="731" alt="image" src="https://github.com/user-attachments/assets/99054100-6eb5-48d7-90b0-f6790a9578fd" />


<img width="2551" height="578" alt="image" src="https://github.com/user-attachments/assets/b29ace01-85b4-4ad1-999a-d1473a630edf" />

Check the record count of cumsters i nthe bronze container


<img width="2579" height="528" alt="image" src="https://github.com/user-attachments/assets/7f497e57-d51b-4df0-bdfe-dbb3e014786b" />



Add the second parquet files to the customers folder ion tyhe source container(This file containes 10 rows)


<img width="1957" height="714" alt="image" src="https://github.com/user-attachments/assets/64bd8165-9373-4b28-957e-5dbeb0ee715b" />

Run the notebook again and check how many records are on the customer folder of the bronze container, TR

<img width="2549" height="486" alt="image" src="https://github.com/user-attachments/assets/0db898fb-e7b3-433d-ab5d-12980e9aaa92" />

We now see that the 10 additional customer rows have been ingested into the bronze layer. This means our incremental load is working as indented as only new files were processed.

Make the notebook dynamic
oarameterize the notebook with loops
param can be changed at runtime

create the parameters notebook

<img width="2429" height="837" alt="image" src="https://github.com/user-attachments/assets/327aec48-73fd-41b3-af2b-c79fe31921d3" />

create teh task
<img width="2891" height="1490" alt="image" src="https://github.com/user-attachments/assets/fa35cc30-2a4c-432d-8d2d-c9ff9655ad42" />

enable the loop so the file_name array can be looped over
<img width="1550" height="905" alt="image" src="https://github.com/user-attachments/assets/a3a0cce9-8b00-414c-b882-146735324f1b" />


<img width="1685" height="587" alt="image" src="https://github.com/user-attachments/assets/a87b4c6a-77af-4f4d-bc25-57d34a8cde3a" />


customers
Test teh incremental loan for the products file
first files has 490 rows and second has 10. This is to ensure processing is happening only once and if a new file is added to the container, it will be processed.

Part 1 - products in the bronze container after being processede
<img width="2506" height="534" alt="image" src="https://github.com/user-attachments/assets/ba33fafd-0602-442c-a575-4e27fe1add1c" />

Part 2 - Add the second part of the products data into the ADLS source container
<img width="2022" height="735" alt="image" src="https://github.com/user-attachments/assets/9603276c-eb66-4394-ba0c-7d71f99304f6" />

Run the job again then check the count in the bronze container
<img width="2472" height="518" alt="image" src="https://github.com/user-attachments/assets/f0489828-dbe4-4313-af8d-d8e4bddf32a9" />



```python
df = spark.read.format("parquet").load("abfss://bronze@adlsdatabricksprojectcz.dfs.core.windows.net/customers")

df.count()
```
