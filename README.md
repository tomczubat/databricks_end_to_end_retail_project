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
<img width="3432" height="1804" alt="image" src="https://github.com/user-attachments/assets/36c589ab-b286-425a-bed8-0beb2897f04d" />


Create notebook to load data incrementally

AUTOLOADER read and write stream
