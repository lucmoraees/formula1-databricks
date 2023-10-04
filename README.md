# Formula 1 ETL

<h3>Project Overview:</h3>
This project aims to provide a data analysis solution for Formula-1 race results using Azure Databricks. This is an ETL pipeline to ingest Formula 1 motor racing data, transform and load it into our data warehouse for reporting and analysis purposes. The data is sourced from ergast.com, a website dedicated to Formula 1 statistics, and is stored in Azure Datalake Gen2 storage. Data transformation and analysis were performed using Azure Databricks. The entire process is orchestrated using Azure Data Factory.

<h3>Formula1 Overview</h3>
Formula 1 (F1) is the top tier of single-seater auto racing worldwide, governed by the FIA. It features high-tech, powerful cars with hybrid engines. Every season happens once a year, each race happens over weekends (Friday to Sunday). Each race is conducted in individual circuits. 10 Teams/Constructors will participate. Two Drivers will be assigned in a team. The season includes 20-23 races (Grands Prix) held in various countries. Safety is a priority with strict regulations and constant advancements. Pit stops for tire changes and adjustments are common. There will be a qualifying round conducted on Saturday to decide the grid positions of drivers for the Sunday match. Each race contains 50-70 laps. Pitstops will be available to change tires or cars. Race results include driver standings and constructor standings. The driver that tops the driver's standings becomes the drivers' champion and the team that tops the constructor standings becomes the constructors' champion.

<h3>Architecture diagram</h3>

<img src="https://github.com/lucmoraees/formula1-databricks/blob/main/images/Architecture.png">

# ER Diagram:

The structure of the database is shown in the following ER Diagram and explained in the [Database User Guide](http://ergast.com/docs/f1db_user_guide.txt)
<img src="https://github.com/lucmoraees/formula1-databricks/blob/main/images/ER Diagram.png">

## How it works:

<h3>Source Date Files</h3>
We are referring to open-source data from the website Ergast Developer API. Data was available from 1950 till 2022.

| File Name    | File Type                   |
| ------------ | --------------------------- |
| Circuits     | CSV                         |
| Races        | CSV                         |
| Constructors | Single Line JSON            |
| Drivers      | Single Line Nested JSON     |
| Results      | Single Line JSON            |
| PitStops     | Multi Line JSON             |
| LapTimes     | Split CSV Files             |
| Qualifying   | Split Multi Line JSON Files |

#### Execution Overview:

- Azure Data Factory (ADF) is responsible for the execution of Azure Datarbicks notebooks as well as monitoring them. We import data from Ergast API to Azure Data Lake Storage Gen2 (ADLS). The raw data is stored in the container at **Bronze zone** (landing zone).
- Data in the Bronze zone is ingested using Azure Databricks notebook. The data is transformed into delta tables using upsert functionality. ADF then uploads the data to ADLS **Silver zone** (standardization zone).
- Ingested data in **Silver zone** is transformed using Azure Databricks SQL notebook. Tables are joined and aggregated for analytical and visualization purposes. The output is loaded to the **Gold zone** (analytical zone).

#### ETL pipeline:

ETL flow comprises two parts:

- Ingestion: Process data from **Bronze zone** to **Silver zone**
- Transformation: Process data from **Silver zone** to **Gold zone**

In the first pipeline, data stored in JSON and CSV format is read using Apache Spark with minimal transformation saved into a delta table. The transformation includes dropping columns, renaming headers, applying schema, and adding audited columns (`ingestion_date` and `file_source`) and `file_date` as the notebook parameter. This serves as a dynamic expression in ADF.

In the second pipeline, Databricks SQL reads preprocessed delta files and transforms them into the final dimensional model tables in delta format. Transformations performed include dropping duplicates, joining tables using join, and aggregating using a window.

ADF is scheduled to run every Sunday at 10 PM and is designed to skip the execution if there is no race that week. We have another pipeline to execute the ingestion pipeline and transformation pipeline using file_date as the parameter for the tumbling window trigger.

<img src="https://github.com/lucmoraees/formula1-databricks/blob/main/images/Pipeline.png">

## Azure Resources Used for this Project:

- Azure Data Lake Storage
- Azure Data Factory
- Azure Databricks
- Azure Key Vault

## Analysis Result:

<img src="https://github.com/lucmoraees/formula1-databricks/blob/main/images/Analysis1.jpeg">

<img src="https://github.com/lucmoraees/formula1-databricks/blob/main/images/Analysis2.jpeg">

<h3>Technologies/Tools Used:</h3>
<ul>
  <li>Pyspark</li> 
  <li>Spark SQL</li> 
  <li>Delta Lake</li> 
  <li>Azure Databricks </li> 
  <li>Azure Data Factory</li> 
  <li>Azure Date Lake Storage Gen2</li> 
  <li>Azure Key Vault</li> 
  <li>Power BI</li> 
</ul>
