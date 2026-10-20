# Paris Olympics Data Engineering on Azure

An Azure data engineering project built around Paris Olympics CSV data (athletes, NOCs, events, coaches). Azure Data Factory ingests the raw CSV files from an ADLS Gen2 `source` container into a `bronze` container as Parquet using metadata-driven, parameterized pipelines, and Azure Databricks notebooks clean the data and write it to a silver layer as Delta tables.

## Architecture / Flow

```
ADLS Gen2 (source, CSV)
   -> Azure Data Factory (Lookup / GetMetadata + ForEach + Copy)
   -> ADLS Gen2 (bronze, Parquet)
   -> Azure Databricks (PySpark notebooks)
   -> Silver layer (Delta, Unity Catalog volume parisolympics/dbo/silver)
```

## Tech Stack

- Azure Data Factory (Git-integrated, publish branch `adf_publish`)
- Azure Data Lake Storage Gen2
- Azure Databricks (PySpark, Delta Lake, Unity Catalog volumes, job task values / widgets)

## Repository Structure

```
.
├── factory/dev-olympics-adf-azure.json       # Data Factory resource definition
├── linkedService/ls_dataLake.json            # ADLS Gen2 linked service
├── dataset/
│   ├── Json1.json                            # tablename.json metadata file (source container)
│   ├── srcCsvData.json                       # Parameterized source CSV (file_name)
│   ├── SinkParquetData.json                  # Parameterized bronze Parquet sink (folder_name, file_name)
│   ├── datalakeDs.json                       # Source container, used for folder listing
│   ├── paraDs.json                           # Parameterized source CSV (p_file)
│   └── SinkParquet.json                      # Bronze Parquet sink under bronze/nocs
├── pipeline/
│   ├── srcToBronze.json                      # Metadata-driven CSV -> Parquet ingestion
│   └── DataLakeIngestion.json                # Folder scan with conditional copy
├── DataBricks/
│   ├── Silver.ipynb                          # Athletes and NOCs transformations
│   ├── Array_parameter.ipynb                 # Builds the file list for a looped job task
│   └── Silver_Entries_Coaches&Events.ipynb   # Parameterized CSV -> Delta loader
└── publish_config.json                       # ADF publish settings
```

## Pipelines (Azure Data Factory)

**srcToBronze**
1. `FieldNameLookup` reads `tablename.json` from the `source` container; each entry provides a `file_name`.
2. `FieldNameIterator` (ForEach) runs a Copy activity per file: CSV from `source` to Parquet (snappy) in `bronze/<name>/<name>.parquet`.
3. Incompatible rows are skipped and logged (copy activity log at Warning level) via the data lake linked service.

**DataLakeIngestion**
1. `GetMetaDataFolder` lists the child items of the `source` container.
2. `ForEachFile` checks each item: if it is the file `nocs.csv`, it is copied to `bronze/nocs` as Parquet; otherwise the file name is appended to the `v_file_array` variable.
3. After the loop, `v_file_names` (string of the array) and `v_files_number` (count) are set.

## Notebooks (Azure Databricks)

**Silver.ipynb**
- Reads `athletes.csv` and `nocs.csv` from `/Volumes/parisolympics/dbo/bronze/`.
- NOCs: drops the `country` column, keeps the part of `tag` before `-`, and writes Delta to `silver/nocs`.
- Athletes: casts `height` and `weight` to float, filters `weight > 0`, sorts by height/weight, replaces "United States" with "US" in `nationality`, renames `code` to `athlete_id`, splits `occupation` into an array, selects the core identity/country/physical columns, and writes Delta to `silver/athlete`.
- Also contains an exploratory `fillna` + filter cell that is not written out.

**Array_parameter.ipynb**
- Defines a list of `{source_container, sink_container, csv_file, sink_file}` entries for `events.csv` and `coaches.csv` and publishes it with `dbutils.jobs.taskValues.set(key="my_output", ...)`, so a downstream job task can loop over it.

**Silver_Entries_Coaches&Events.ipynb**
- Takes `source_container`, `sink_container`, `csv_file` and `sink_file` as widgets, reads the CSV with header and inferred schema, and overwrites it as Delta at `sink_container + sink_file`.

## Dataset

Paris Olympics CSV files referenced in the code: `athletes.csv`, `nocs.csv`, `events.csv`, `coaches.csv`. The athletes schema in the ADF datasets includes columns such as `code`, `name`, `gender`, `country_code`, `nationality`, `height`, `weight`, `disciplines`, `events`, `birth_date` and others.

## How to Run

Prerequisites implied by the code:
- An ADLS Gen2 account with `source` and `bronze` containers, and a `tablename.json` file in `source` listing the CSV files to ingest (`[{"file_name": "athletes.csv"}, ...]`).
- An Azure Data Factory instance connected to this repository; update the URL and credentials of `ls_dataLake` for your own storage account.
- A Databricks workspace with a Unity Catalog catalog `parisolympics`, schema `dbo`, and `bronze` / `silver` volumes containing the CSV files.

Steps:
1. Upload the CSV files to the `source` container and run `srcToBronze` (or `DataLakeIngestion`) in ADF.
2. Run `Silver.ipynb` in Databricks for athletes and NOCs.
3. Create a Databricks job: `Array_parameter` task, followed by a For Each task over `{{tasks.<task>.values.my_output}}` that runs `Silver_Entries_Coaches&Events` with the entry fields as parameters.
