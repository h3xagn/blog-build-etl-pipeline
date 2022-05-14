# Building an ETL pipeline from device to cloud

## Blog post series on h3xagn.com

In these series of blogs I covered building an ETL data pipeline using Python. The data is coming from 80 field devices that are installed on mobile equipment. Data is collected using an on-premise FastAPI web server which processes, transforms and uploads the data to Azure. Data is ingested into Azure Data Explorer and then made available to end-users. A seperate Azure Function app also sends data to the device vendor.

Each blog focusses on a specific part of the pipeline to move the data from the field device to the cloud. The code here reflects the latest improvements - see the BONUS blog for more information.

- [Part 1. Building a simple, secure web server](https://h3xagn.com/building-an-etl-pipeline-from-device-to-cloud-part-1/)
- [Part 2. Scaling up the web server with FastAPI](https://h3xagn.com/building-an-etl-pipeline-from-device-to-cloud-part-2/)
- [Part 3. Creating the ELT module as a background worker](https://h3xagn.com/building-an-etl-pipeline-from-device-to-cloud-part-3/)
- [Part 4. Moving data to the Azure Blob storage](https://h3xagn.com/building-an-etl-pipeline-from-device-to-cloud-part-4/)
- [Part 5. Creating a function app to send data to the vendor](https://h3xagn.com/building-an-etl-pipeline-from-device-to-cloud-part-5/)
- [Part 6. Ingesting data into Azure Data Explorer](https://h3xagn.com/building-an-etl-pipeline-from-device-to-cloud-part-6/)
- [BONUS. Improving our data pipeline](https://h3xagn.com/building-an-etl-pipeline-from-device-to-cloud-bonus-part/)

## Repo structure and file descriptions

### 01 Web Server

This folder contains the FastAPI web server receiving data from the field devices, transforming it and uploading it to the cloud.

- `main.py`: FastAPI web server using uvicorn.
- `utils/process_json.py`: Background task to process JSON file, transform data to CSV and upload to Azure Blob Storage.

The dependencies and virtual environment details are located in the `Pipfile` which can be used with `pipenv`.

### 02 Function App

This folder contains the Azure Function app code that is triggered by the blob upload and sends data to the vendor.

- `main.py`: The function app that reads the compressed JSON file and sends it to an API endpoint.
- `function.json`: The configuration of the function app which sets the trigger path.
- `requirements.txt`: Contains the dependencies for the function app.

Ensure all other Application Settings are updated in the Azure Portal for the function app.

### 03 Azure Data Explorer

This folder contains the Azure Data Explorer commands to setup ADX for our data and export it to the Data Lake.

- `kusto-commands.kql`: Contains all the commands to setup tables, exports and continuous exports.

Note that for the `factOilData`, the data quality checks are built into the continuous export query. The quality check implemented are:

- removing duplicate records based on `TimeStamp` and `TagName`.
- removing invalid data, i.e. where `Value` is below 0 and above 10 000.
- removing records where `TagName` is empty.

## License

GNU GPL v3

## Author

[Coenraad Pretorius](https://h3xagn.com/coenraad-pretorius/)
