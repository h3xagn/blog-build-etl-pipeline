# Building an ETL pipeline from device to cloud (part 3)

In this part, we will create the Extract, Transform and Load (ETL) module to transform the JSON data into the required CSV format and add it to the web server as a background task.

## Blog post series on h3xagn

In these series of blogs I will be covering building an ETL data pipeline using Python. The data is coming from field devices that are installed on mobile equipment. Each blog focusses on a specific part of the pipeline to move the data from the field device to the cloud.

- [Part 1. Building a simple, secure web server](https://h3xagn.com/building-an-etl-pipeline-from-device-to-cloud-part-1/)
- [Part 2. Scaling up the web server with FastAPI](https://h3xagn.com/building-an-etl-pipeline-from-device-to-cloud-part-2/)
- [Part 3. Creating the ELT module as a background worker](https://h3xagn.com/building-an-etl-pipeline-from-device-to-cloud-part-3/) **(you are here)**
- Part 4. Moving data to the Azure Blob storage
- Part 5. Creating a function app to send data to the vendor
- Part 6. Ingesting data into Azure Data Explorer

## File descriptions

- FastAPI web server: `web-server.py`
- Utilities module: `utils`
  - `process_json.py` contains the methods to save the JSON object locally and transform the data into CSV format.

## How to run the code

Dependencies and virtual environment details are located in the `Pipfile` which can be used with `pipenv`.

## License

GNU GPL v3

## Author

[Coenraad Pretorius](https://h3xagn.com/coenraad-pretorius/)
