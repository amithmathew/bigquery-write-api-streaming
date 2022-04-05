# BigQuery Storage API - Streaming Write example

Map `customer_record.proto` to bigquery table schema used. The code (as it is) expects the following columns -

1. `when` of type `DATETIME` . 
2. `name` of type `STRING` . 
3. `licenseplate` of type `STRING` . 


Run `protoc --python_out=. customer_record.proto` if changing the proto schema.