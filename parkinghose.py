from faker import Faker
import argparse
import time
from datetime import datetime

from google.cloud import bigquery_storage_v1
from google.cloud.bigquery_storage_v1 import types
from google.cloud.bigquery_storage_v1 import writer
from google.protobuf import descriptor_pb2

import sys

import customer_record_pb2

STREAM_OFFSET=0

# If you update the customer_record.proto protocol buffer definition, run:
#
#   protoc --python_out=. customer_record.proto
#
# from the samples/snippets directory to generate the customer_record_pb2.py module.


def create_row_data(fake):
    
    row = customer_record_pb2.CustomerRecord()
    row.when = fake.date_time().strftime("%Y-%m-%d %H:%M:%S")
    row.name = fake.name()
    row.licenseplate = fake.license_plate()
    return row.SerializeToString()


def bq_init(project_id: str, dataset_id: str, table_id: str):
    """
    This code sample demonstrates how to write records in pending mode
    using the low-level generated client for Python.
    """

    """Create a write stream, write some sample data, and commit the stream."""
    write_client = bigquery_storage_v1.BigQueryWriteClient()
    parent = write_client.table_path(project_id, dataset_id, table_id)
    write_stream = types.WriteStream()

    # When creating the stream, choose the type. Use the PENDING type to wait
    # until the stream is committed before it is visible. See:
    # https://cloud.google.com/bigquery/docs/reference/storage/rpc/google.cloud.bigquery.storage.v1#google.cloud.bigquery.storage.v1.WriteStream.Type
    write_stream.type_ = types.WriteStream.Type.COMMITTED
    write_stream = write_client.create_write_stream(
        parent=parent, write_stream=write_stream
    )
    stream_name = write_stream.name

    # Create a template with fields needed for the first request.
    request_template = types.AppendRowsRequest()

    # The initial request must contain the stream name.
    request_template.write_stream = stream_name

    # So that BigQuery knows how to parse the serialized_rows, generate a
    # protocol buffer representation of your message descriptor.
    proto_schema = types.ProtoSchema()
    proto_descriptor = descriptor_pb2.DescriptorProto()
    customer_record_pb2.CustomerRecord.DESCRIPTOR.CopyToProto(proto_descriptor)
    proto_schema.proto_descriptor = proto_descriptor
    proto_data = types.AppendRowsRequest.ProtoData()
    proto_data.writer_schema = proto_schema
    request_template.proto_rows = proto_data

    # Some stream types support an unbounded number of requests. Construct an
    # AppendRowsStream to send an arbitrary number of requests to a stream.
    append_rows_stream = writer.AppendRowsStream(write_client, request_template)

    print(f"Stream created with name {stream_name}")

    return append_rows_stream


def write_to_stream(append_rows_stream, rowdata):
    global STREAM_OFFSET
    # Create a batch of row data by appending proto2 serialized bytes to the
    # serialized_rows repeated field.

    proto_rows = types.ProtoRows()

    proto_rows.serialized_rows.append(rowdata)

    # Set an offset to allow resuming this stream if the connection breaks.
    # Keep track of which requests the server has acknowledged and resume the
    # stream at the first non-acknowledged message. If the server has already
    # processed a message with that offset, it will return an ALREADY_EXISTS
    # error, which can be safely ignored.
    #
    # The first request must always have an offset of 0.
    request = types.AppendRowsRequest()
    request.offset = STREAM_OFFSET
    proto_data = types.AppendRowsRequest.ProtoData()
    proto_data.rows = proto_rows
    request.proto_rows = proto_data

    response_future_1 = append_rows_stream.send(request)

    STREAM_OFFSET += 1

    print(response_future_1.result())



    # A PENDING type stream must be "finalized" before being committed. No new
    # records can be written to the stream after this method has been called.
    # write_client.finalize_write_stream(name=write_stream.name)

    # Commit the stream you created earlier.
    #batch_commit_write_streams_request = types.BatchCommitWriteStreamsRequest()
    #batch_commit_write_streams_request.parent = parent
    #batch_commit_write_streams_request.write_streams = [write_stream.name]
    #write_client.batch_commit_write_streams(batch_commit_write_streams_request)

    #print(f"Writes to stream: '{write_stream.name}' have been committed.")



def main():
    try:
        fake = Faker()
        parser = argparse.ArgumentParser()
        parser.add_argument("-s", "--sleep", type=int, help="delay between records in ms", default=500)
        parser.add_argument("-p", "--project", type=str, help="GCP Project", required=True)
        parser.add_argument("-d", "--dataset", type=str, help="BQ Dataset id", required=True)
        parser.add_argument("-t", "--table", type=str, help="BQ Table id", required=True)
        args = parser.parse_args()

        print(f"Delay set to {args.sleep}")
        print(f"Project is {args.project}. Dataset is {args.dataset}. Table is {args.table}")

        append_rows_stream = bq_init(args.project, args.dataset, args.table)
        
        while time.sleep(args.sleep/1000) is None:
            start= datetime.now()
            rowdata = create_row_data(fake)
            write_to_stream(append_rows_stream, rowdata)
            diff = datetime.now() - start
            print(f'{datetime.now().strftime("%Y-%m-%d %H:%M:%S:%f")} | Write took {diff.total_seconds()*1000}ms.')

        # Shutdown background threads and close the streaming connection.
        append_rows_stream.close()

    except BaseException():        
        tb = sys.exc_info()[2]
        print(tb)
        # Shutdown background threads and close the streaming connection.
        append_rows_stream.close()
        print("Stream closed.")


if __name__ == "__main__":
    main()



    