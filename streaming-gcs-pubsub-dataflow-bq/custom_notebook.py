import apache_beam as beam
import json
import logging
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from datetime import datetime


class ProcessMessage(beam.DoFn):
    def process(self, element):
        try:
            decoded = element.decode('utf-8')
            message = json.loads(decoded)

            logging.info(f"Incoming message: {message}")

            bucket = message.get("bucket")
            name = message.get("name")

            if not bucket or not name:
                logging.error(f"Missing fields in message: {message}")
                return

            file_path = f"gs://{bucket}/{name}"
            logging.info(f"Generated file path: {file_path}")

            yield file_path

        except Exception as e:
            logging.error(f"Error parsing Pub/Sub message: {str(e)}")

def parse_transaction(line):
    try:
        field = line.split(",")

        if len(field) < 9:
            logging.error(f"Invalid record: {line}")
            return None

        return {
            "transaction_id": field[0],
            "user_id": field[1],
            "amount": float(field[2]),
            "currency": field[3],
            "transaction_type": field[4],
            "transaction_time": datetime.fromisoformat(field[5].replace("Z", "+00:00")),
            "merchant": field[6],
            "location": field[7],
            "status": field[8]
        }

    except Exception as e:
        logging.error(f"Error parsing line: {line}, Error: {str(e)}")
        return None

def run():
    options = PipelineOptions()
    options.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=options) as p:
        def log_element(x):
            logging.info(f"Raw PubSub: {x}")
            return x

        file_paths = (
            p
            | "Read from PubSub" >> beam.io.ReadFromPubSub(
                topic="projects/eighth-azimuth-472115-b8/topics/gcs-file-topic"
            )
            | "Debug Raw Message" >> beam.Map(log_element)
            | "Parse Message" >> beam.ParDo(ProcessMessage())
            )

        lines = (
            file_paths
            | "Read Files" >> beam.io.ReadAllFromText()
            | "Debug File Content" >> beam.Map(log_element)
        )

        (
            lines
            | "Parse Lines" >> beam.Map(parse_transaction)
            | "Filter Valid Rows" >> beam.Filter(lambda x: x is not None)
            | "Write to BigQuery" >> beam.io.WriteToBigQuery(
                table="eighth-azimuth-472115-b8:myDataSet.user_transaction",
                schema="""
                transaction_id:STRING,
                user_id:STRING,
                amount:FLOAT,
                currency:STRING,
                transaction_type:STRING,
                transaction_time:TIMESTAMP,
                merchant:STRING,
                location:STRING,
                status:STRING
                """,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )


if __name__ == "__main__":
    run()