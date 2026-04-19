import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows
import time
from apache_beam.io import ReadFromText
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io import ReadFromPubSub
import os
import logging
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/HP/Downloads/<Key File>.json"
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, Repeatedly, AccumulationMode
options = PipelineOptions(streaming=True, project="<Project Name>")


# 🔹 Step 1: Process PubSub Message → Get GCS File Path
class ProcessMessage(beam.DoFn):
    def process(self, element):
        try:
            message = json.loads(element.decode('utf-8'))
            bucket = message.get("bucket")
            name = message.get("name")

            if bucket and name:
                file_path = f"gs://{bucket}/{name}"
                logging.info(f"File Path: {file_path}")
                yield file_path

        except Exception as e:
            logging.error(f"Error parsing PubSub message: {e}")

def parse_data(line):
    # 1. Parse Data

    date_str, ticker, price, volume = line.split(",")


    # Skip if this is the header row
    if date_str != "timestamp": 
        # 2. Assign event time, Converting date string to unix timestamp
        unix_time = time.mktime(time.strptime(date_str, "%Y-%m-%d %H:%M:%S"))
        return unix_time, (ticker, float(price))

class AddTimestampDoFn(beam.DoFn):
    def process(self, element):
        unix_time, data = element
        # Wrap the data in a TimestampedValue so the Window transform sees it
        yield beam.window.TimestampedValue(data, unix_time)

# Main Pipeline
with beam.Pipeline(options=options) as p:

    data = p | 'read data' >> beam.io.ReadFromPubSub(subscription="projects/<Project Name>/subscriptions/subscription")
    
    #Get Bucket and CSV Name
    filname = data | 'CSV Name' >> beam.ParDo(ProcessMessage())


    #Get Data From Bucket
    data1 = filname|'Data Extraction'>>beam.io.ReadAllFromText()
    #data1|'display'>>beam.Map(print)

    # Parse data
    parsed_pcoll = data1 | 'parse data' >> beam.Map(parse_data)

    #Eliminate None Values
    filter_data = parsed_pcoll | "Drop Nones" >> beam.Filter(lambda x: x is not None)


    # Distribute Timestamps
    distribute_timestamp = filter_data | "Distribute Timestamps" >> beam.ParDo(AddTimestampDoFn())
    # Apply Windowing
    fixed_window = (
            distribute_timestamp 
            | "Apply Fixed Window" >> beam.WindowInto(
                FixedWindows(60),
                trigger=Repeatedly(AfterProcessingTime(1)), 
                accumulation_mode=AccumulationMode.ACCUMULATING  # <--- Fix is here
            )
        )

    #fixed_window | 'fixed_window Data' >> beam.Map(print)
    # Aggregation
    agg_data = fixed_window | 'Aggregation' >> beam.CombinePerKey(beam.combiners.MeanCombineFn())
    
    # Display data (optional for debugging)
    agg_data | "Display_data" >> beam.Map(print)

    # Step H: Format for BigQuery
    # IMPORTANT: Dictionary keys MUST match the BigQuery schema names below
    bigquery_format = (
        agg_data 
        | "Format BQ" >> beam.Map(lambda x: {
            'Ticker': x[0], 
            'Avg_Price': x[1]
        })
    )

    #bigquery_format | "Displdddday" >>beam.Map(print)

    # Step I: Write to BigQuery
    # Fixed the 'result =' indentation and alignment
    bigquery_format | "Write to BQ" >> WriteToBigQuery(
            table="<Project Name>:myDataSet.Ticker_Price_agg",
            schema="Ticker:STRING,Avg_Price:FLOAT",
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
        )