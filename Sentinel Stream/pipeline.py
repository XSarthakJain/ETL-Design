import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
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
from apache_beam.transforms.trigger import AfterWatermark, AfterProcessingTime, AfterCount,Repeatedly, AccumulationMode
from apache_beam.transforms.userstate import ReadModifyWriteStateSpec, TimerSpec
from apache_beam.coders import StrUtf8Coder, BooleanCoder
import hashlib
import time
from apache_beam.transforms.userstate import (
    ReadModifyWriteStateSpec, 
    TimerSpec, 
    on_timer
)
options = PipelineOptions(streaming=True, project="<Project_ID>")

class getBucket(beam.DoFn):
    def process(self,ele):
        try:
            data = json.loads(ele.decode('utf-8'))
            bucket = data.get('bucket')
            name = data.get('name')
            if bucket and name:
                file_path = f"gs://{bucket}/{name}"
                logging.info(f"Sucessfully get the Bucket name with File{file_path}")
                yield file_path
        except Exception as e:                          # FIX 1
            logging.error(f"Getting Error {e}")


def parse_data(ele):
    try:
        parts = ele.strip().split(",")
        if len(parts) != 4:
            return None
        timestamp,ticker,price,volume = parts
        if timestamp.strip() != 'timestamp':
            return {
                'timestamp': timestamp.strip(),
                'ticker':    ticker.strip(),
                'price':     float(price.strip()),
                'volume':    float(volume.strip())
            }
    except Exception as e:                              # FIX 1
        logging.error(f'Getting error during parse {e}')
    return None


def createFingerPrint(elem):
    try:
        timestamp = elem['timestamp']
        price = elem['price']
        payload = f"{timestamp}_{price}_{elem['ticker']}_{elem['volume']}"
        fingerprint = hashlib.md5(payload.encode()).hexdigest()
        return (fingerprint,elem)
    except Exception as e:                              # FIX 1
        logging.error(f"FingerPrint Creation is not sucessful {e}")


class deduplicate(beam.DoFn):
    ID_STATE = ReadModifyWriteStateSpec('seen_ids', BooleanCoder())
    EXPIRY_TIMER = TimerSpec('expiry', beam.TimeDomain.REAL_TIME)

    def process(self,element_pair,seen_state=beam.DoFn.StateParam(ID_STATE),timer=beam.DoFn.TimerParam(EXPIRY_TIMER)):
        already_seen = seen_state.read() or False

        if not already_seen:
            seen_state.write(True)
            timer.set(time.time()+60)
            fingerprint,data = element_pair
            yield data
        else:
        	print(f"Dropping Duplicate Element {element_pair[1]}")
        	logging.info(f"Dropping Duplicate Element {element_pair[1]}")
            # FIX 2: removed yield None — duplicates silently dropped

    @beam.transforms.userstate.on_timer(EXPIRY_TIMER)
    def expiry_callback(self, seen_state=beam.DoFn.StateParam(ID_STATE)):
        seen_state.clear()


# FIX 3: replaced parse_data1 + AddTimestampDoFn with single to_ticker_price
# ReadAllFromText never advances watermark so FixedWindows never flush
# We stay in GlobalWindow and use AfterCount trigger instead
def to_ticker_price(elem):
    try:
        return (elem['ticker'], elem['price'])
    except Exception as e:
        logging.error(f"to_ticker_price error: {e}")
        return None


# FIX 4: proper streaming-safe combiner replaces builtin sum()
class SumCombineFn(beam.CombineFn):
    def create_accumulator(self):
        return 0.0
    def add_input(self, accumulator, input_val):
        return accumulator + input_val
    def merge_accumulators(self, accumulators):
        return sum(accumulators)
    def extract_output(self, accumulator):
        return round(accumulator, 4)


with beam.Pipeline(options=options) as P:
    p1 = P | 'read data' >> beam.io.ReadFromPubSub(
        subscription="projects/<Project_ID>/subscriptions/DataFlow_Subscriber")

    p2 = p1 | 'Get Bucket Name'     >> beam.ParDo(getBucket())
    p3 = p2 | 'Get Data From Bucket' >> beam.io.ReadAllFromText()
    p4 = p3 | 'Extract data'         >> beam.Map(parse_data)
    p5 = p4 | 'Eliminate NULL'       >> beam.Filter(lambda ele: ele is not None)
    p6 = p5 | 'Create FingerPrint'   >> beam.Map(createFingerPrint)

    p7 = p6 | 'Global Window FOr DeDuplicate Data' >> beam.WindowInto(
        beam.window.GlobalWindows())

    p8 = p7 | 'Eliminate DeDuplicate' >> beam.ParDo(deduplicate())

    # FIX 5: removed Filter Dupes — no longer needed since dedup doesn't yield None
    # Convert dict → (ticker, price) tuple for CombinePerKey
    p9 = p8 | 'To Ticker Price' >> beam.Map(to_ticker_price)
    p10 = p9 | 'Filter Conversion Errors' >> beam.Filter(lambda e: e is not None)

    # FIX 6: GlobalWindows + AfterCount(1) replaces FixedWindows + AfterProcessingTime
    # This guarantees combiner flushes on every element without needing a watermark
    windowed = (
        p10
        | 'Aggregation Window' >> beam.WindowInto(
            beam.window.GlobalWindows(),
            trigger=Repeatedly(AfterProcessingTime(5)),
            accumulation_mode=AccumulationMode.ACCUMULATING
        )
    )

    windowed | 'Display' >> beam.Map(
        lambda e: logging.info(f"Pre-Aggregation: {e}"))

    windowed | 'Display1' >> beam.Map(
        lambda e: print(f"Pre-Aggregation: {e}"))

    # FIX 7: SumCombineFn replaces builtin sum()
    agg_data = windowed | 'Aggregation' >> beam.CombinePerKey(SumCombineFn())

    agg_data | 'Display_data' >> beam.Map(
        lambda e: logging.info(f"RESULT — Ticker: {e}"))

    agg_data | 'Display_data1' >> beam.Map(
    	lambda e: print(f"RESULT — Ticker: {e}"))

    bigquery_format = (
        agg_data 
        | "Format BQ" >> beam.Map(lambda x: {
            'Ticker': x[0], 
            'Avg_Price': x[1]
        })
    )|"Write to BQ" >> WriteToBigQuery(
            table="<Project_ID>:myDataSet.Ticker_Price_agg",
            schema="Ticker:STRING,Avg_Price:FLOAT",
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )