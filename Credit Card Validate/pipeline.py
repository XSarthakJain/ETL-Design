import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import time
from apache_beam.io import ReadFromText
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io import ReadFromPubSub
import os
import logging
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/HP/Downloads/<KEY>.json"
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import logging
from apache_beam import pvalue

options = PipelineOptions(streaming=True, project=<PROJECT_ID>)

#Tags
process_tag = 'process'
fail_tag = 'fail'

#Define BigQuery Schema
table_spec = '<PROJECT_ID>:myDataSet.credit_card_transactions'


schema = (
    'transaction_id:STRING, event_timestamp:TIMESTAMP, Card_Number:STRING, '
    'CVV:INTEGER, ExpiryDate:STRING, Issued_Date:STRING, '
    'Credit_Card_Name:STRING, First_Name:STRING, Last_Name:STRING, '
    'IssuerName:STRING, CardType:STRING,is_luhn_valid:BOOLEAN'
)

#Bad Records
table_spec_bad = '<PROJECT_ID>:myDataSet.bad_records'
bad_records_schema = (
    'transaction_id:STRING, event_timestamp:TIMESTAMP, Card_Number:STRING, '
    'CVV:STRING, ExpiryDate:STRING, Issued_Date:STRING, '
    'Credit_Card_Name:STRING, First_Name:STRING, Last_Name:STRING, '
    'IssuerName:STRING, CardType:STRING, card_valid:BOOLEAN, error:STRING'
)
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
        except Exception as e:                         
            logging.error(f"Getting Error {e}")


class parseData(beam.DoFn):
	def process(self,ele):
		try:
			transaction_id,event_timestamp,Card_Number,CVV,ExpiryDate,Issued_Date,Credit_Card_Name,First_Name,Last_Name,IssuerName,CardType = ele.split(",")
			if Card_Number and CVV and ExpiryDate:
				yield pvalue.TaggedOutput(process_tag,{"transaction_id": transaction_id,"event_timestamp": event_timestamp,"Card_Number": Card_Number,"CVV": CVV,"ExpiryDate": ExpiryDate,"Issued_Date": Issued_Date,"Credit_Card_Name": Credit_Card_Name,"First_Name": First_Name,"Last_Name": Last_Name,"IssuerName": IssuerName,"CardType": CardType})
		except Exception as e:
			ele['error'] = f'parse data is not completed {e}'
			logging.error(f'Parse Operation Can not be possible {e}')
			yield pvalue.TaggedOutput(fail_tag,ele)

class luhn(beam.DoFn):
	def process(self,ele1):
		Card_Number = ele1['Card_Number']
		ele = str(Card_Number)[::-1]
		result = 0
		ele = str(ele).strip().replace(" ", "").replace("-", "")
		item = ele
		if not item.isdigit():
			logging.info(f'Invalid Card {Card_Number}')
			ele1['error'] = f"Card Number is not Valid because its AlphaNumaric {ele1}"
			yield pvalue.TaggedOutput(fail_tag,ele1)
		for index,item in enumerate(ele):
			item = int(item)
			if index%2!=0:
				item1 = item*2-9 if item*2>9 else item*2
				item = item1
			result += item
		if result%10==0:
			logging.info(f'Valid Card {Card_Number}')
			ele1['error'] = f"Card Number is not Valid {ele1}"
			yield pvalue.TaggedOutput(process_tag,ele1)
		else:
			logging.error(f'Invalid Card {Card_Number}')
			ele1['error'] = f"Card Number is not Valid {ele1}"
			yield pvalue.TaggedOutput(fail_tag,ele1)

class isIssuerName(beam.DoFn):
	def process(self,ele,issuername):
		try:
			if ele['IssuerName'] in issuername and bool(issuername[ele['IssuerName']]):
				yield pvalue.TaggedOutput(process_tag,ele)
		except Exception as e:
			logging.error(f"IssuerName is not Correct {e}")
			ele['error'] = f"IssuerName is not Valid {e}"
			yield pvalue.TaggedOutput(fail_tag,ele)

class isCardType(beam.DoFn):
	def process(self,ele,cardtype):
		try:
			if ele['CardType'] in cardtype and bool(cardtype[ele['CardType']]):
				yield pvalue.TaggedOutput(process_tag,ele)
		except Exception as e:
			logging.error(f"Card Type is not Correct {e}")
			ele['error'] = f"Card Type is Not Valid {e}"
			yield pvalue.TaggedOutput(fail_tag,ele)


def format_bad_record(ele):
    return {
        "transaction_id": ele['transaction_id'],
        "event_timestamp": ele['event_timestamp'],
        "Card_Number": ele['Card_Number'],
        "CVV": ele['CVV'],
        "ExpiryDate": ele['ExpiryDate'],
        "Issued_Date": ele['Issued_Date'],
        "Credit_Card_Name": ele['Credit_Card_Name'],
        "First_Name": ele['First_Name'],
        "Last_Name": ele['Last_Name'],
        "IssuerName": ele['IssuerName'],
        "CardType": ele['CardType'],
        "card_valid": False,
        "error": ele['error']
    }

with beam.Pipeline(options=options) as P:

	source = P|'Source Data'>> beam.io.ReadFromPubSub(subscription="projects/<PROJECT_ID>/subscriptions/creditCard")
	#Read IssuerName Side Input
	issuerName = P|'Issuer Name'>>beam.Create(['issuerName.csv'])
	issuerName_t1 = issuerName|"Read Issuer Name Data">>beam.io.ReadAllFromText(skip_header_lines=1)
	issuerName_t2 = issuerName_t1|"Transform in Key Value">>beam.Map(lambda e: (e.split(',')[0],e.split(',')[1]))

	#Read CardType Side Input
	cardType = P|'Card Type'>>beam.Create(['cardtype.csv'])
	cardtype_t1 = cardType|'Read Card Type Data'>> beam.io.ReadAllFromText(skip_header_lines=1)
	cardtype_t2 = cardtype_t1|"Transform in Key Value for CardType">>beam.Map(lambda e: (e.split(',')[0],e.split(',')[1]))


	p1 = source | 'Get PubSub CSV' >> beam.ParDo(getBucket())
	p2 = p1 | 'Read CSV' >> beam.io.ReadAllFromText()
	p3 = p2 |'Parse Data' >> beam.ParDo(parseData()).with_outputs(process_tag,fail_tag)

	#Validate IssuerName
	p4 = p3[process_tag]|'Validate IssuerName'>>beam.ParDo(isIssuerName(),issuername = beam.pvalue.AsDict(issuerName_t2)).with_outputs(process_tag,fail_tag)
	p5 = p4[process_tag]|'Validate CardType' >> beam.ParDo(isCardType(),cardtype=beam.pvalue.AsDict(cardtype_t2)).with_outputs(process_tag,fail_tag)

	#Validate Card Number
	p6 = p5[process_tag]|'validate Card Number'>> beam.ParDo(luhn()).with_outputs(process_tag,fail_tag)

	p6[process_tag]|'print'>>beam.Map(print)

	#Sucess Records
	bigquery_format_sucess = p6[process_tag]|'BigQuery Format'>>beam.Map(lambda ele:{"transaction_id": ele['transaction_id'],"event_timestamp": ele['event_timestamp'],"Card_Number": ele['Card_Number'],"CVV": ele['CVV'],"ExpiryDate": ele['ExpiryDate'],"Issued_Date": ele['Issued_Date'],"Credit_Card_Name": ele['Credit_Card_Name'],"First_Name": ele['First_Name'],"Last_Name": ele['Last_Name'],"IssuerName": ele['IssuerName'],"CardType": ele['CardType'],"is_luhn_valid":True})
	bigquery_format_sucess | 'Inserting Sucess Data in BigQuery'>>  WriteToBigQuery(table=table_spec,schema=schema,write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)

	#Bad Records
	all_bad_records = (
		(p3[fail_tag], p4[fail_tag], p5[fail_tag], p6[fail_tag]) 
		| 'Flatten Bad Records' >> beam.Flatten()
		| 'BigQuery Format Bad' >> beam.Map(format_bad_record)
		)
	all_bad_records | 'Inserting Bad Data in BigQuery1'>>  WriteToBigQuery(table=table_spec_bad,schema=bad_records_schema,write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)