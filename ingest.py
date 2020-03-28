import argparse
import csv
import logging
import os
import re 

import apache_beam as beam
from apache_beam.io.gcp import bigquery
from apache_beam.io.gcp.bigquery import parse_table_schema_from_json
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.pvalue import AsDict
from datetime import datetime

#VAR
SCHEMA = "id:INTEGER,case_in_country:INTEGER,reporting_date:DATE,summary:STRING,location:STRING,country:STRING,gender:STRING,age:INTEGER,symptom_onset:DATE,If_onset_approximated:INTEGER,hosp_visit_date:DATE,exposure_start:DATE,exposure_end:DATE,visiting_Wuhan:INTEGER,from_Wuhan:INTEGER,death:DATE,recovered:DATE,symptom:STRING,source:STRING,link:STRING"

class DataIngestion():
    def parse_method(self, string_input):
        import csv
        row = list(csv.reader([string_input]))[0]
        ver = ['0','1','2','3','4','5','6','7','8','9','10','11','12']
        for r in range(len(row)):
            if row[r]=='' or row[r]=='NA':
                row[r]=0
            elif '/' in row[r] and row[r][0] in ver:
                splt = row[r].split('/')
                if len(splt[-1])==4 and len(splt[0])==1:
                    row[r] = '0'+row[r]
                elif len(splt[-1])==2:
                    dt_frst = '/'.join(splt[0:2])
                    row[r] = dt_frst+'/'+'20'+splt[-1]
                    if len(row[r].split('/')[0])==1:
                        row[r] = '0'+row[r]
        return row

def trata(row):
    from datetime import datetime
    x = {
    "id":int(row[0]),
    "case_in_country":int(row[1]),
    "reporting_date": "1001-01-01" if row[2]==0 else str(datetime.strptime(row[2], '%m/%d/%Y').date()),
    "summary":str(row[4]),
    "location":str(row[5]),
    "country":str(row[6]),
    "gender":str(row[7]),
    "age": int(float(row[8])),
    "symptom_onset": "1001-01-01" if row[9]==0 else str(datetime.strptime(row[9], '%m/%d/%Y').date()),
    "If_onset_approximated":int(row[10]),
    "hosp_visit_date": "1001-01-01" if row[11]==0 or '' in row[11].split('/') else str(datetime.strptime(row[11], '%m/%d/%Y').date()),
    "exposure_start": "1001-01-01" if row[12]==0 else str(datetime.strptime(row[12], '%m/%d/%Y').date()),
    "exposure_end": "1001-01-01" if row[13]==0 else str(datetime.strptime(row[13], '%m/%d/%Y').date()),
    "visiting_Wuhan":int(row[14]),
    "from_Wuhan":int(row[15]),
    "death":"1001-01-01" if row[16]=='0' or row[16]=='1' else str(datetime.strptime(row[16], '%m/%d/%Y').date()),
    "recovered":"1001-01-01" if row[17]=='0' or row[17]=='1' else str(datetime.strptime(row[17], '%m/%d/%Y').date()),
    "symptom":str(row[18]),
    "source":str(row[19]),
    "link":str(row[20])
    }
    return x

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--input',
        dest='input',
        required=False,
        help='Input',
        default='gs://treinamento_dataflow/file/COVID19_line_list_data.csv'
        #default='gs://treinamento_dataflow/file/teste2.csv'
    )

    parser.add_argument(
        '--output',
        dest='output',
        required=False,
        default='TREINO.COVID'
    )

    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        '--runner=DataflowRunner',
        #'--runner=DirectRunner',
        '--project=dataflow-271218',
        '--staging_location=gs://treinamento_dataflow/staging/stagin_area',
        '--temp_location=gs://treinamento_dataflow/temp/temp_area',
        '--job_name=treinamento-dataflow',
    ])
    data_ingestion = DataIngestion()

    p = beam.Pipeline(options=PipelineOptions(pipeline_args))

    row = (
        p
        |'Read from text' >> beam.io.ReadFromText(known_args.input, skip_header_lines=1)
        |'String to Bigquery Row' >> beam.Map(lambda s: data_ingestion.parse_method(s))
        |'Tratamento' >> beam.Map(trata)
        |'Escrita BigQuery' >> beam.io.WriteToBigQuery(
            known_args.output,
            schema=SCHEMA,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE
        ))
    p.run().wait_until_finish()

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()