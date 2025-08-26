import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions, StandardOptions, SetupOptions
from datetime import datetime
import csv

# --- Parsing + Cleaning helpers ---
class ParseCSV:
    def __call__(self, line: str):
        return next(csv.DictReader([line]))

class CleanCustomer:
    def __call__(self, row):
        if not row:
            return None
        try:
            return {
                "customer_id": int(row["customer_id"]),
                "first_name": row.get("first_name", "").strip().title(),
                "last_name": row.get("last_name", "").strip().title(),
                "email": row.get("email"),
                "signup_date": row.get("signup_date")
            }
        except Exception:
            return None

class CleanTransaction:
    def __call__(self, row):
        if not row:
            return None
        try:
            return {
                "transaction_id": int(row["transaction_id"]),
                "customer_id": int(row["customer_id"]),
                "transaction_ts": row.get("transaction_ts"),
                "amount": float(row["amount"]) if row.get("amount") else None,
                "currency": row.get("currency", "").upper() if row.get("currency") else None
            }
        except Exception:
            return None

# --- BQ Schemas ---
BQ_SCHEMA_CUSTOMERS = (
    'customer_id:INTEGER,first_name:STRING,last_name:STRING,'
    'email:STRING,signup_date:DATE'
)

BQ_SCHEMA_TRANSACTIONS = (
    'transaction_id:INTEGER,customer_id:INTEGER,transaction_ts:TIMESTAMP,'
    'amount:NUMERIC,currency:STRING'
)

def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--project", required=True)
    parser.add_argument("--region", default="us-central1")
    parser.add_argument("--runner", default="DataflowRunner")
    parser.add_argument("--temp_location", required=True)
    parser.add_argument("--staging_location", required=True)
    parser.add_argument("--customers_path", required=True)
    parser.add_argument("--transactions_path", required=True)
    parser.add_argument("--dataset", default="customer_analytics")
    args, beam_args = parser.parse_known_args(argv)

    options = PipelineOptions(beam_args)
    gcp = options.view_as(GoogleCloudOptions)
    gcp.project = args.project
    gcp.region = args.region
    options.view_as(StandardOptions).runner = args.runner
    options.view_as(SetupOptions).save_main_session = True

    customers_table = f"{args.project}:{args.dataset}.customers"
    transactions_table = f"{args.project}:{args.dataset}.transactions"

    with beam.Pipeline(options=options) as p:
        # Customers
        (p
         | "ReadCustomers" >> beam.io.ReadFromText(args.customers_path, skip_header_lines=1)
         | "ParseCust" >> beam.Map(ParseCSV())
         | "CleanCust" >> beam.Map(CleanCustomer())
         | "FilterValidCust" >> beam.Filter(lambda x: x is not None)
         | "WriteCustBQ" >> beam.io.WriteToBigQuery(
                customers_table,
                schema=BQ_SCHEMA_CUSTOMERS,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

        # Transactions
        (p
         | "ReadTx" >> beam.io.ReadFromText(args.transactions_path, skip_header_lines=1)
         | "ParseTx" >> beam.Map(ParseCSV())
         | "CleanTx" >> beam.Map(CleanTransaction())
         | "FilterValidTx" >> beam.Filter(lambda x: x is not None)
         | "WriteTxBQ" >> beam.io.WriteToBigQuery(
                transactions_table,
                schema=BQ_SCHEMA_TRANSACTIONS,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == "__main__":
    run()
