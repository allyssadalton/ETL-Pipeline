import argparse
import json
import logging
import os
import sys
from datetime import datetime, UTC
from validation.validator import validate_records
from transformation.transformer import transform_records
from analytics.quality_metrics import compute_quality_metrics, print_quality_report
from analytics.reporting import print_business_report
import pandas as pd
from storage.database import create_tables, insert_clean_records, insert_rejected_records


def setup_logging(ingestion_id: str):
    """
    Configure logging to write to both ingestion.log and error.log files.
    """
    # Create logs directory if it doesn't exist
    os.makedirs("logs", exist_ok=True)
    
    # Create logger
    logger = logging.getLogger("ingestion")
    logger.setLevel(logging.DEBUG)
    
    # Ingestion log handler (INFO level)
    ingestion_handler = logging.FileHandler("logs/ingestion.log")
    ingestion_handler.setLevel(logging.INFO)
    ingestion_formatter = logging.Formatter(
        f"[{ingestion_id}] %(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    ingestion_handler.setFormatter(ingestion_formatter)
    
    # Error log handler (ERROR level)
    error_handler = logging.FileHandler("logs/error.log")
    error_handler.setLevel(logging.ERROR)
    error_formatter = logging.Formatter(
        f"[{ingestion_id}] %(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    error_handler.setFormatter(error_formatter)
    
    # Add handlers to logger
    logger.addHandler(ingestion_handler)
    logger.addHandler(error_handler)
    
    return logger


def load_client_config(client_name: str) -> dict:
    """
    Load client configuration JSON from config/clients.
    """
    config_path = os.path.join(
        "config", "clients", f"{client_name}.json"
    )

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Client config not found: {config_path}")

    with open(config_path, "r") as f:
        return json.load(f)


def load_mapping_config(client_name: str) -> dict:
    """
    Load source-to-target mapping for a client.
    """
    mapping_path = os.path.join(
        "config", "mappings", f"{client_name}_mapping.json"
    )

    if not os.path.exists(mapping_path):
        raise FileNotFoundError(f"Mapping config not found: {mapping_path}")

    with open(mapping_path, "r") as f:
        return json.load(f)


def read_input_file(file_path: str, client_config: dict) -> pd.DataFrame:
    """
    Read raw client file into a DataFrame based on client configuration.
    """
    file_format = client_config["file_format"]

    if file_format == "csv":
        return pd.read_csv(
            file_path,
            delimiter=client_config.get("delimiter", ","),
            encoding=client_config.get("encoding", "utf-8")
        )

    raise ValueError(f"Unsupported file format: {file_format}")


def main():
    parser = argparse.ArgumentParser(
        description="Lender Data Ingestion Pipeline"
    )
    parser.add_argument(
        "--client",
        required=True,
        help="Client name (e.g. lender_a)"
    )
    parser.add_argument(
        "--file",
        required=True,
        help="Path to raw input file"
    )

    args = parser.parse_args()

    ingestion_id = f"INGEST_{datetime.now(UTC).strftime('%Y%m%d%H%M%S')}"
    logger = setup_logging(ingestion_id)

    try:
        logger.info(f"Starting ingestion: {ingestion_id}")

        client_config = load_client_config(args.client)
        mapping_config = load_mapping_config(args.client)

        df_raw = read_input_file(args.file, client_config)

        logger.info(f"Client: {client_config['client_id']}")
        logger.info(f"Records read: {len(df_raw)}")

            
            # Next steps # Convert raw dataframe to list of dicts
        records = df_raw.to_dict(orient="records")

        # Load canonical schema
        with open("config/schemas/loan_schema.json", "r") as f:
            loan_schema = json.load(f)

        # Transform records first
        transformed_records = transform_records(
            records,
            mapping_config.get("mapping", mapping_config),
            client_config,
            ingestion_id
        )

        # Validate records after transformation
        from validation.validator import validate_records

        clean_records, rejected_records = validate_records(
            transformed_records,
            loan_schema,
            client_config
        )
        
        create_tables()
        insert_clean_records(clean_records)
        insert_rejected_records(rejected_records)

        logger.info(f"Clean records: {len(clean_records)}")
        logger.info(f"Rejected records: {len(rejected_records)}")

        metrics = compute_quality_metrics(clean_records, rejected_records)
        print_quality_report(metrics)

        print_business_report(clean_records)

    except Exception as e:
        logger.error(f"Ingestion failed: {e}", exc_info=True)
        sys.exit(1)



if __name__ == "__main__":
    main()
