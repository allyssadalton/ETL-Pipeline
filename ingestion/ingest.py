import argparse
import json
import os
import sys
from datetime import datetime
from validation.validator import validate_records

import pandas as pd


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

    ingestion_id = f"INGEST_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}"

    try:
        print(f"Starting ingestion: {ingestion_id}")

        client_config = load_client_config(args.client)
        mapping_config = load_mapping_config(args.client)

        df_raw = read_input_file(args.file, client_config)

        print(f"Client: {client_config['client_id']}")
        print(f"Records read: {len(df_raw)}")

        
        # Next steps 
        # 1. Validation
        # 2. Transformation
        # 3. Storage
        # 4. Metrics & logging

    except Exception as e:
        print(f"Ingestion failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
