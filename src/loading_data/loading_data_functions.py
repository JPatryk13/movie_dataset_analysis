import psycopg2
import logging
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine
from datatypes_to_open import *

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

archive_path = Path(__file__).resolve().parent.parent.parent / "dataset" / "cleaned"


def open_one_dataset(df_name: str, index_name: str | list = None) -> pd.DataFrame:
    df = pd.read_csv(archive_path / df_name, low_memory=False, dtype=all_dtypes, index_col=index_name,
                     keep_default_na=False, na_values='')

    return df


def open_all_datasets(dfs_list: list) -> dict:
    all_dfs = {}
    for df_name in dfs_list:
        df = pd.read_csv(archive_path / df_name, low_memory=False, dtype=all_dtypes, keep_default_na=False,
                         na_values='')

        all_dfs[df_name] = df
    return all_dfs


def load_one_df_to_database(df: pd.DataFrame, schema_name: str, table_name: str) -> bool:
    try:
        # Connect to the PostgreSQL database
        engine = create_engine('postgresql://postgres:admin@localhost:5432/films_analyse_dissertation')

        # Insert the DataFrame into the PostgreSQL table
        df.to_sql(table_name, engine, schema=schema_name, if_exists='append', chunksize=100000, index=False)

        # Close the database connection
        engine.dispose()
        return True
    except Exception as e:
        print(f"An error occurred while loading the DataFrame: {str(e)}")
        return False


def load_data_to_postgres(file_path, table_name, conn):
    try:
        with conn.cursor() as cur:
            # Use the COPY command to load data into the table efficiently
            copy_sql = f"COPY {table_name} FROM STDIN WITH (FORMAT CSV, HEADER, DELIMITER ',', NULL '');"
            with open(file_path, 'r', encoding='utf-8') as f:
                cur.copy_expert(copy_sql, f)

            # Commit the changes
            conn.commit()

    except Exception as e:
        # Log the error details, including the file path and the error message
        logging.error(f"An error occurred while loading CSV file: {file_path}, Error: {str(e)}")
        raise  # Reraise the exception to halt the loading process


def load_all_csv_to_database(schema_name: str) -> bool:
    try:
        # Connect to the local PostgreSQL database
        conn = psycopg2.connect(
            dbname="films_analyse_dissertation",
            user="postgres",
            password="admin",
            host="localhost",
            port="5432"
        )

        # Create a Path object for the CSV directory
        csv_dir = Path(__file__).resolve().parent.parent.parent / "dataset" / "cleaned"

        # Loop through CSV files in the directory and load each file to its corresponding table
        for table_name in loading_order:
            csv_file = csv_dir / table_name
            try:
                load_data_to_postgres(csv_file, f"{schema_name}.{table_name}", conn)
            except Exception as e:
                # Handle the exception (e.g., log the error, skip the file, or take appropriate action)
                print(f"Error while loading table '{table_name}': {str(e)}")
                continue

        # Close the database connection
        conn.close()
        return True

    except Exception as e:
        print(f"An error occurred while loading the CSV files: {str(e)}")
        return False


if __name__ == "__main__":
    logging.basicConfig(filename="data_loading_errors.log", level=logging.ERROR)
    schema_name = "films"
    load_all_csv_to_database(schema_name=schema_name)