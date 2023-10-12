from src.loading_data.loading_data_functions import *

if __name__ == "__main__":
    logging.basicConfig(filename="loading_data/data_loading_errors.log", level=logging.ERROR)
    schema_name = "films"
    load_all_csv_to_database(schema_name=schema_name)
