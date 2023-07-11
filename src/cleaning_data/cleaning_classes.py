import pandas as pd
import json
from pathlib import Path
from ast import literal_eval


class CleanMoviesMetadata:

    def __init__(self):
        archive_path = Path(__file__).resolve().parent.parent.parent / "dataset" / "archive"

        self.df = pd.read_csv(
            archive_path / "movies_metadata.csv",
            low_memory=False
        ).drop_duplicates(ignore_index=True).rename(columns={'id': 'film_id'})

    def get_movies_df(self):
        self.df = self.df.drop_duplicates(ignore_index=True)  # .add_index('film_id').sort_index()
        return self.df

    def drop_unnecessary_columns(self, columns: list | str) -> None:
        self.df.drop(columns, axis=1, inplace=True)
        return None

    def list_faulty_ids(self) -> list:
        wrong_ids = [film_id for film_id in self.df['film_id'] if not film_id.isdigit()]
        return wrong_ids

    def drop_faulty_ids(self, wrong_ids: list) -> None:
        for film_id in wrong_ids:
            self.df.drop(self.df.loc[self.df['film_id'] == film_id].index, inplace=True)
        return None

    def data_types_conversion(self):
        self.df['release_date'] = pd.to_datetime(self.df['release_date']).dt.date

        self.df = self.df.astype(
            {'adult': 'bool', 'budget': 'Int64', 'film_id': 'UInt64', 'original_language': 'category',
             'revenue': 'Int64', 'runtime': 'UInt64', 'status': 'category', 'video': 'bool'})
        return None


class CleanCast:

    def __init__(self):
        archive_path = Path(__file__).resolve().parent.parent.parent / "dataset" / "archive"

        self.df = pd.read_csv(archive_path / "credits.csv", low_memory=False).drop_duplicates(
            ignore_index=True).rename(columns={'id': 'film_id'}).astype({'film_id': 'Int64'})[['film_id', 'cast']]

    def make_raw_dataframe(self, col_name: str) -> pd.DataFrame:
        self.df[col_name] = self.df[col_name].apply(literal_eval)

        df_to_json = self.df[['film_id', col_name]].to_json(orient='records')

        self.df = pd.json_normalize(
            json.loads(df_to_json),
            record_path=col_name,
            meta='film_id').drop_duplicates(ignore_index=True)

        return self.df

    def drop_unnecessary_columns(self, columns: list | str) -> None:
        self.df = self.df.drop(columns, axis=1).drop_duplicates(ignore_index=True)
        return None

    def data_types_conversion(self):
        # zmienić typy danych tam gdzie object na string
        self.df = self.df.rename(columns={'id': 'person_id'}).astype(
            {'character': 'string', 'gender': 'category', 'person_id': 'UInt64', 'name': 'string',
             'profile_path': 'string', 'film_id': 'UInt64'})

        return None

    def drop_faulty_ids(self, wrong_ids: list) -> None:
        for person_id, film_id in wrong_ids:
            self.df.drop(self.df[(self.df['person_id'] == person_id) & (self.df['film_id'] == film_id)].index,
                         inplace=True)
        return None

    def replace_wrong_ids(self, ids_list: list) -> None:
        for wrong_id, correct_id in ids_list:
            self.df.loc[self.df.person_id == wrong_id, 'person_id'] = correct_id
        return None

    def replace_null_fields(self):
        self.df['character'] = self.df['character'].apply(lambda x: 'UNSPECIFIED' if x is None or x == '' else x)
        return None

    def replace_values_in_column(self, col_name: str, id_and_value: list) -> None:
        for id, value in id_and_value:
            if not value:
                value = pd.NA
            self.df.loc[self.df.person_id == id, col_name] = value
        return None

    def get_df(self):
        self.df = self.df.drop_duplicates(ignore_index=True)  # .add_index('film_id').sort_index()
        return self.df


class CleanCrew:

    def __init__(self):
        archive_path = Path(__file__).resolve().parent.parent.parent / "dataset" / "archive"

        self.df = pd.read_csv(archive_path / "credits.csv", low_memory=False).drop_duplicates(
            ignore_index=True).rename(columns={'id': 'film_id'}).astype({'film_id': 'UInt64'})[['film_id', 'crew']]

    def make_raw_dataframe(self, col_name: str) -> pd.DataFrame:
        self.df[col_name] = self.df[col_name].apply(literal_eval)

        df_to_json = self.df[['film_id', col_name]].to_json(orient='records')

        self.df = pd.json_normalize(
            json.loads(df_to_json),
            record_path=col_name,
            meta='film_id').drop_duplicates(ignore_index=True)

        return self.df

    def drop_unnecessary_columns(self, columns: list | str) -> None:
        self.df = self.df.drop(columns, axis=1).drop_duplicates(ignore_index=True)
        return None

    def data_types_conversion(self):
        # zmienić typy danych tam gdzie object na string
        self.df = self.df.rename(columns={'id': 'person_id'}).astype(
            {'department': 'category', 'gender': 'category', 'person_id': 'UInt64', 'job': 'string', 'name': 'string',
             'profile_path': 'string', 'film_id': 'UInt64'})

        return None

    def drop_faulty_ids(self, wrong_ids: list) -> None:
        for person_id in wrong_ids:
            self.df.drop(self.df[self.df['person_id'] == person_id].index, inplace=True)

        return None

    def replace_values_in_column(self, col_name: str, id_and_value: list) -> None:
        for id, value in id_and_value:
            if not value:
                value = pd.NA
            self.df.loc[self.df.person_id == id, col_name] = value
        return None

    def get_df(self):
        self.df = self.df.drop_duplicates(ignore_index=True)
        return self.df
