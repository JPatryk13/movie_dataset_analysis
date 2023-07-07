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
        self.df = self.df.drop_duplicates(ignore_index=True)  # .set_index('film_id').sort_index()
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
            {'adult': 'bool', 'budget': 'Int64', 'film_id': 'Int64', 'original_language': 'category',
             'revenue': 'Int64', 'runtime': 'Int64', 'status': 'category', 'video': 'bool'})
        return None


class CleanCast:

    def __init__(self):
        archive_path = Path(__file__).resolve().parent.parent.parent / "dataset" / "archive"

        self.df = pd.read_csv(archive_path / "credits.csv", low_memory=False).drop_duplicates(
            ignore_index=True).rename(columns={'id': 'film_id'}).astype({'film_id': 'Int64'})[['film_id', 'cast']]

    def clean_cast_df(self):
        self.df['cast'] = self.df['cast'].apply(literal_eval)

        df_to_json = self.df[['film_id', 'cast']].to_json(orient='records')

        normalized_df = pd.json_normalize(
            json.loads(df_to_json),
            record_path='cast',
            meta='film_id',
        )

        print(normalized_df)

        return None

    def get_cast_df(self):
        # self.df = self.df.drop_duplicates(ignore_index=True)  # .set_index('film_id').sort_index()
        return self.df


#### napisać funkcję zwracającą czysty movies_df
if __name__ == "__main__":
    ### cleaning movies ###
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    # cmm = CleanMoviesMetadata()
    # wrong_id_list = cmm.list_faulty_ids()
    #
    # cmm.drop_unnecessary_columns(columns=['popularity', 'vote_average', 'vote_count'])
    # cmm.drop_faulty_ids(wrong_ids=wrong_id_list)
    # cmm.data_types_conversion()
    #
    # df = cmm.get_movies_df()

    # print(df)

    ### cleaning cast ###

    cc = CleanCast()

    cc.clean_cast_df()

    cast_df = cc.get_cast_df()

    # print(cast_df.dtypes)
