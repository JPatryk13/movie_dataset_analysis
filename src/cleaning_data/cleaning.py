import pandas as pd
from pathlib import Path


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


#### napisać funkcję zwracającą czysty movies_df
if __name__ == "__main__":
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    cmm = CleanMoviesMetadata()
    wrong_id_list = cmm.list_faulty_ids()

    cmm.drop_unnecessary_columns(columns=['popularity', 'vote_average', 'vote_count'])
    cmm.drop_faulty_ids(wrong_ids=wrong_id_list)
    cmm.data_types_conversion()

    df = cmm.get_movies_df()

    print(df)
