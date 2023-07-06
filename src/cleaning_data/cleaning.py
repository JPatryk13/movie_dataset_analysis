import pandas as pd
from pathlib import Path


class CleanMoviesMetadata:

    def __init__(self):
        archive_path = Path(__file__).resolve().parent.parent.parent / "dataset" / "archive"

        self.df = pd.read_csv(
            archive_path / "movies_metadata.csv",
            low_memory=False
        ).drop_duplicates(ignore_index=True)

    def get_main_df(self):
        return self.df

    def drop_unnecessary_columns(self, columns: list | str) -> pd.DataFrame:
        self.df.drop(columns, axis=1, inplace=True)
        return self.df


#### odpalamy
if __name__ == "__main__":
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    cmm = CleanMoviesMetadata()

    # print(cmm.get_main_df())
    print(cmm.drop_unnecessary_columns(columns=['popularity', 'vote_average', 'vote_count']))
