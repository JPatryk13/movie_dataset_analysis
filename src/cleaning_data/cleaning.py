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


class CleanCast():

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
            {'character': 'string', 'gender': 'category', 'person_id': 'Int64', 'name': 'string',
             'profile_path': 'string', 'film_id': 'Int64'})

        return None

    def drop_faulty_ids(self, wrong_ids: list) -> None:
        for person_id, film_id in wrong_ids:
            self.df.drop(self.df[(self.df['person_id'] == person_id) & (self.df['film_id'] == film_id)].index,
                         inplace=True)
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

    cc.make_raw_dataframe(col_name='cast')

    cc.drop_unnecessary_columns(['cast_id', 'credit_id', 'order'])

    cc.data_types_conversion()

    # lista id do dropnięcia bo to duplikaty
    person_and_film_ids = [
        (1883276, 37975), (1426261, 9061), (1850357, 25066), (1567849, 124834), (1567850, 124834), (1481131, 346),
        (1435997, 11174), (1899341, 17130), (1899342, 17130), (117675, 31678), (1899343, 17130), (1899346, 17130),
        (1868851, 16363), (1868852, 16363), (1868854, 16363), (1868856, 16363), (108340, 15292), (1376089, 16219),
        (1501999, 15263), (159519, 159519), (1802664, 92663), (1802665, 92663), (1802666, 92663), (1802667, 92663),
        (1802668, 92663), (1802669, 92663), (1482690, 3115), (79166, 16074), (1467868, 43837), (159519, 42634),
        (1441480, 3549), (1783300, 13120), (1562895, 77621), (1647890, 25755), (1901802, 14804), (554530, 14236),
        (1518081, 15957), (1518085, 15957), (1812080, 46871), (1473719, 24171), (936529, 83036), (1459893, 252059),
        (1591288, 42150), (1407770, 75311), (1407779, 75311), (1432916, 97038), (107408, 65035), (1739921, 80271),
        (1339651, 43761), (1893131, 104732), (1580364, 190341), (86443, 20695), (1856811, 50767), (1856812, 50767),
        (1849487, 159667), (1671119, 116495), (1593232, 55225), (1593233, 55225), (1593235, 55225), (1067581, 73452),
        (1293262, 123581), (1871506, 42441), (1871507, 42441), (1188353, 23273), (1361042, 290695), (1361043, 290695),
        (1361044, 290695), (1361045, 290695), (1361046, 290695), (109976, 256196), (1465754, 72400), (1726648, 44672),
        (1892600, 322488), (1195489, 122767), (1845953, 31984), (1175351, 59404), (1355223, 256836), (1348207, 116236),
        (1805974, 163428), (1805977, 163428), (1805981, 163428), (1805982, 163428), (1805983, 163428),
        (1754529, 336222), (572462, 74842)]

    cc.drop_faulty_ids(person_and_film_ids)

    cast_df = cc.get_cast_df()

    ### sprawdzanie czy dane są poprawne ###

    # lista id do dropnięcia bo to duplikaty
    print(cast_df.loc[cast_df[['character', 'name', 'film_id']].duplicated(keep=False)].head(15))  # 1428 375
    print(cast_df.loc[cast_df[['character', 'name', 'film_id']].duplicated(keep=False)])

    # print(cast_df.dtypes)
    # print(cast_df.loc[cast_df.index.duplicated(keep=False)])
    # print(cast_df.loc[cast_df.character.duplicated(keep=False)])
    # print(cast_df.loc[cast_df.gender.duplicated(keep=False)])
    # print(cast_df.loc[cast_df.id.duplicated(keep=False)])
    # print(cast_df.loc[cast_df.name.duplicated(keep=False)])
    # print(cast_df.loc[cast_df.profile_path.duplicated(keep=False)])
