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

    def get_cast_df(self):
        self.df = self.df.drop_duplicates(ignore_index=True)  # .set_index('film_id').sort_index()
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
        (1754529, 336222), (572462, 74842), (1439244, 72054), (1746196, 75282), (1310867, 130993), (1523166, 364088),
        (1643037, 377290), (9409, 168098), (1565685, 190326), (1600262, 391088), (1600263, 391088), (1600273, 391088),
        (1650174, 339419), (1799284, 381058), (1896758, 29136), (1705836, 422874), (1711706, 426958), (1711707, 426958),
        (1711708, 426958), (1797846, 121856), (1669701, 266882), (1558443, 376003), (1772899, 430128),
        (1693152, 411024)]

    person_ids = [(1216756, 113387), (1127849, 33608), (179942, 1216483), (1234191, 932097), (1070406, 555778)]

    clean_names = [
        (9779, 'Morris Chestnut'), (23764, 'Erika Eleniak'), (58646, 'Damian Chapa'), (1091312, 'Russ Clark'),
        (235722, 'Haruo Nakajima'), (85775, 'Dick Pinner'), (115772, 'Charles Tannen'), (87637, 'Masami Nagasawa'),
        (117642, 'Jason Momoa'), (67212, 'Tom Wu'), (78456, 'Kimbo Slice'), (87662, 'Issei Takahashi'),
        (74947, 'Kitty Zhang Yuqi'), (932764, 'Jung In-Sun'), (111690, 'Takako Matsu'), (132233, 'Tony Schiena'),
        (78809, 'Rashad Evans'), (99692, 'Liao Fan'), (72932, 'Ryuhei Matsuda'), (555778, 'Pongsatorn Jongwilak'),
        (932097, 'Celestina Aladekoba')]

    clean_gender = [
        (1785844, 2), (78809, 2), (70883, 1), (191752, 1), (1216483, 1), (1104340, 1), (262075, 1), (4644, 1),
        (47395, 2), (1608740, 2), (555778, 2), (17199, 2), (935841, 2), (1135277, 2), (231784, 2), (114733, 2)]

    clean_profile_path = [
        (6718, '/fN9LyZnztco8Tk7NpV0JiGnFkIy.jpg'), (8241, '/vsT0YQb3LSBH4nkUiWjx1ogSIXM.jpg'),
        (3363, '/kI8j8WGjDay9ovWnoTrqvcKYtN3.jpg'), (136195, '/6yVzZc5XAoaVBZ4JcK15NKBeGlH.jpg'),
        (7682, '/yzh26eyKJBtORjLlz8nyLQT7C1d.jpg'), (936, '/we10IusqRV1NHRtA68ftyck2N33.jpg'),
        (1634622, '/xKFZXYUGZGZdiJSJOtEKhvMo7K2.jpg'), (70883, '/sofDsjadyHeRiafPclN4i6TChir.jpg'),
        (1200780, '/7Yn7BVgep48QNAf5cAjzMQl0a13.jpg'), (1608740, '/cXb0xksR2SmuJEoUCOIr7NyBZRt.jpg'),
        (29368, '/greQI4q5Cbpy0SUkZCHZ1tuaaAe.jpg'), (37028, '/rkNC80zwfLwDAb23eXZuyZO0JOO.jpg'),
        (115647, '/uNwvMOGvFhQKWqt0rYuT48KRxac.jpg'), (99888, '/2ppMruL72FDz5Q0xIcGaFxwCDI6.jpg'),
        (29986, '/88sD6UHbUsCuur3r0x1PXToFx6q.jpg'), (1135277, '/prcO9VZBcQS4FhxATbVzDr1005A.jpg'),
        (145092, '/fmR9dO5cxrcwk5bjENU4eNITgLi.jpg'), (56843, '/tE7JkSuyv2N8vtEN0YypsCAxW9B.jpg'),
        (7547, '/gvQKaVUjfmi8fnWIRKJbwd8UfRc.jpg'), (228167, '/aPIAkjn75D1iYsgFJMvZ62WvMOx.jpg'),
        (51641, '/pk3xrVZWlu0Adrkqwp61sojZ239.jpg'), (111690, '/e1vaEiwuglqOYZXvbCX4KLgALeI.jpg'),
        (1331602, '/mkgLRMZHgQQ4PM8KbidOACnorSc.jpg'), (70013, '/7CTTjm8Wh2SKH6Cb4RIpNjjn1zn.jpg'),
        (212225, '/mzeEB6ZDAE036Z2oh2ZIqzrzVic.jpg'), (128045, '/v8h4Ypnu6YVHKER3R4u3THdGAq.jpg'),
        (104018, '/yJk1tsIhEYhlJkDtcvhZIrwbToK.jpg'), (37430, '/ls9gOUs7nSZyutuL5MULC1PZftN.jpg'),
        (52701, '/dGAnHPYdQJ9FjddU855aJ7xjKwS.jpg'), (161310, '/r3nMsFTaVkQ7XPncbvrJBVbqhUa.jpg'),
        (139567, '/mYGbPBzjepkpjWDbUzVhXiFGrFe.jpg'), (1153503, '/byMVHHhtaMvLFSThKKhuqAHOOIl.jpg'),
        (145087, '/4WKsWWGFv8FEL4aowzthMT8RqCH.jpg'), (145086, '/tSFiZ95afNa64EsxPVXteRrwPJz.jpg'),
        (932097, '/vzSaw247wxsUKYoQa647pUeMKaQ.jpg'), (231784, '/tG2MNITHmcJ0RsB8sApGZ9zrIcT.jpg'),
        (1104340, '/8wdFxKcaBx98sIXOZUAhelQuv7M.jpg'), (262075, '/gT8xNBN2xhP2JqN44qklXqUshHK.jpg'),
        (1747946, '/9DfqsBLKvxCgMRSprlo2isMFpV5.jpg'), (1747947, '/29RUAACDGXopRfJ78d41RctDIf0.jpg'),
        (555778, '/AqOS0w0QhrFsgVxaaO1vV8v8MfS.jpg'), (1301102, None), (990654, None), (976019, None), (131606, None),
        (131605, None), (107221, None), (88471, None)]

    cc.drop_faulty_ids(wrong_ids=person_and_film_ids)

    cc.replace_wrong_ids(person_ids)

    cc.replace_null_fields()

    cc.replace_values_in_column(col_name='name', id_and_value=clean_names)

    cc.replace_values_in_column(col_name='gender', id_and_value=clean_gender)

    cc.replace_values_in_column(col_name='profile_path', id_and_value=clean_profile_path)

    cc.data_types_conversion()

    cast_df = cc.get_cast_df()
