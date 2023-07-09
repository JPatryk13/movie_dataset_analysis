import pandas as pd
import json
from tools import nested_dict_col_to_df
from ast import literal_eval
from pathlib import Path
from cleaning import CleanMoviesMetadata, CleanCast, CleanCrew


class MakeDataframesFromMovies:

    def __init__(self, df: pd.DataFrame):
        self.df = df

    def create_collections_df(self) -> pd.DataFrame:
        self.df['belongs_to_collection'] = self.df['belongs_to_collection'].fillna('[]')

        btc_df = nested_dict_col_to_df(self.df['belongs_to_collection'], save_original=True)

        btc_df = btc_df.rename(columns={'id': 'collection_id'}).drop_duplicates(ignore_index=True).astype(
            {'collection_id': 'Int64'}).sort_values('collection_id')  # .add_index('collection_id').sort_index()

        self.df = self.df.merge(
            btc_df,
            how='left',
            left_on='belongs_to_collection',
            right_on='original_value').drop(
            ['belongs_to_collection', 'name', 'poster_path_y', 'backdrop_path', 'original_value'], axis=1)

        btc_df.drop('original_value', axis=1, inplace=True)

        return btc_df

    def extract_df_from_col(self, col_name: str, new_index_name: str = None, category_index: str = None) \
            -> tuple[pd.DataFrame, pd.DataFrame | None]:

        if not new_index_name:
            new_index_name = col_name

        self.df[col_name] = self.df[col_name].fillna('[]').apply(literal_eval)

        df_to_json = self.df[['film_id', col_name]].to_json(orient='records')

        normalized_df = pd.json_normalize(
            json.loads(df_to_json),
            record_path=col_name,
            meta='film_id',
        )

        if category_index:
            normalized_df = normalized_df.rename(columns={category_index: new_index_name}).astype(
                {new_index_name: 'category', 'film_id': 'Int64'})

        else:
            new_index_name = f'{new_index_name}_id'

            normalized_df = normalized_df.rename(columns={'id': new_index_name}).astype(
                {new_index_name: 'Int64', 'film_id': 'Int64'})

        junction_df = normalized_df[['film_id', new_index_name]].sort_values('film_id')

        transformed_df = normalized_df.drop('film_id', axis=1).drop_duplicates(ignore_index=True).sort_values(
            new_index_name)

        return transformed_df, junction_df

    def final_transformation_movies_df(self):
        # drop columns with nested list of dicts
        self.df = self.df.drop(['genres', 'production_companies', 'production_countries', 'spoken_languages'],
                               axis=1).rename(
            columns={'poster_path_x': 'poster_path'})  # .add_index('film_id').sort_index()
        return None

    def get_movies_df(self) -> pd.DataFrame:
        return self.df  # .add_index('film_id').sort_index()


class MakeDataframeFromRatings:

    def __init__(self):
        archive_path = Path(__file__).resolve().parent.parent.parent / "dataset" / "archive"

        self.df = pd.read_csv(archive_path / "ratings.csv", low_memory=False).drop_duplicates(
            ignore_index=True).rename(columns={'userId': 'user_id', 'movieId': 'movie_id'}).sort_values('user_id')

    def extract_date_and_time(self):
        # convert timestamp column to date time object representing date and time
        self.df['timestamp'] = pd.to_datetime(self.df['timestamp'], unit='s')

        self.df['date'] = self.df['timestamp'].dt.date
        self.df['time'] = self.df['timestamp'].dt.time

        self.df.drop('timestamp', axis=1, inplace=True)

        return None

    def get_ratings_df(self) -> pd.DataFrame:
        return self.df


class MakeDataframeFromKeywords:

    def __init__(self):
        archive_path = Path(__file__).resolve().parent.parent.parent / "dataset" / "archive"

        self.df = pd.read_csv(archive_path / "keywords.csv", low_memory=False).drop_duplicates(ignore_index=True)

    def extract_df_from_col(self):
        self.df['keywords'] = self.df['keywords'].apply(literal_eval)

        df_to_json = self.df.to_json(orient='records')

        normalized_df = pd.json_normalize(
            json.loads(df_to_json),
            record_path='keywords',
            meta='id',
            meta_prefix='film_'
        ).rename(columns={'id': 'keyword_id'}).astype({'film_id': 'Int64', 'keyword_id': 'Int64'})

        transformed_df = normalized_df[['keyword_id', 'name']].drop_duplicates(ignore_index=True).sort_values(
            'keyword_id')

        junction_df = normalized_df[['film_id', 'keyword_id']].sort_values('film_id')

        return transformed_df, junction_df


class MakeDataframesFromCast:

    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.sub_df = pd.DataFrame()

    def create_index_for_cast_data(self) -> pd.DataFrame:
        self.sub_df = self.df.drop('film_id', axis=1).drop_duplicates(ignore_index=True).reset_index(names='cast_id')
        return self.sub_df

    def create_df(self, columns: list, sort_by_index: str = None) -> pd.DataFrame:
        transformed_df = self.sub_df[columns].drop_duplicates(ignore_index=True)

        return transformed_df.sort_values(sort_by_index) if sort_by_index else transformed_df

    def create_cast_movies_junction(self) -> pd.DataFrame:
        junction_df = self.df.merge(
            self.sub_df,
            how='left',
            left_on=['character', 'gender', 'person_id', 'name', 'profile_path'],
            right_on=['character', 'gender', 'person_id', 'name', 'profile_path']
        ).drop(['character', 'gender', 'person_id', 'name', 'profile_path'], axis=1).sort_values('film_id')

        junction_df = junction_df[['film_id', 'cast_id']]

        return junction_df

    def get_df(self) -> pd.DataFrame:
        return self.df


class MakeDataframesFromCrew:

    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.sub_df = pd.DataFrame()

    def create_index_for_crew_data(self) -> pd.DataFrame:
        self.sub_df = self.df.drop('film_id', axis=1).drop_duplicates(ignore_index=True).reset_index(names='crew_id')
        return self.sub_df

    def create_df(self, columns: list, sort_by_index: str = None, add_index: str = None) -> pd.DataFrame:
        transformed_df = self.sub_df[columns].drop_duplicates(ignore_index=True)
        if sort_by_index:
            transformed_df.sort_values(sort_by_index)
        if add_index:
            transformed_df.reset_index(names=add_index, inplace=True)

        return transformed_df

    def create_crew_movies_junction(self) -> pd.DataFrame:
        junction_df = self.df.merge(
            self.sub_df,
            how='left',
            on=['department', 'gender', 'person_id', 'job', 'name', 'profile_path']
        ).drop(['department', 'gender', 'person_id', 'job', 'name', 'profile_path'], axis=1).sort_values('film_id')

        return junction_df

    def create_departments_jobs_junction(self, dept: pd.DataFrame, job: pd.DataFrame) -> pd.DataFrame:
        junction_df = dept.merge(self.sub_df, how='left', on='department').merge(job, how='left', on='job')

        junction_df = junction_df[['department_id', 'job_id']].drop_duplicates(ignore_index=True)

        return junction_df

    def create_crew_departments_junction(self, dept: pd.DataFrame) -> pd.DataFrame:
        junction_df = dept.merge(self.sub_df, how='left', on='department')

        junction_df = junction_df[['crew_id', 'department_id']].drop_duplicates(ignore_index=True).sort_values(
            'crew_id')

        return junction_df

    def get_df(self) -> pd.DataFrame:
        return self.df


if __name__ == "__main__":
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    ### cleaning movies ###
    #
    # cmm = CleanMoviesMetadata()
    # wrong_id_list = cmm.list_faulty_ids()
    #
    # cmm.drop_unnecessary_columns(columns=['popularity', 'vote_average', 'vote_count'])
    # cmm.drop_faulty_ids(wrong_ids=wrong_id_list)
    # cmm.data_types_conversion()
    #
    # clean_movies_df = cmm.get_movies_df()

    ### transformation movies ###
    # mdfm = MakeDataframesFromMovies(clean_movies_df)
    #
    # collections_df = mdfm.make_collections_df()
    #
    # genres_df, genres_movies_junction = mdfm.extract_df_from_col(col_name='genres')
    #
    # production_companies_df, companies_movies_junction = mdfm.extract_df_from_col(
    #     col_name='production_companies',
    #     new_index_name='company')
    #
    # production_companies_df = production_companies_df[['company_id', 'name']]
    #
    # production_countries_df, countries_movies_junction = mdfm.extract_df_from_col(
    #     col_name='production_countries',
    #     new_index_name='country_code',
    #     category_index='iso_3166_1')
    #
    # spoken_languages_df, languages_movies_junction = mdfm.extract_df_from_col(
    #     col_name='spoken_languages',
    #     new_index_name='language_code',
    #     category_index='iso_639_1')
    #
    # mdfm.final_transformation_movies_df()
    #
    # movies_df = mdfm.get_movies_df()

    ### transformation ratings ###

    # mdfr = MakeDataframeFromRatings()
    #
    # mdfr.extract_date_and_time()
    #
    # ratings_df = mdfr.get_ratings_df()

    ### transformation keywords ###

    # mdfk = MakeDataframeFromKeywords()
    #
    # keywords_df, keywords_movies_junction = mdfk.extract_df_from_col()

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
        (555778, '/AqOS0w0QhrFsgVxaaO1vV8v8MfS.jpg'), (56865, '/j1Xktbs6M6kNIwaLUps9swSEdVs.jpg'), (1301102, None),
        (990654, None), (976019, None), (131606, None), (131605, None), (107221, None), (88471, None)]

    cc.drop_faulty_ids(wrong_ids=person_and_film_ids)

    cc.replace_wrong_ids(person_ids)

    cc.replace_null_fields()

    cc.replace_values_in_column(col_name='name', id_and_value=clean_names)

    cc.replace_values_in_column(col_name='gender', id_and_value=clean_gender)

    cc.replace_values_in_column(col_name='profile_path', id_and_value=clean_profile_path)

    cc.data_types_conversion()

    clean_cast_df = cc.get_df()

    ### transformation cast ###

    mdfcast = MakeDataframesFromCast(clean_cast_df)

    mdfcast.create_index_for_cast_data()

    cast_df = mdfcast.create_df(columns=['cast_id', 'character', 'person_id'])

    people_cast = mdfcast.create_df(columns=['person_id', 'gender', 'name', 'profile_path'],
                                    sort_by_index='person_id')

    cast_movies_junction = mdfcast.create_cast_movies_junction()

    ### cleaning crew ###

    ccrew = CleanCrew()

    ccrew.make_raw_dataframe(col_name='crew')

    ccrew.drop_unnecessary_columns(columns='credit_id')

    ccrew.data_types_conversion()

    ids_to_drop = [1339312, 572046]

    clean_gender_crew = [(1551219, 1), (1785844, 2)]

    clean_profile_crew = [(56865, '/j1Xktbs6M6kNIwaLUps9swSEdVs.jpg'), (52701, '/dGAnHPYdQJ9FjddU855aJ7xjKwS.jpg'),
                          (148084, None), (260050, None), (46391, None), (127279, None)]

    clean_name_crew = [(63574, 'Cheung Ka-Fai'), (9779, 'Morris Chestnut'), (117642, 'Jason Momoa')]

    ccrew.drop_faulty_ids(wrong_ids=ids_to_drop)

    ccrew.replace_values_in_column(col_name='gender', id_and_value=clean_gender_crew)
    ccrew.replace_values_in_column(col_name='profile_path', id_and_value=clean_profile_crew)
    ccrew.replace_values_in_column(col_name='name', id_and_value=clean_name_crew)

    clen_crew_df = ccrew.get_df()

    ### transformation crew ###

    mdfcrew = MakeDataframesFromCrew(clen_crew_df)

    mdfcrew.create_index_for_crew_data()

    crew_df = mdfcrew.create_df(columns=['crew_id', 'person_id'])

    crew_movies_junction = mdfcrew.create_crew_movies_junction()

    people_crew = mdfcrew.create_df(columns=['person_id', 'gender', 'name', 'profile_path'], sort_by_index='person_id')

    departments_df = mdfcrew.create_df(columns=['department'], add_index='department_id')

    jobs_df = mdfcrew.create_df(columns=['job'], add_index='job_id')

    departments_jobs_junction = mdfcrew.create_departments_jobs_junction(dept=departments_df, job=jobs_df)

    crew_departments_junction = mdfcrew.create_crew_departments_junction(dept=departments_df)

    ### transformation złączanie people dfów ###

    all_people_df = pd.concat([people_cast, people_crew]).drop_duplicates(
        ignore_index=True).sort_values('person_id')

    print(all_people_df)

    ### printowanie movies dfów ###

    # print(movies_df)
    # print(collections_df)
    # print(genres_df)
    # print(genres_movies_junction)
    # print(production_companies_df)
    # print(companies_movies_junction)
    # print(production_countries_df)
    # print(countries_movies_junction)
    # print(spoken_languages_df)
    # print(languages_movies_junction)

    ### printowanie ratings df ###

    # print(ratings_df)

    ### printowanie keywords df ###

    # print(keywords_df)
    # print(keywords_movies_junction)

    ### printowanie cast df ###

    # print(cast_df)
    # print(people_cast)
    # print(cast_movies_junction)

    ### printowanie crew df ###

    # print(departments_jobs_junction)
    # print(crew_departments_junction)
    # print(jobs_df)
    # print(departments_df)
    # print(people_crew)
    # print(crew_df)
    # print(crew_movies_junction)
