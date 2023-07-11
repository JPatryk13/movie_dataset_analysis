import pandas as pd
import json
from ast import literal_eval
from pathlib import Path


class MakeDataframesFromMovies:

    def __init__(self, df: pd.DataFrame):
        self.df = df

    def create_collections_df(self) -> pd.DataFrame:
        self.df['belongs_to_collection'] = self.df['belongs_to_collection'].fillna('[]').apply(
            lambda x: f'[{x}]' if x != '[]' else x).apply(literal_eval)

        df_to_json = self.df[['film_id', 'belongs_to_collection']].to_json(orient='records')

        btc_df = pd.json_normalize(
            json.loads(df_to_json),
            record_path='belongs_to_collection',
            meta='film_id').drop_duplicates(ignore_index=True)

        btc_df = btc_df.rename(columns={'id': 'collection_id'}).drop_duplicates(ignore_index=True).astype(
            {'collection_id': 'UInt64', 'name': 'string', 'poster_path': 'string', 'backdrop_path': 'string',
             'film_id': 'UInt64'}).sort_values('collection_id')

        self.df = self.df.merge(btc_df, how='left', on='film_id').drop(
            ['belongs_to_collection', 'name', 'poster_path_y', 'backdrop_path'], axis=1)

        btc_df = btc_df.drop('film_id', axis=1).drop_duplicates(ignore_index=True)

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
                {new_index_name: 'category', 'film_id': 'UInt64'})

        else:
            new_index_name = f'{new_index_name}_id'

            normalized_df = normalized_df.rename(columns={'id': new_index_name}).astype(
                {new_index_name: 'UInt64', 'film_id': 'UInt64'})

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
        ).rename(columns={'id': 'keyword_id'}).astype({'film_id': 'UInt64', 'keyword_id': 'UInt64'})

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
