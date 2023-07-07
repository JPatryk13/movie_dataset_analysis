import pandas as pd
import json
from tools import nested_dict_col_to_df
from ast import literal_eval
from cleaning import CleanMoviesMetadata


class MakeDataframesFromMoviesFile:
    def __init__(self, df: pd.DataFrame):
        self.df = df

    def make_collections_df(self) -> pd.DataFrame:
        self.df['belongs_to_collection'] = self.df['belongs_to_collection'].fillna('[]')

        btc_df = nested_dict_col_to_df(self.df['belongs_to_collection'], save_original=True)

        btc_df = btc_df.rename(columns={'id': 'collection_id'}).drop_duplicates(ignore_index=True).astype(
            {'collection_id': 'Int64'}).sort_values('collection_id')  # .set_index('collection_id').sort_index()

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
                               axis=1).rename(columns={'poster_path_x': 'poster_path'})  # .set_index('film_id').sort_index()
        return None

    def get_movies_df(self) -> pd.DataFrame:
        return self.df  # .set_index('film_id').sort_index()


if __name__ == "__main__":
    ### cleaning ###
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    cmm = CleanMoviesMetadata()
    wrong_id_list = cmm.list_faulty_ids()

    cmm.drop_unnecessary_columns(columns=['popularity', 'vote_average', 'vote_count'])
    cmm.drop_faulty_ids(wrong_ids=wrong_id_list)
    cmm.data_types_conversion()

    clean_movies = cmm.get_movies_df()

    ### transformation ###
    movies_object = MakeDataframesFromMoviesFile(clean_movies)

    collections_df = movies_object.make_collections_df()

    genres_df, genres_movies_junction = movies_object.extract_df_from_col(col_name='genres')

    production_companies_df, companies_movies_junction = movies_object.extract_df_from_col(
        col_name='production_companies',
        new_index_name='company')

    production_companies_df = production_companies_df[['company_id', 'name']]

    production_countries_df, countries_movies_junction = movies_object.extract_df_from_col(
        col_name='production_countries',
        new_index_name='country_code',
        category_index='iso_3166_1')

    spoken_languages_df, languages_movies_junction = movies_object.extract_df_from_col(
        col_name='spoken_languages',
        new_index_name='language_code',
        category_index='iso_639_1')

    movies_object.final_transformation_movies_df()

    movies_df = movies_object.get_movies_df()

    ### printowanie wszystkich dfów ###

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
