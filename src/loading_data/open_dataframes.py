import pandas as pd
from pathlib import Path
from datatypes_to_open import *

dfs_list = ['collections_df', 'genres_df', 'genres_movies_junction', 'production_companies_df',
            'companies_movies_junction', 'production_countries_df', 'countries_movies_junction', 'spoken_languages_df',
            'languages_movies_junction', 'movies_df', 'ratings_df', 'keywords_df', 'keywords_movies_junction',
            'cast_df', 'cast_movies_junction', 'crew_df', 'crew_movies_junction', 'departments_df', 'jobs_df',
            'departments_jobs_junction', 'crew_departments_junction', 'people_df']

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)


def open_cleaned_datasets(df_names: str | list):
    archive_path = Path(__file__).resolve().parent.parent.parent / "dataset" / "cleaned"

    df = pd.read_csv(archive_path / df_names, low_memory=False, dtype=collections_dtypes)

    return df


test_df = open_cleaned_datasets('collections_df')

print(test_df)
print(test_df.dtypes)

"""
user_id       int64
movie_id      int64
rating      float64
date         object
time         object
dtype: object
"""
