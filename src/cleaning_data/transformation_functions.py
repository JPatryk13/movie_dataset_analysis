from data_transformation_classes import *


def make_dict_from_movies_dfs(clean_movies_df: pd.DataFrame) -> dict:
    """
    Function bla bla bla bla bla -> dodać opis
    :param clean_movies_df: cleaned movies DataFrame object
    :return: dictionary which contain all transformed DataFrames from movies file
            (keys -> name of DataFrame; values -> transformed DataFrame object ready for loading to DB stage)
    """
    dfs_dict = {}

    mdfm = MakeDataframesFromMovies(clean_movies_df)
    dfs_dict['collections'] = mdfm.create_collections_df()

    genres_df, genres_movies_junction = mdfm.extract_df_from_col(col_name='genres', new_index_name='genre')
    dfs_dict['genres'] = genres_df
    dfs_dict['genres_movies'] = genres_movies_junction

    production_companies_df, companies_movies_junction = mdfm.extract_df_from_col(
        col_name='production_companies',
        new_index_name='company')

    production_companies_df = production_companies_df[['company_id', 'name']]
    dfs_dict['companies'] = production_companies_df
    dfs_dict['companies_movies'] = companies_movies_junction

    production_countries_df, countries_movies_junction = mdfm.extract_df_from_col(
        col_name='production_countries',
        new_index_name='country_code',
        category_index='iso_3166_1')
    dfs_dict['countries'] = production_countries_df
    dfs_dict['countries_movies'] = countries_movies_junction

    spoken_languages_df, languages_movies_junction = mdfm.extract_df_from_col(
        col_name='spoken_languages',
        new_index_name='language_code',
        category_index='iso_639_1')
    dfs_dict['languages'] = spoken_languages_df
    dfs_dict['languages_movies'] = languages_movies_junction

    mdfm.final_transformation_movies_df()

    movies_df = mdfm.get_movies_df()
    movies_df.insert(0, 'film_id', movies_df.pop('film_id'))
    dfs_dict['movies'] = movies_df

    return dfs_dict


def transform_ratings(movies_df: pd.DataFrame) -> pd.DataFrame:
    mdfr = MakeDataframeFromRatings()

    mdfr.extract_date_and_time()

    df = mdfr.get_ratings_df()

    transformed_df = df.merge(movies_df, left_on='movie_id', right_on='film_id').drop('film_id', axis=1)

    return transformed_df.sort_values(['user_id', 'movie_id'])


def make_dict_from_keywords_dfs() -> dict:
    dfs_dict = {}

    mdfk = MakeDataframeFromKeywords()

    keywords_df, keywords_movies_junction = mdfk.extract_df_from_col()
    dfs_dict['keywords'] = keywords_df
    dfs_dict['keywords_movies'] = keywords_movies_junction

    return dfs_dict


def make_dict_from_credits_dfs(clean_cast_df: pd.DataFrame, clean_crew_df: pd.DataFrame) -> dict:
    dfs_dict = {}

    # transformation cast
    mdfcast = MakeDataframesFromCast(clean_cast_df)

    mdfcast.create_index_for_cast_data()

    cast_df = mdfcast.create_df(columns=['character_id', 'character', 'person_id'])
    dfs_dict['characters'] = cast_df

    cast_movies_junction = mdfcast.create_cast_movies_junction()
    dfs_dict['characters_movies'] = cast_movies_junction

    # transformation crew
    mdfcrew = MakeDataframesFromCrew(clean_crew_df)

    mdfcrew.create_index_for_crew_data()

    crew_df = mdfcrew.create_df(columns=['crew_id', 'person_id'], index_name='crew_id')
    dfs_dict['crew'] = crew_df

    crew_movies_junction = mdfcrew.create_crew_movies_junction()
    dfs_dict['crew_movies'] = crew_movies_junction

    departments_df = mdfcrew.create_df(columns=['department'], index_name='department_id')

    jobs_df = mdfcrew.create_df(columns=['job'], index_name='job_id')

    departments_jobs_junction = mdfcrew.create_departments_jobs_junction(dept=departments_df, job=jobs_df)
    dfs_dict['departments_jobs'] = departments_jobs_junction

    crew_departments_junction = mdfcrew.create_crew_departments_junction(dept=departments_df)
    dfs_dict['crew_departments'] = crew_departments_junction

    dfs_dict['departments'] = departments_df
    dfs_dict['jobs'] = jobs_df

    # transformation with concatenation both people dfs

    people_cast = mdfcast.create_df(columns=['person_id', 'gender', 'name', 'profile_path'], index_name='person_id')

    people_crew = mdfcrew.create_df(columns=['person_id', 'gender', 'name', 'profile_path'], index_name='person_id')

    people_df = pd.concat([people_cast, people_crew]).drop_duplicates(ignore_index=True)

    dfs_dict['people'] = people_df

    return dfs_dict


def save_cleaned_datasets(dataframes_dict: dict):
    dataset_path = Path(__file__).resolve().parent.parent.parent / "dataset"

    cleaned_path = Path(dataset_path, 'cleaned')
    cleaned_path.mkdir(parents=True, exist_ok=True)

    for df_name, df in dataframes_dict.items():
        df.to_csv(cleaned_path / df_name, index=False)

    return None
