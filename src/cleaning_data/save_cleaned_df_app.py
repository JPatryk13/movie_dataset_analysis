from cleaning_functions import clean_movies, clean_cast, clean_crew
from transformation_functions import *

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
if __name__ == "__main__":
    all_dfs_dict = {}

    clean_movies = clean_movies()
    clean_cast = clean_cast()
    clean_crew = clean_crew()

    movies_dict = make_dict_from_movies_dfs(clean_movies_df=clean_movies)

    keywords_dict = make_dict_from_keywords_dfs()

    credits_dict = make_dict_from_credits_dfs(clean_cast_df=clean_cast, clean_crew_df=clean_crew)

    # add all dataframes to empty all_dfs dict as key: dataframe_name, value: dataframe object
    all_dfs_dict['ratings_df'] = transform_ratings(movies_dict['movies_df']['film_id'])

    movies_dict['movies_df'] = movies_dict['movies_df'].set_index('film_id').sort_index()

    all_dfs_dict.update(movies_dict)
    all_dfs_dict.update(keywords_dict)
    all_dfs_dict.update(credits_dict)

    # list of all dataframes names to save
    # dataframes_list = list(all_dfs_dict.keys())

    save_cleaned_datasets(all_dfs_dict)
