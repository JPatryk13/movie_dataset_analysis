import pandas as pd
from src.cleaning_data.cleaning_classes import CleanMoviesMetadata, CleanCast, CleanCrew
from src.cleaning_data.cleaning_variables import *


def clean_movies() -> pd.DataFrame:
    cmm = CleanMoviesMetadata()

    wrong_id_list = cmm.list_faulty_ids()

    cmm.drop_unnecessary_columns(columns=['popularity', 'vote_average', 'vote_count'])
    cmm.drop_faulty_ids(wrong_ids=wrong_id_list)
    cmm.data_types_conversion()

    df = cmm.get_movies_df()

    return df


def clean_cast() -> pd.DataFrame:
    cc = CleanCast()

    cc.make_raw_dataframe(col_name='cast')

    cc.drop_unnecessary_columns(['cast_id', 'credit_id', 'order'])

    cc.data_types_conversion()

    cc.drop_faulty_ids(wrong_ids=person_and_film_ids)

    cc.replace_wrong_ids(person_ids)

    cc.replace_null_fields()

    cc.replace_values_in_column(col_name='name', id_and_value=clean_names)

    cc.replace_values_in_column(col_name='gender', id_and_value=clean_gender)

    cc.replace_values_in_column(col_name='profile_path', id_and_value=clean_profile_path)

    cc.data_types_conversion()

    df = cc.get_df()

    return df


def clean_crew() -> pd.DataFrame:
    cc = CleanCrew()

    cc.make_raw_dataframe(col_name='crew')

    cc.drop_unnecessary_columns(columns='credit_id')

    cc.data_types_conversion()

    cc.drop_faulty_ids(wrong_ids=ids_to_drop)

    cc.replace_values_in_column(col_name='gender', id_and_value=clean_gender_crew)
    cc.replace_values_in_column(col_name='profile_path', id_and_value=clean_profile_crew)
    cc.replace_values_in_column(col_name='name', id_and_value=clean_name_crew)

    df = cc.get_df()

    return df


pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)
print(clean_movies())