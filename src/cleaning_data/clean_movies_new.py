import pandas as pd
import json
from pathlib import Path
from tools import nested_dict_col_to_df
from ast import literal_eval

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

archive_path = Path(__file__).resolve().parent.parent.parent / "dataset" / "archive"

df = pd.read_csv(
    archive_path / "movies_metadata.csv",
    low_memory=False
).drop_duplicates(ignore_index=True)

##############################
#      moje zmagania         #
# cleaning i conversja typów #
##############################

# drop unnecessary columns
df.drop('popularity', axis=1, inplace=True)
df.drop('vote_average', axis=1, inplace=True)
df.drop('vote_count', axis=1, inplace=True)

# drop mess data
df.drop(df.loc[df['id'] == '1997-08-20'].index, inplace=True)
df.drop(df.loc[df['id'] == '2012-09-29'].index, inplace=True)
df.drop(df.loc[df['id'] == '2014-01-01'].index, inplace=True)

# convert string date to date object
df['release_date'] = pd.to_datetime(df['release_date']).dt.date

# converting to appropriate data types of columns
df = df.rename(columns={'id': 'film_id'}).astype(
    {'adult': 'bool', 'budget': 'Int64', 'film_id': 'Int64', 'original_language': 'category',
     'revenue': 'Int64', 'runtime': 'Int64', 'status': 'category', 'video': 'bool'}
).drop_duplicates(ignore_index=True).sort_values('film_id').reset_index()

##################################
# prepare df to match ERD tables #
##################################

# drop duplicates and set index film_id as index (PK for movies df) and index ascending

# btc_df
df['belongs_to_collection'] = df['belongs_to_collection'].fillna('[]')
btc_df = nested_dict_col_to_df(df['belongs_to_collection'], save_original=True)
btc_df = btc_df.rename(columns={'id': 'collection_id'}).drop_duplicates(ignore_index=True)
btc_df['collection_id'] = btc_df['collection_id'].astype('Int64')
collections_df = btc_df.sort_values('collection_id')  # .set_index('collection_id').sort_index()

df = df.merge(
    btc_df,
    how='left',
    left_on='belongs_to_collection',
    right_on='original_value').drop(
    ['belongs_to_collection', 'name', 'poster_path_y', 'backdrop_path', 'original_value'], axis=1)

### collections_df -> done
collections_df.drop('original_value', axis=1, inplace=True)


# prepare genres_df
def extract_df_from_col(col_name, index_name=None):
    if not index_name:
        index_name = col_name

    df[col_name] = df[col_name].fillna('[]').apply(literal_eval)
    df_to_json = df[['film_id', col_name]].to_json(orient='records')

    normalized_df = pd.json_normalize(
        json.loads(df_to_json),
        record_path=col_name,
        meta='film_id',
    )

    if col_name not in ['production_countries', 'spoken_languages']:
        normalized_df = normalized_df.rename(columns={'id': f'{index_name}_id'}).astype(
            {f'{index_name}_id': 'Int64', 'film_id': 'Int64'})

        junction_df = normalized_df[['film_id', f'{index_name}_id']]
        transformed_df = normalized_df.drop('film_id', axis=1).drop_duplicates(ignore_index=True).sort_values(
            f'{index_name}_id')

    else:
        junction_df = normalized_df[['film_id', f'{index_name}']]
        transformed_df = normalized_df.drop('film_id', axis=1).drop_duplicates(ignore_index=True).sort_values(
            f'{index_name}')

    return transformed_df, junction_df


### genres_df and genres_movies_junction dataframe -> done wszystkie typy poprawne i posortowane
genres_df, genres_movies_junction = extract_df_from_col('genres')

### production_companies_df and production_companies_junction -> done
production_companies_df, production_companies_junction = extract_df_from_col('production_companies', 'company')
production_companies_df = production_companies_df[['company_id', 'name']]

### production_countries_df and production_countries_junction -> done
production_countries_df, production_countries_junction = extract_df_from_col('production_countries', 'iso_3166_1')
production_countries_df = production_countries_df.rename(columns={'iso_3166_1': 'country_code'}).astype(
    {'country_code': 'category'})
production_countries_junction = production_countries_junction.rename(columns={'iso_3166_1': 'country_code'}).astype(
    {'country_code': 'category', 'film_id': 'Int64'})

### spoken_languages_df and languages_movies_junction -> done + jak mi się będzie chciało to ręcznie pododawać brakujące name w językach
spoken_languages_df, languages_movies_junction = extract_df_from_col('spoken_languages', 'iso_639_1')
spoken_languages_df = spoken_languages_df.rename(columns={'iso_639_1': 'language_code'}).astype(
    {'language_code': 'category'})
languages_movies_junction = languages_movies_junction.rename(columns={'iso_639_1': 'language_code'}).astype(
    {'language_code': 'category', 'film_id': 'Int64'})

# movies_df -> done + sprawdzić czy set_index wstawi index do bazy danych jako PK
movies_df = df.drop(['index', 'genres', 'production_companies', 'production_countries', 'spoken_languages'],
                    axis=1).rename(columns={'poster_path_x': 'poster_path'})#.set_index('film_id').sort_index()


