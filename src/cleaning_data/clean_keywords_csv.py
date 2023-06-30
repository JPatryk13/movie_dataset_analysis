import pandas as pd
import json
from ast import literal_eval
from pathlib import Path

archive_path = Path(__file__).resolve().parent.parent.parent / "dataset" / "archive"

df = pd.read_csv(archive_path / "keywords.csv", low_memory=False).drop_duplicates(ignore_index=True)#


# convert string representation of list to actual list type in keywords column -> cleaning
df['keywords'] = df['keywords'].apply(literal_eval)


df_json = df.to_json(orient='records')

all_keywords_df = pd.json_normalize(
    json.loads(df_json),
    record_path='keywords',
    meta='id',
    meta_prefix='film_'
).rename(columns={'id': 'keyword_id'})


##################################
# prepare df to match ERD tables #
##################################

# keywords_df -> done
keywords_df = all_keywords_df[['keyword_id', 'name']].drop_duplicates(ignore_index=True).sort_values('keyword_id')

# keywords_movies_junction_df -> done
all_keywords_df = all_keywords_df[['film_id', 'keyword_id']].sort_values('film_id')




