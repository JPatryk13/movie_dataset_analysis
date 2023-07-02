import pandas as pd
import json
from pathlib import Path
from tools import make_junction_table

archive_path = Path(__file__).resolve().parent.parent.parent / "dataset" / "archive"

df = pd.read_csv(archive_path / "credits.csv", low_memory=False)

replace_map = {
    "\"": "\'",
    ": \'": ": \"",
    ", \'": ", \"",
    "{\'": "{\"",
    "\', ": "\", ",
    "\'}": "\"}",
    "\': ": "\": ",
    "None": "null"
}

for key, value in replace_map.items():
    df['crew'] = df['crew'].str.replace(key, value, regex=False)

df['crew'] = df['crew'].apply(json.loads)

df_json = df[['crew', 'id']].to_json(orient='records')

crew_film_id_df = pd.json_normalize(
    json.loads(df_json),
    record_path='crew',
    meta='id',
    meta_prefix='film_'
).drop(['credit_id'], axis=1).drop_duplicates(ignore_index=True)

############
# CLEANING #
############

# clean wrong people -> check each time is all people valid
crew_film_id_df.loc[crew_film_id_df.name == 'Sally Boldt', 'gender'] = 1
crew_film_id_df.loc[crew_film_id_df.name == 'Peter Malota', 'gender'] = 2
crew_film_id_df.loc[crew_film_id_df.id == 148084, 'profile_path'] = None
crew_film_id_df.loc[crew_film_id_df.id == 260050, 'profile_path'] = None
crew_film_id_df.loc[crew_film_id_df.id == 46391, 'profile_path'] = None
crew_film_id_df.loc[crew_film_id_df.id == 127279, 'profile_path'] = None
crew_film_id_df.loc[crew_film_id_df.name == 'Fruit Chan', 'profile_path'] = '/j1Xktbs6M6kNIwaLUps9swSEdVs.jpg'
crew_film_id_df.loc[crew_film_id_df.name == 'Ka-Fai Cheung', 'name'] = 'Cheung Ka-Fai'
crew_film_id_df.drop(crew_film_id_df.loc[crew_film_id_df.id == 1339312].index, inplace=True)
crew_film_id_df.drop(crew_film_id_df.loc[crew_film_id_df.id == 572046].index, inplace=True)

##################################
# prepare df to match ERD tables #
##################################

# only_crew_df - used to merge with other dataframes -> done
only_crew = crew_film_id_df.drop(['film_id'], axis=1).drop_duplicates(ignore_index=True).reset_index(names='crew_id')

# crew df -> done, brak nulls, typy zgodny int
crew_df = only_crew[['crew_id', 'id']]

# people df  -> done only for crew column (add cast people to this df), typy zgodne, brak nulls - tylko w profile path
# brak duplikatów
people_df = only_crew[['id', 'gender', 'name', 'profile_path']].drop_duplicates(ignore_index=True)

# departments df -> done
departments_df = only_crew[['department']].drop_duplicates(ignore_index=True).reset_index(
    names='department_id')

# jobs df -> done, no duplicates, correct dtypes
jobs_df = only_crew[['job']].drop_duplicates(ignore_index=True).reset_index(names='job_id')

# departments_jobs_junction df -> done
departments_crew = departments_df.merge(only_crew, how='left', on='department')
departments_crew_jobs = departments_crew.merge(jobs_df, how='left', on='job')
departments_jobs_junction = departments_crew_jobs[['department_id', 'job_id']].drop_duplicates(ignore_index=True)

# crew_departments_junction -> done
crew_departments_junction = make_junction_table(
    only_crew,
    departments_df,
    merge_on=('department', 'department'),
    old_index_names=('crew_id', 'department_id'),
    new_index_names=('crew_fk', 'department_fk')
)

# crew_movies_junction -> done, correct types
columns_to_merge = ['department', 'gender', 'id', 'job', 'name', 'profile_path']
crew_movies_junction = crew_film_id_df.merge(only_crew, how='left', on=columns_to_merge)[['film_id', 'crew_id']].astype(
    {'film_id': 'int64'})

# cast_df -> done: in other py file

# move people from cast add to people_df