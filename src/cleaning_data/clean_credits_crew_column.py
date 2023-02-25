import pandas as pd
import json
from pathlib import Path
from make_junction_table import make_junction_table

pd.set_option('display.max_columns', None)

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
).drop(['credit_id'], axis=1).drop_duplicates(ignore_index=True).reset_index(names='crew_id')

# people_df before junction
people_df = crew_film_id_df[['id', 'gender', 'name', 'profile_path']].drop_duplicates(ignore_index=True)
#
# 3 duplicates people below checking duplicates function
# print(people_df[people_df[['id', 'name']].duplicated(keep=False)])

# jobs df -> done
jobs_before_merge = crew_film_id_df[['job', 'department']].drop_duplicates(ignore_index=True).reset_index(names='job_id')

# departments df -> done
departments_df = jobs_before_merge[['department']].drop_duplicates(ignore_index=True).reset_index(names='department_id')

# add departments to jobs -> done
jobs_df = jobs_before_merge.merge(departments_df, 'left', on='department').drop('department', axis=1)

# crew jobs junction -> done
crew_jobs_junction = make_junction_table(
    crew_film_id_df,
    jobs_before_merge,
    merge_on=('job', 'job'),
    old_index_names=('crew_id', 'job_id'),
    new_index_names=('crew_fk', 'jobs_fk')
)

# crew dataframe need to match with crew table from ERD
#crew_df = crew_film_id_df