import pandas as pd
import json
from pathlib import Path

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
)
