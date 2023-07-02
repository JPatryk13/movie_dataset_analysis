import pandas as pd
from pathlib import Path

archive_path = Path(__file__).resolve().parent.parent.parent / "dataset" / "archive"

# ratings_df has no duplicates with the same user_id and movie_id also no null values in all columns
# all types in df are valid -> only int and float
# ratings_df -> done
ratings_df = pd.read_csv(archive_path / "ratings.csv", low_memory=False).drop_duplicates(ignore_index=True).rename(
    columns={'userId': 'user_id', 'movieId': 'movie_id'})

# convert timestamp column to date time object representing date and time
ratings_df['timestamp'] = pd.to_datetime(ratings_df['timestamp'], unit='s')
