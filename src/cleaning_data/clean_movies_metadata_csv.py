import numpy as np
import pandas as pd
from pathlib import Path
from tools import nested_dict_col_to_df, make_junction_table, unique_row_unique_index
from typing import Literal


class CleanMoviesMetadata:
    COL_NAMES = Literal[
        "belongs_to_collection",
        "genres",
        "production_countries",
        "production_companies",
        "spoken_languages"
    ]

    def __init__(self):
        self.archive_path = Path(__file__).resolve().parent.parent.parent / "dataset" / "archive"

        self.df = pd.read_csv(
            self.archive_path / "movies_metadata.csv",
            low_memory=False
        ).drop_duplicates(ignore_index=True).reset_index()

    def extract_df_from_col(self, *, col_name: COL_NAMES) -> tuple[pd.DataFrame, pd.DataFrame | None]:
        """
        Extract column with given name and transform the data that are embedded within dictionaries/lists of
        dictionaries to form a separate table and - if lists of dictionaries - junction table. It deals with duplicates
        and sets additional index column to be the primary key of the table. It also builds relations between each table
        and the main one (self.df) and - if lists of dictionaries - between each table and the junction table.

        :param col_name: name of the column to be extracted and transformed into a separate dataframe
        :return: a dataframe based on the given column
        """

        # Break down dictionaries/list of dictionaries (within one column) into a separate dataframe. Dictionary
        # embedded in a column will be joined to a dataframe along with dictionaries from all rows. The same happens
        # with a list of dicts - the list is first broken down in separate dictionaries which are then added to a df.
        # Such procedure may cause duplicates which are dealt with using drop_duplicates() method. Indices are being
        # reset and relationships dealt with separately by comparing content of the new dataframe with the main
        # (metadata) one.
        sub_df = nested_dict_col_to_df(
            self.df[col_name].dropna(),
            save_original=True
        ).drop_duplicates(ignore_index=True).reset_index()

        # Type hinting cause pycharm was getting confused
        junction_df: pd.DataFrame | None = None

        # Only 'belongs_to_collection' column has embedded dictionaries - the rest has lists of dictionaries
        if col_name == "belongs_to_collection":

            # Add fk referencing elements from belongs_to_collection to the main df.
            # Merge "index" and "original_value" columns from sub_df with the main df (self.df) using the latter one as
            # the merge-on value. In the process, the "index" @ sub_df is renamed to "belongs_to_collection_fk". That
            # results in the main df having two extra columns from which "original_value" and "belongs_to_collection"
            # are redundant since we now have a foreign key connecting main df and sub_df.
            self.df = self.df.merge(
                sub_df[["index", "original_value"]].rename(columns={"index": "belongs_to_collection_fk"}),
                how="left",
                left_on="belongs_to_collection",
                right_on="original_value"
            )

            # Remove "original_value" column (the one that we were merging on) and change the type of the
            # "belongs_to_collection_fk" to Int64 that incorporates use of missing values within integer-type variables
            self.df = self.df.loc[:, ~self.df.columns.isin(["original_value"])].astype({f"{col_name}_fk": "Int64"})

            # Remove "original_value" column from the sub_df
            sub_df = sub_df.loc[:, ~sub_df.columns.isin(["original_value"])]

        else:

            # Generate indices based on the value in columns that are not original_value and index. Allows for removing
            # duplicates later without destroying the relationship between sub_df and junction df
            sub_df = unique_row_unique_index(sub_df)

            junction_df = make_junction_table(
                left_df=self.df,
                right_df=sub_df,
                merge_on=(col_name, "original_value"),
                new_index_names=("movie_metadata_fk", f"{col_name}_fk")
            )

            # remove original_value column and duplicate rows
            sub_df = sub_df.loc[:, ~sub_df.columns.isin(["original_value"])].drop_duplicates(ignore_index=True)

        # Remove col_name column as it is redundant. The junction table will cover the relationship between the data
        # from col_name column and the main df
        self.df = self.df.loc[:, ~self.df.columns.isin([col_name])]

        return sub_df, junction_df

    def get_main_df(self):
        """
        Advised to use after executing extract_df_from_col() for each column as that function edits the self.df
        dataframe in the process.

        :return: main dataframe
        """
        return self.df


if __name__ == "__main__":
    pd.set_option('display.max_columns', 500)
    pd.set_option('display.width', 1000)

    cmm = CleanMoviesMetadata()

    belongs_to_collection_df, _ = cmm.extract_df_from_col(col_name="belongs_to_collection")
    genres_df, genres_junction_df = cmm.extract_df_from_col(col_name="genres")
    production_countries_df, production_countries_junction_df = cmm.extract_df_from_col(col_name="production_countries")
    production_companies_df, production_companies_junction_df = cmm.extract_df_from_col(col_name="production_companies")
    spoken_languages_df, spoken_languages_junction_df = cmm.extract_df_from_col(col_name="spoken_languages")

    df = cmm.get_main_df()

    # print(belongs_to_collection_df)
    # print(genres_df, genres_junction_df)
    # print(production_countries_df, production_countries_junction_df)
    # print(production_companies_df, production_companies_junction_df)
    # print(spoken_languages_df, spoken_languages_junction_df)

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

# runtime może być powyżej kilku godzin bo są to seriale i czas to łączna ich długość odcinków -> notatka

# convert string date to date object
df['release_date'] = pd.to_datetime(df['release_date']).dt.date

# converting to appropriate data types of columns
df = df.rename(columns={'id': 'film_id'}).astype(
    {'adult': 'bool', 'budget': 'int64', 'film_id': 'int64', 'original_language': 'category', 'status': 'category',
     'video': 'bool'})


##################################
# prepare df to match ERD tables #
##################################

# drop duplicates and set index film_id as index (PK for movies df) and index ascending
df.drop('index', axis=1, inplace=True)
movies_df = df.drop_duplicates(ignore_index=True).set_index('film_id').sort_index()

# powyższe czyszczenie movies_df przerzucić albo do klasy konstruktora albo do oddzielnego pliku tak aby dalszych modyfikacji dokonywać na czystym już pliku
# genres -> działamy trzeba sprawdzić czy index movie_fk odpowiada movie id + dropnąć index w genres i ustawić id jako nowy index jako genre_id
print(genres_df)
print(genres_junction_df)