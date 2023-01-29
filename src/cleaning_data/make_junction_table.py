import pandas as pd


def make_junction_table(
        left_df: pd.DataFrame,
        right_df: pd.DataFrame,
        *, merge_on: tuple[str, str],
        old_index_names: tuple[str, str] = ("index", "index"),
        new_index_names: tuple[str, str] = ("left_df_fk", "left_df_fk")
) -> pd.DataFrame:
    """
    Merge (inner) given dataframes on merge_on columns and extract only foreign key (from the left and right dataframes)
    columns to form junction table of many-to-many relationship between right and left table.

    :param left_df:
    :param right_df:
    :param merge_on: list of column names to merge on in order: [<left_df_col_name>, <right_df_col_name>]
    :param old_index_names: (optional) old index column names - by default both set to "index"
    :param new_index_names: (optional) renames index column names from index to [<left_df_fk>, <right_df_fk>]
    :return: junction table
    """
    # TODO:
    #  verify that <left_df_col_name>, <right_df_col_name> exist in left/right_df
    #  .
    #  verify that both column names exist in left/right_df
    #  .
    #  verify that <left_df_fk>, <right_df_fk> exist in left/right_df

    left_df = left_df[[old_index_names[0], merge_on[0]]].rename(columns={old_index_names[0]: new_index_names[0]})
    right_df = right_df[[old_index_names[1], merge_on[1]]].rename(columns={old_index_names[1]: new_index_names[1]})

    return left_df.merge(
        right_df,
        how="inner",
        left_on=merge_on[0],
        right_on=merge_on[1]
    )[[new_index_names[0], new_index_names[1]]]
