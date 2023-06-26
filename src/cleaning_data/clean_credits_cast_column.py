import pandas as pd
from pathlib import Path
import re
import json
from tools import make_junction_table

pd.set_option('display.max_columns', 500)
pd.set_option('display.width', 1000)

archive_path = Path(__file__).resolve().parent.parent.parent / "dataset" / "archive"

df = pd.read_csv(archive_path / "credits.csv", low_memory=False).drop_duplicates(ignore_index=True).reset_index()

df.rename(columns={'id': 'film_id'}, inplace=True)


cast_dict_keys = ("cast_id", "character", "credit_id", "gender", "id", "name", "order", "profile_path")

converted = []

for elem in df["cast"].drop_duplicates(keep=False):

    # noinspection PyBroadException
    try:

        # Skips empty lists
        if not json.loads(elem):
            continue

    except Exception:

        # Do nothing on error - the iteration of the loop is going to be skipped anyway
        pass

    finally:

        str_list = elem.split("}, {")

        for str_dict in str_list:

            # store converted/cleaned key-value pairs
            temp_dict = {}

            for i, key in enumerate(cast_dict_keys):

                if key is not cast_dict_keys[-1]:

                    key2 = cast_dict_keys[i + 1]

                    # Find values based to the preceding and following keys + remove first and last three chars which
                    # are single quotes, spaces and double colons or commas
                    val_match_obj = re.search(
                        rf"\b{key}\b(.*?)\b{key2}\b",
                        str_dict
                    )

                    if val_match_obj:
                        val = val_match_obj.group()[len(key) + 3:-len(key2) - 3]
                    else:
                        val = None

                else:

                    val_match_obj = re.search(
                        rf"{key}\s*([^\n\r]*)",
                        str_dict
                    )

                    if val_match_obj:
                        val = val_match_obj.group()[len(key) + 3:].replace("'}", "")
                    else:
                        val = None

                    temp_dict["original_value"] = elem

                if val and "None" not in val:

                    if "'" in val:

                        # val is a string
                        temp_dict[key] = val[1:-1].replace("\"", "\'")

                    else:

                        # value is numeric type
                        temp_dict[key] = float(val) if "." in val else int(val)

                else:

                    temp_dict[key] = None

            converted.append(temp_dict)

sub_df = pd.DataFrame(converted).drop(['cast_id', 'credit_id', 'order'], axis=1).reset_index(names='cast_id')

# tutaj jakiś cleaning wejdzie
junction_df = make_junction_table(
    left_df=df,
    right_df=sub_df,
    merge_on=("cast", "original_value"),
    old_index_names= ("index", "cast_id"),
    new_index_names=("credits_fk", "cast_fk")
)
# print(junction_df)
# print(df)
# print(sub_df)
# print(sub_df[['id', 'character']].duplicated().sum())
# print(sub_df[sub_df[['id', 'character']].duplicated(keep=False)])


#################
# moje zmagania #
#################

# r = df.merge(sub_df, how='left', left_on='cast', right_on='original_value')[['film_id', 'cast_id', 'character', 'gender', 'id', 'name', 'profile_path']]
# print(r)
# print(r[r[['film_id', 'character', 'name']].duplicated()][[ 'name']])
# print(sub_df)


#################
# data cleaning #
#################

sub_df.drop(sub_df.loc[sub_df.cast_id == 2059].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 4182].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 5503].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 26386].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 26387].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 33802].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 71915].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 85094].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 85095].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 85096].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 85099].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 96760].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 96761].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 96763].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 96765].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 106889].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 125230].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 141154].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 142087].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 146006].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 146007].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 146008].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 146009].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 146010].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 146011].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 157382].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 176252].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 180234].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 184604].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 190151].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 190378].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 194565].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 195967].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 200400].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 201104].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 201113].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 204718].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 212381].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 233409].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 250679].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 261009].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 263170].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 263217].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 272952].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 272960].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 282904].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 286214].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 289500].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 291556].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 297270].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 302492].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 304510].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 304509].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 327887].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 330469].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 333306].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 333307].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 340863].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 341563].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 342839].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 342840].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 342841].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 357038].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 359080].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 359852].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 359853].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 366055].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 374553].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 374554].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 374555].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 374556].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 374557].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 384468].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 384764].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 403130].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 410610].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 412112].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 414193].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 418860].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 436507].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 436834].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 440095].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 440098].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 440100].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 440101].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 440102].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 457781].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 461004].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 469770].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 470600].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 480658].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 483883].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 491523].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 497975].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 500734].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 506646].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 508405].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 508406].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 508427].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 515029].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 515143].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 521302].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 526423].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 527865].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 527866].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 527867].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 528558].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 530196].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 533095].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 537661].index, inplace=True)
sub_df.drop(sub_df.loc[sub_df.cast_id == 545498].index, inplace=True)


#### po czyszczeniu zresetować indeks jako cast_id
# sprawdzić czy są jeszcze gdzieś duplikaty w sub_df

print(sub_df[sub_df[['original_value', 'character', 'name']].duplicated(keep=False)])
print(sub_df[sub_df[['original_value', 'id', 'character']].duplicated(keep=False)])



# r = sub_df.loc[(sub_df.character == '') & (sub_df.name == 'Gábor Jászberényi')].to_dict()
# print(r)
# print(r['original_value'][527864])
# print(r['original_value'][527865])

