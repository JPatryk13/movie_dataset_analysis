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
    old_index_names=("index", "cast_id"),
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

# poniżej do usunięcia
# r = df.merge(sub_df, how='left', left_on='cast', right_on='original_value')[['film_id', 'cast_id', 'character', 'gender', 'id', 'name', 'profile_path']]
# print(r)
# print(r[r[['film_id', 'character', 'name']].duplicated()][[ 'name']])
# print(sub_df)


#################
# data cleaning #
#################


#### zamknąć w funkcji lub klasie od tąd -> usuwanie duplikatów z cast_id
cast_ids_list = [2059, 4182, 5503, 26386, 26387, 33802, 71915, 85094, 85095, 85096, 85099, 96760, 96761, 96763, 96765,
                 106889, 125230, 141154, 142087, 146006, 146007, 146008, 146009, 146010, 146011, 157382, 176252, 180234,
                 184604, 190151, 190378, 194565, 195967, 200400, 201104, 201113, 204718, 212381, 233409, 250679, 261009,
                 263170, 263217, 272952, 272960, 282904, 286214, 289500, 291556, 297270, 302492, 304510, 304509, 327887,
                 330469, 333306, 333307, 340863, 341563, 342839, 342840, 342841, 357038, 359080, 359852, 359853, 366055,
                 374553, 374554, 374555, 374556, 374557, 384468, 384764, 403130, 410610, 412112, 414193, 418860, 436507,
                 436834, 440095, 440098, 440100, 440101, 440102, 457781, 461004, 469770, 470600, 480658, 483883, 491523,
                 497975, 500734, 506646, 508405, 508406, 508427, 515029, 515143, 521302, 526423, 527865, 527866, 527867,
                 528558, 530196, 533095, 537661, 545498]

for id in cast_ids_list:
    sub_df.drop(sub_df.loc[sub_df.cast_id == id].index, inplace=True)

# sprawdzić czy sub_df ma wciąż duplikaty
sub_df.loc[sub_df.id == 1216756, 'id'] = 113387
sub_df.loc[sub_df.id == 1127849, 'id'] = 33608
sub_df.loc[sub_df.id == 179942, 'id'] = 1216483
sub_df.loc[sub_df.id == 1234191, 'id'] = 932097
sub_df.loc[sub_df.id == 1070406, 'id'] = 555778
sub_df.loc[sub_df.id == 932097, 'name'] = 'Celestina Aladekoba'

sub_df.reset_index(drop=True, inplace=True)
sub_df = sub_df.drop('cast_id', axis=1).reset_index(names='cast_id')

### do tąd


##################################
# prepare df to match ERD tables #
##################################

# people df
people_df = sub_df[['id', 'gender', 'name', 'profile_path']].drop_duplicates(ignore_index=True)

# czyszczenie people df
people_df.loc[people_df.id == 9779, 'name'] = 'Morris Chestnut'
people_df.loc[people_df.id == 23764, 'name'] = 'Erika Eleniak'
people_df.loc[people_df.id == 58646, 'name'] = 'Damian Chapa'
people_df.loc[people_df.id == 1091312, 'name'] = 'Russ Clark'
people_df.loc[people_df.id == 235722, 'name'] = 'Haruo Nakajima'
people_df.loc[people_df.id == 85775, 'name'] = 'Dick Pinner'
people_df.loc[people_df.id == 115772, 'name'] = 'Charles Tannen'
people_df.loc[people_df.id == 87637, 'name'] = 'Masami Nagasawa'
people_df.loc[people_df.id == 117642, 'name'] = 'Jason Momoa'
people_df.loc[people_df.id == 67212, 'name'] = 'Tom Wu'
people_df.loc[people_df.id == 78456, 'name'] = 'Kimbo Slice'
people_df.loc[people_df.id == 87662, 'name'] = 'Issei Takahashi'
people_df.loc[people_df.id == 74947, 'name'] = 'Kitty Zhang Yuqi'
people_df.loc[people_df.id == 932764, 'name'] = 'Jung In-Sun'
people_df.loc[people_df.id == 111690, 'name'] = 'Takako Matsu'
people_df.loc[people_df.id == 132233, 'name'] = 'Tony Schiena'
people_df.loc[people_df.id == 78809, 'name'] = 'Rashad Evans'
people_df.loc[people_df.id == 99692, 'name'] = 'Liao Fan'
people_df.loc[people_df.id == 72932, 'name'] = 'Ryuhei Matsuda'
people_df.loc[people_df.id == 555778, 'name'] = 'Pongsatorn Jongwilak'
people_df.loc[people_df.id == 1785844, 'gender'] = 2
people_df.loc[people_df.id == 78809, 'gender'] = 2
people_df.loc[people_df.id == 70883, 'gender'] = 1
people_df.loc[people_df.id == 191752, 'gender'] = 1
people_df.loc[people_df.id == 1216483, 'gender'] = 1
people_df.loc[people_df.id == 1104340, 'gender'] = 1
people_df.loc[people_df.id == 262075, 'gender'] = 1
people_df.loc[people_df.id == 4644, 'gender'] = 1
people_df.loc[people_df.id == 47395, 'gender'] = 2
people_df.loc[people_df.id == 1608740, 'gender'] = 2
people_df.loc[people_df.id == 555778, 'gender'] = 2
people_df.loc[people_df.id == 17199, 'gender'] = 2
people_df.loc[people_df.id == 935841, 'gender'] = 2
people_df.loc[people_df.id == 1135277, 'gender'] = 2
people_df.loc[people_df.id == 231784, 'gender'] = 2
people_df.loc[people_df.id == 114733, 'gender'] = 2
people_df.loc[people_df.id == 6718, 'profile_path'] = '/fN9LyZnztco8Tk7NpV0JiGnFkIy.jpg'
people_df.loc[people_df.id == 8241, 'profile_path'] = '/vsT0YQb3LSBH4nkUiWjx1ogSIXM.jpg'
people_df.loc[people_df.id == 3363, 'profile_path'] = '/kI8j8WGjDay9ovWnoTrqvcKYtN3.jpg'
people_df.loc[people_df.id == 136195, 'profile_path'] = '/6yVzZc5XAoaVBZ4JcK15NKBeGlH.jpg'
people_df.loc[people_df.id == 7682, 'profile_path'] = '/yzh26eyKJBtORjLlz8nyLQT7C1d.jpg'
people_df.loc[people_df.id == 936, 'profile_path'] = '/we10IusqRV1NHRtA68ftyck2N33.jpg'
people_df.loc[people_df.id == 1634622, 'profile_path'] = '/xKFZXYUGZGZdiJSJOtEKhvMo7K2.jpg'
people_df.loc[people_df.id == 70883, 'profile_path'] = '/sofDsjadyHeRiafPclN4i6TChir.jpg'
people_df.loc[people_df.id == 1200780, 'profile_path'] = '/7Yn7BVgep48QNAf5cAjzMQl0a13.jpg'
people_df.loc[people_df.id == 1608740, 'profile_path'] = '/cXb0xksR2SmuJEoUCOIr7NyBZRt.jpg'
people_df.loc[people_df.id == 29368, 'profile_path'] = '/greQI4q5Cbpy0SUkZCHZ1tuaaAe.jpg'
people_df.loc[people_df.id == 37028, 'profile_path'] = '/rkNC80zwfLwDAb23eXZuyZO0JOO.jpg'
people_df.loc[people_df.id == 115647, 'profile_path'] = '/uNwvMOGvFhQKWqt0rYuT48KRxac.jpg'
people_df.loc[people_df.id == 99888, 'profile_path'] = '/2ppMruL72FDz5Q0xIcGaFxwCDI6.jpg'
people_df.loc[people_df.id == 29986, 'profile_path'] = '/88sD6UHbUsCuur3r0x1PXToFx6q.jpg'
people_df.loc[people_df.id == 1135277, 'profile_path'] = '/prcO9VZBcQS4FhxATbVzDr1005A.jpg'
people_df.loc[people_df.id == 145092, 'profile_path'] = '/fmR9dO5cxrcwk5bjENU4eNITgLi.jpg'
people_df.loc[people_df.id == 56843, 'profile_path'] = '/tE7JkSuyv2N8vtEN0YypsCAxW9B.jpg'
people_df.loc[people_df.id == 7547, 'profile_path'] = '/gvQKaVUjfmi8fnWIRKJbwd8UfRc.jpg'
people_df.loc[people_df.id == 228167, 'profile_path'] = '/aPIAkjn75D1iYsgFJMvZ62WvMOx.jpg'
people_df.loc[people_df.id == 51641, 'profile_path'] = '/pk3xrVZWlu0Adrkqwp61sojZ239.jpg'
people_df.loc[people_df.id == 111690, 'profile_path'] = '/e1vaEiwuglqOYZXvbCX4KLgALeI.jpg'
people_df.loc[people_df.id == 1331602, 'profile_path'] = '/mkgLRMZHgQQ4PM8KbidOACnorSc.jpg'
people_df.loc[people_df.id == 70013, 'profile_path'] = '/7CTTjm8Wh2SKH6Cb4RIpNjjn1zn.jpg'
people_df.loc[people_df.id == 212225, 'profile_path'] = '/mzeEB6ZDAE036Z2oh2ZIqzrzVic.jpg'
people_df.loc[people_df.id == 128045, 'profile_path'] = '/v8h4Ypnu6YVHKER3R4u3THdGAq.jpg'
people_df.loc[people_df.id == 104018, 'profile_path'] = '/yJk1tsIhEYhlJkDtcvhZIrwbToK.jpg'
people_df.loc[people_df.id == 37430, 'profile_path'] = '/ls9gOUs7nSZyutuL5MULC1PZftN.jpg'
people_df.loc[people_df.id == 52701, 'profile_path'] = '/dGAnHPYdQJ9FjddU855aJ7xjKwS.jpg'
people_df.loc[people_df.id == 161310, 'profile_path'] = '/r3nMsFTaVkQ7XPncbvrJBVbqhUa.jpg'
people_df.loc[people_df.id == 139567, 'profile_path'] = '/mYGbPBzjepkpjWDbUzVhXiFGrFe.jpg'
people_df.loc[people_df.id == 1153503, 'profile_path'] = '/byMVHHhtaMvLFSThKKhuqAHOOIl.jpg'
people_df.loc[people_df.id == 145087, 'profile_path'] = '/4WKsWWGFv8FEL4aowzthMT8RqCH.jpg'
people_df.loc[people_df.id == 145086, 'profile_path'] = '/tSFiZ95afNa64EsxPVXteRrwPJz.jpg'
people_df.loc[people_df.id == 932097, 'profile_path'] = '/vzSaw247wxsUKYoQa647pUeMKaQ.jpg'
people_df.loc[people_df.id == 231784, 'profile_path'] = '/tG2MNITHmcJ0RsB8sApGZ9zrIcT.jpg'
people_df.loc[people_df.id == 1104340, 'profile_path'] = '/8wdFxKcaBx98sIXOZUAhelQuv7M.jpg'
people_df.loc[people_df.id == 262075, 'profile_path'] = '/gT8xNBN2xhP2JqN44qklXqUshHK.jpg'
people_df.loc[people_df.id == 1747946, 'profile_path'] = '/9DfqsBLKvxCgMRSprlo2isMFpV5.jpg'
people_df.loc[people_df.id == 1747947, 'profile_path'] = '/29RUAACDGXopRfJ78d41RctDIf0.jpg'
people_df.loc[people_df.id == 555778, 'profile_path'] = '/AqOS0w0QhrFsgVxaaO1vV8v8MfS.jpg'
people_df.loc[people_df.id == 1301102, 'profile_path'] = None
people_df.loc[people_df.id == 990654, 'profile_path'] = None
people_df.loc[people_df.id == 976019, 'profile_path'] = None
people_df.loc[people_df.id == 131606, 'profile_path'] = None
people_df.loc[people_df.id == 131605, 'profile_path'] = None
people_df.loc[people_df.id == 107221, 'profile_path'] = None
people_df.loc[people_df.id == 88471, 'profile_path'] = None


# posprawdzać czy w people_df nie ma więcej duplikatów - czy są unikalne rekordy
# przerzucić to czyszczenie go jakiejś funkcji

people_df = people_df.drop_duplicates(ignore_index=True)
print(people_df)
print(people_df[people_df['name'].duplicated(keep=False)])
print(people_df[people_df[['name', 'profile_path']].duplicated(keep=False)])


nulls = people_df[~people_df.profile_path.isnull()]
print(nulls[nulls['profile_path'].duplicated(keep=False)])
print(nulls['profile_path'].duplicated(keep=False).sum())

r = nulls.loc[nulls.profile_path == '/uS4a3epqXVtjTUGRR37zG0yOQFS.jpg']
# print(r)

x = people_df.loc[people_df.name == 'Steve Martin']
y = people_df.loc[people_df.name == 'John Marshall']
z = sub_df.loc[sub_df.id == 1301102]
print(x)
print(y)
# print(z)
#