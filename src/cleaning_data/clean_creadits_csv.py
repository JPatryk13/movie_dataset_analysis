import pandas as pd
import json
from flatten_json import flatten

# pd.set_option('display.max_columns', None)

file_path = 'D:\projekt z patrykiem\movie_dataset_analysis\dataset\\archive\credits.csv'

# df = pd.read_csv(file_path, index_col='id')
df = pd.read_csv(file_path)

df['cast'] = df['cast'].str.replace("'",'"', regex=False)
df['cast'] = df['cast'].str.replace("None",'null', regex=False)
# print(df['cast'])
# df['cast'] = df['cast'].apply(json.loads)
# print(df['cast'])

df = df[['id', 'cast']].iloc[50:150].to_json(orient='records')
print(df)
df_json = json.loads(df)
# print(df_json)
json.loads(df_json[0]["cast"])
# print(json.loads(df_json[0]["cast"]))
print(type(df_json[0]["cast"]))
# print(pd.json_normalize())

# df_json = df.to_json(orient='records')
# json_from_df = json.loads(df_json)
# for record in json_from_df:
    # print(record)


# dic = json_from_df[0]
# dic_flattened = [flatten(d) for d in dic]
# print(dic_flattened)
# for d in dic:
#     print(d)



# df = df[['cast', 'id']][:2]
# df_json = df.to_json(orient='records')
# print(json.loads(df_json)[0]['cast'])
# print(pd.json_normalize(json.loads(df_json)[0]))
# check_json = pd.json_normalize(json.loads(df_json), meta=['cast'])
# print(json.loads(df_json))
# print(type(json.loads(df_json)))
# print(check_json)
# df_cast = pd.json_normalize(df, 'cast')
# print(df_cast)


# y = json.dumps(json.loads(df_json))
# print(y)
# print(type(y))
# final_df = pd.json_normalize(json.dumps(json.loads(df_json)), meta = ['id'])
# print(final_df)

# print(df)
# print(df['cast'])
# print(type(df['cast']))

# print(pd.json_normalize(eval(df['cast'][0])))


# first_item = eval(df['cast'][0])
# print(pd.json_normalize(first_item))






#### PRZYKŁAD
# json_response = {"id":1670,"symbol":"FX-GBP","name":"London","calType":"C","events":[{"date":"2021-01-01","name":"New Year's Day","type":"FULL","updateTime":"2021-06-18T18:15:55"},{"date":"2021-04-02","name":"Good Friday","type":"FULL","updateTime":"2021-06-18T18:15:55"},{"date":"2021-04-05","name":"Easter Monday","type":"FULL","updateTime":"2021-06-18T18:15:55"},{"date":"2021-05-03","name":"Early May Bank Holiday","type":"FULL","updateTime":"2021-06-18T18:15:55"},{"date":"2021-05-31","name":"Late May Bank Holiday","type":"FULL","updateTime":"2021-06-18T18:15:55"},{"date":"2021-08-30","name":"Summer Bank Holiday","type":"FULL","updateTime":"2021-06-18T18:15:55"},{"date":"2021-12-27","name":"Christmas OBS","type":"FULL","updateTime":"2021-06-18T18:15:55"},{"date":"2021-12-28","name":"Boxing Day OBS","type":"FULL","updateTime":"2021-06-18T18:15:55"}]}
#
# df_rest = pd.DataFrame(json_response)
# df_event = pd.json_normalize(json_response, 'events')
# complete_df = pd.concat([df_rest, df_event], axis=1)
#
# print(df_rest)
# print(df_event)
# complete_df = complete_df.drop(columns = ['events'])
# print(complete_df)





#
# import json
# from pandas import DataFrame
# from pandas.io.json import json_normalize
#
# df = DataFrame({'post_id':123456,
#                 'comments': [{'from':'Bob','present':True},
#                              {'from':'Jon', 'present':False}]})
# df_json = df.to_json(orient='records')
# print(df_json)
# finaldf = json_normalize(json.loads(df_json), meta=['post_id'])
# print(finaldf)
#
# #   comments.from comments.present  post_id
# # 0           Bob             True   123456
# # 1           Jon            False   123456