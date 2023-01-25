import pandas as pd

file_path = 'D:\projekt z patrykiem\movie_dataset_analysis\dataset\\archive\credits.csv'

df = pd.read_csv(file_path)


first_item = eval(df['cast'][0])
print(first_item)
print(type(first_item[0]))
print(pd.json_normalize(first_item[0], sep='_'))

