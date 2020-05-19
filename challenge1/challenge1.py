#%%
import pandas as pd
from tqdm import tqdm

#%%
keyword = pd.read_csv("Keyword_spam_question.csv")
keyword.head()
keyword['groups_found'] = [[] for i in range(keyword.shape[0])]

#%%
extra = pd.read_csv("Extra Material 2 - keyword list_with substring.csv")
extra.head()
category_index_name = {}
for index in range(len(extra['Keywords'])):
    category_index_name[index] = extra['Keywords'][index].split(", ")

#%%
# keyword = keyword.sample(10)
for keyword_index, keyword_description in tqdm(
        zip(keyword['index'], keyword['name'])):
    assigned_category = []
    for category_index, category_list in category_index_name.items():
        for category in category_list:
            if category in keyword_description:
                # print(keyword_description, category)
                # print(category_index)
                assigned_category.append(category_index)
        # print(index, category_list)
    # print(assigned_category)
    keyword.at[keyword_index, "groups_found"] = assigned_category
#%%
keyword.drop('name', axis=1, inplace=True)
import datetime
now = str(datetime.datetime.now()).replace('-', '')
keyword.to_csv(f'submission_text_{now}.csv', index=False)