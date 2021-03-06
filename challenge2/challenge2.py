#%%
import pandas as pd
from tqdm import tqdm

#%%
orders = pd.read_csv("orders.csv")
orders = orders.set_index("orderid", drop=False)
orders['is_fraud'] = [0 for i in range(orders.shape[0])]
orders.head()

#%%
bank_accounts = pd.read_csv("bank_accounts.csv")
bank_accounts = bank_accounts.set_index("userid", drop=False)
bank_accounts.head()
#%%
credit_cards = pd.read_csv("credit_cards.csv")
credit_cards = credit_cards.set_index("userid", drop=False)
credit_cards.head()
#%%
devices = pd.read_csv("devices.csv")
devices = devices.set_index("userid", drop=False)
devices.head()
#%%
len(
    list(
        set(devices.loc[47388162]['device'].to_list())
        & set(devices.loc[20822974]['device'].to_list())))
#%%

# iterate through all orders
# check if there's overlap of buyer_userid and seller_userid in device, credit_card, bank_account
# check overlap by filtering each df with user_id, check device column, convert to list, then convert to set, then check overlap, then convert to list, then count the length of list

orders_sub = orders
for orderid, buyer_userid, seller_userid in tqdm(
        zip(orders_sub['orderid'], orders_sub['buyer_userid'],
            orders_sub['seller_userid'])):
    device_count, bank_account_count, credit_card_count = 0, 0, 0

    try:
        device_count = len(
            list(
                set(devices.loc[buyer_userid]['device'].to_list())
                & set(devices.loc[seller_userid]['device'].to_list())))
    except:
        pass

    try:
        bank_account_count = len(
            list(
                set(bank_accounts.loc[buyer_userid]['bank_account'].to_list())
                & set(bank_accounts.loc[seller_userid]
                      ['bank_account'].to_list())))
    except:
        pass

    try:
        credit_card_count = len(
            list(
                set(credit_card.loc[buyer_userid]['credit_card'].to_list())
                & set(credit_card.loc[seller_userid]['credit_card'].to_list()))
        )
    except:
        pass

    if sum([device_count, bank_account_count, credit_card_count]) > 0:
        print(orderid)

        orders.at[orderid, "is_fraud"] = 1
    # print(list1, list2)

#%%
orders.drop('buyer_userid', axis=1, inplace=True)
orders.drop('seller_userid', axis=1, inplace=True)
print(orders.head())
import datetime
now = str(datetime.datetime.now()).replace('-', '')
orders.to_csv(f'submission_text_{now}.csv', index=False)
