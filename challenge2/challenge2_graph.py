#%%
import pandas as pd
from tqdm import tqdm

#%%
orders = pd.read_csv("orders.csv").astype(str)
orders = orders.set_index("orderid", drop=False)
orders['is_fraud'] = [0 for i in range(orders.shape[0])]
orders.head()

#%%
bank_accounts = pd.read_csv("bank_accounts.csv").astype(str)
bank_accounts = bank_accounts.set_index("userid", drop=False)
bank_accounts.head()
#%%
credit_cards = pd.read_csv("credit_cards.csv").astype(str)
credit_cards = credit_cards.set_index("userid", drop=False)
credit_cards.head()
#%%
devices = pd.read_csv("devices.csv").astype(str)
devices = devices.set_index("userid", drop=False)
devices.head()

#%%
from iteration_utilities import duplicates, unique_everseen
import itertools
import networkx as nx


def find_repeated_info(df):

    global merged
    global info_with_multiple_userid_ls

    # find duplicate entries with many userid
    df_colunm1_ls = df[df.columns[1]].to_list()
    info_with_multiple_userid_ls = list(
        unique_everseen(duplicates(df_colunm1_ls)))

    # construct new df, to cut down on the number of rows
    info_with_multiple_userid = pd.DataFrame(info_with_multiple_userid_ls,
                                             columns=["info"])
    info_with_multiple_userid.head()

    merged = pd.merge(info_with_multiple_userid,
                      df,
                      how="left",
                      left_on="info",
                      right_on=df.columns[1]).drop(df.columns[1], axis=1)


def build_graph(start, end):
    # build graph
    graph = nx.Graph()
    # using the info as edge, associated userid are linked nodes
    for info_repeated in tqdm(info_with_multiple_userid_ls[start:end]):
        # get the associated userid
        userid_ls = merged[merged['info'].isin([info_repeated
                                                ])]['userid'].tolist()

        for userid_1, userid_2 in itertools.combinations(userid_ls, 2):
            graph.add_edge(userid_1, userid_2)

    # return graph
    return graph


result_list = []


def log_result(result):
    global result_list
    result_list.append(result)


from multiprocessing import Pool


def split_build_graph(split_count=12):

    pool = Pool()

    split_size = len(info_with_multiple_userid_ls) // split_count

    for i in range(split_count):
        # determine the indices of the list this thread will handle
        start = i * split_size
        # special case on the last chunk to account for uneven splits
        end = None if i + 1 == split_count else (i + 1) * split_size

        # create the pool
        pool.apply_async(build_graph, args=(start, end), callback=log_result)

    pool.close()
    pool.join()

    # join all graphs
    global graph
    result_list.append(graph)
    graph = nx.compose_all(result_list)

    print(graph.number_of_edges())
    print(graph.number_of_nodes())


#%%
# build graph
graph = nx.Graph()
merged = 0
info_with_multiple_userid_ls = 0

# multiprocessing, cut down processing time from 6 min to 1min20s
for df in [bank_accounts, devices, credit_cards]:
    find_repeated_info(df)
    split_build_graph(split_count=8)

#%%
# check graph statistics
print(graph.number_of_edges())
print(graph.number_of_nodes())
#%%
# iterate through all orders
orders_original = orders
for orderid, buyer_userid, seller_userid in tqdm(
        zip(orders_original['orderid'], orders_original['buyer_userid'],
            orders_original['seller_userid'])):
    device_count, bank_account_count, credit_card_count = 0, 0, 0

    try:
        # find a path between buy and seller
        length = nx.shortest_path_length(graph, buyer_userid, seller_userid)
        orders.at[orderid, "is_fraud"] = 1
        print(f"\norderid: {orderid}, length: {length}")
    except Exception:
        pass

#%%
orders.drop('buyer_userid', axis=1, inplace=True)
orders.drop('seller_userid', axis=1, inplace=True)
print(orders.head())
import datetime
now = str(datetime.datetime.now()).replace('-', '')
orders.to_csv(f'submission_text_{now}.csv', index=False)
