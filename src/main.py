import os
import pandas as pd

from database import Database
from generator import generate_dataset

DIR_DATA = "./data"

def generate_all_datasets(force=False):
    dataset_template = {100: (17500, 35000, 10),
                        200: (25500, 50000, 10),
                        300: (35500, 55000, 10),}

    file_names = ("customer", "terminal", "transaction")

    for k, v in dataset_template.items():
        path = f"{DIR_DATA}/{k}/"

        if force or not any([os.path.isfile(path+f"{name}.pkl") for name in file_names]):
            print(f"Generate {k}Mbyte dataset")

            datasets = dict(zip(file_names,
                                generate_dataset(n_customers = v[0],
                                                 n_terminals = v[1],
                                                 nb_days     = v[2], 
                                                 start_date  = "2018-04-01",
                                                 r           = 5)))

            if not os.path.exists(path):
                os.makedirs(path)

            for name, data in datasets.items():
                data.to_pickle(path+f"{name}.pkl")


def generate_test_dataset(force=False):
    file_names = ("customer", "terminal", "transaction")

    path = f"./data/test/"

    if force or not any([os.path.isfile(path+f"{name}.pkl") for name in file_names]):
        print(f"Generate test dataset")

        datasets = dict(zip(file_names,
                            generate_dataset(n_customers = 5000,
                                             n_terminals = 10000,
                                             nb_days     = 40, 
                                             start_date  = "2018-04-01",
                                             r           = 5)))
        
        if not os.path.exists(path):
            os.makedirs(path)

        for name, data in datasets.items():
            data.to_pickle(path+f"{name}.pkl")


if __name__ == "__main__":
    generate_all_datasets()
    # generate_test_dataset()

    # See https://neo4j.com/developer/aura-connect-driver/ for Aura specific connection URL.
    scheme = "neo4j"  # Connecting to Aura, use the "neo4j+s" or "neo4j+ssc" URI scheme
    host_name = "example.com"
    port = 7687
    url = f"{scheme}://{host_name}:{port}"
    user = "<Username for Neo4j database>"
    password = "<Password for Neo4j database>"
    # db = Database(url, user, password)
    # try:
    #     customer_df = pd.read_pickle(f"{DIR_DATA}/test/customer.pkl")
    #     db.insert_customer(customer_df) # 13214 ms

    #     terminal_df = pd.read_pickle(f"{DIR_DATA}/test/terminal.pkl")
    #     db.insert_terminal(terminal_df) # 7513 ms

    #     transaction_df = pd.read_pickle(f"{DIR_DATA}/test/transaction.pkl")
    #     db.insert_transaction(transaction_df)

    #     # db.create_friendship("Alice", "David")
    #     # db.find_person("Alice")
    # finally:
    #     db.close()

    for i in range(100, 301, 100):
        print(i, "Mbyte dataset")
        customer_df = pd.read_pickle(f"{DIR_DATA}/{i}/customer.pkl")
        terminal_df = pd.read_pickle(f"{DIR_DATA}/{i}/terminal.pkl")
        transaction_df = pd.read_pickle(f"{DIR_DATA}/{i}/transaction.pkl")

        print(customer_df.shape)
        print(terminal_df.shape)
        print(transaction_df.shape)
