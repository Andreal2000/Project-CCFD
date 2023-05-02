import os

from database import Database
from generator import generate_dataset

DIR_DATA = "./data"
DIR_OUTPUT = "./output"

if os.path.exists(f"{DIR_OUTPUT}/log.txt"):
    os.remove(f"{DIR_OUTPUT}/log.txt")

def log(string):
    open(f"{DIR_OUTPUT}/log.txt", "a").write(f"{string}\n")
    print(string) 

def generate_all_datasets(force=False):
    dataset_template = {100: (2500,  5000, 365),
                        200: (5000, 10000, 365),
                        300: (7500, 15000, 365),}

    file_names = ("customer", "terminal", "transaction")

    for k, v in dataset_template.items():
        path = f"{DIR_DATA}/{k}/"

        if force or not any([os.path.isfile(f"{path}{name}.csv") for name in file_names]):
            print(f"Generate {k}Mbyte dataset")

            datasets = dict(zip(file_names,
                                generate_dataset(n_customers = v[0],
                                                 n_terminals = v[1],
                                                 nb_days     = v[2], 
                                                 start_date  = "2022-01-01",
                                                 r           = 5)))

            if not os.path.exists(path):
                os.makedirs(path)

            for name, data in datasets.items():
                data.to_csv(f"{path}{name}.csv", index=False)

if __name__ == "__main__":
    generate_all_datasets()

    for size in range(100, 301, 100):
        # See https://neo4j.com/developer/aura-connect-driver/ for Aura specific connection URL.
        scheme = "neo4j"  # Connecting to Aura, use the "neo4j+s" or "neo4j+ssc" URI scheme
        host_name = f"neo4j_{size}"
        port = 7687
        url = f"{scheme}://{host_name}:{port}"
        user = "neo4j"
        password = "neo4jpassword"
        db = Database(url, user, password, f"{DIR_OUTPUT}/{size}")
        try:
            db.load_customer(f"file:///{size}/customer.csv")
            db.index_customer()

            db.load_terminal(f"file:///{size}/terminal.csv")
            db.index_terminal()

            db.load_transaction(f"file:///{size}/transaction.csv")
            db.index_transaction()
            
            db.query_1()
            db.query_2()
            db.query_3()
            db.query_4_1()
            db.query_4_2()
            db.query_4_3()
            db.query_5()
            
        finally:
            db.close()
