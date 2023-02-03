import os

from generator import generate_dataset

def generate_all_datasets(force=False):
    dataset_template = {100: ( 5500, 10000, 180),
                        200: (10000, 20000, 180),
                        300: (15000, 30000, 180),}
    
    file_names = ("customer", "terminal", "transaction")

    for k, v in dataset_template.items():
        path = f"./data/{k}/"

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
                            generate_dataset(n_customers    = 5000,
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
    generate_test_dataset()
