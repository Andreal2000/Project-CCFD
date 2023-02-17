import logging
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable


class Database:

    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        # Don't forget to close the driver connection when you are finished with it
        self.driver.close()

    def insert_customer(self, df):
        with self.driver.session() as session:
            tx = session.begin_transaction()
            try:
                tx_time = 0
                query = """CREATE (n:Customer { CUSTOMER_ID : $CUSTOMER_ID,
                                                x_customer_id : $x_customer_id,
                                                y_customer_id : $y_customer_id,
                                                mean_amount : $mean_amount,
                                                std_amount : $std_amount,
                                                mean_nb_tx_per_day : $mean_nb_tx_per_day})"""

                for index, row in df.iterrows():

                    result = tx.run(query,
                                    CUSTOMER_ID=row["CUSTOMER_ID"],
                                    x_customer_id=row["x_customer_id"],
                                    y_customer_id=row["y_customer_id"],
                                    mean_amount=row["mean_amount"],
                                    std_amount=row["std_amount"],
                                    mean_nb_tx_per_day=row["mean_nb_tx_per_day"],)


                    avail = result.consume().result_available_after
                    cons = result.consume().result_consumed_after
                    total_time = avail + cons
                    tx_time += total_time
                    # print(total_time, "ms")

                    # if (index + 1) % 1000 == 0:
                    #     tx.commit()
                    #     tx = session.begin_transaction()

                print(tx_time, "ms")
                
                if not tx.closed():
                    tx.commit()
            finally:
                tx.close()  # rolls back if not yet committed

    def insert_terminal(self, df):
        with self.driver.session() as session:
            tx = session.begin_transaction()
            try:
                tx_time = 0
                query = """CREATE (n:Terminal { TERMINAL_ID : $TERMINAL_ID,
                                                x_terminal_id : $x_terminal_id,
                                                y_terminal_id : $y_terminal_id})"""

                for index, row in df.iterrows():

                    result = tx.run(query,
                                    TERMINAL_ID=row["TERMINAL_ID"],
                                    x_terminal_id=row["x_terminal_id"],
                                    y_terminal_id=row["y_terminal_id"],)


                    avail = result.consume().result_available_after
                    cons = result.consume().result_consumed_after
                    total_time = avail + cons
                    tx_time += total_time
                    # print(total_time, "ms")

                    # if (index + 1) % 1000 == 0:
                    #     tx.commit()
                    #     tx = session.begin_transaction()

                print(tx_time, "ms")
                
                if not tx.closed():
                    tx.commit()
            finally:
                tx.close()  # rolls back if not yet committed

    def insert_transaction(self, df):
        with self.driver.session() as session:
            tx = session.begin_transaction()
            try:
                tx_time = 0
                query = """MATCH (c:Customer), (t:Terminal)
                           WHERE c.CUSTOMER_ID = $CUSTOMER_ID AND t.TERMINAL_ID = $TERMINAL_ID
                           CREATE (c)-[r:TRANSACTION { TX_DATETIME : $TX_DATETIME, TX_AMOUNT: $TX_AMOUNT }]->(t)"""
                
                for index, row in df.iterrows():

                    result = tx.run(query,
                                    TRANSACTION_ID=row["TRANSACTION_ID"],
                                    CUSTOMER_ID=row["CUSTOMER_ID"],
                                    TERMINAL_ID=row["TERMINAL_ID"],
                                    TX_DATETIME=row["TX_DATETIME"],
                                    TX_AMOUNT=row["TX_AMOUNT"],)


                    avail = result.consume().result_available_after
                    cons = result.consume().result_consumed_after
                    total_time = avail + cons
                    tx_time += total_time
                    # print(total_time, "ms")

                    if (index + 1) % 1000 == 0:
                        tx.commit()
                        tx = session.begin_transaction()

                print(tx_time, "ms")
                
                if not tx.closed():
                    tx.commit()
            finally:
                tx.close()  # rolls back if not yet committed

    # def create_friendship(self, person1_name, person2_name):
    #     with self.driver.session() as session:
    #         # Write transactions allow the driver to handle retries and transient errors
    #         result = session.execute_write(
    #             self._create_and_return_friendship, person1_name, person2_name)
    #         for record in result:
    #             print("Created friendship between: {p1}, {p2}".format(
    #                 p1=record['p1'], p2=record['p2']))

    # @ staticmethod
    # def _create_and_return_friendship(tx, person1_name, person2_name):

    #     # To learn more about the Cypher syntax,
    #     # see https://neo4j.com/docs/cypher-manual/current/

    #     # The Reference Card is also a good resource for keywords,
    #     # see https://neo4j.com/docs/cypher-refcard/current/

    #     query = (
    #         "CREATE (p1:Person { name: $person1_name }) "
    #         "CREATE (p2:Person { name: $person2_name }) "
    #         "CREATE (p1)-[:KNOWS]->(p2) "
    #         "RETURN p1, p2"
    #     )
    #     result = tx.run(query, person1_name=person1_name,
    #                     person2_name=person2_name)
    #     try:
    #         return [{"p1": record["p1"]["name"], "p2": record["p2"]["name"]}
    #                 for record in result]
    #     # Capture any errors along with the query and data for traceability
    #     except ServiceUnavailable as exception:
    #         logging.error("{query} raised an error: \n {exception}".format(
    #             query=query, exception=exception))
    #         raise

    # def find_person(self, person_name):
    #     with self.driver.session() as session:
    #         result = session.execute_read(
    #             self._find_and_return_person, person_name)
    #         for record in result:
    #             print("Found person: {record}".format(record=record))

    # @ staticmethod
    # def _find_and_return_person(tx, person_name):
    #     query = (
    #         "MATCH (p:Person) "
    #         "WHERE p.name = $person_name "
    #         "RETURN p.name AS name"
    #     )
    #     result = tx.run(query, person_name=person_name)
    #     return [record["name"] for record in result]
