import os
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

    def load_customer(self, path):
        with self.driver.session() as session:
            # Write transactions allow the driver to handle retries and transient errors
            query = (
                "LOAD CSV WITH HEADERS FROM $path AS row"
                "WITH toInteger(row.CUSTOMER_ID) AS CUSTOMER_ID,"
                "     toFloat(row.x_customer_id) AS x_customer_id,"
                "     toFloat(row.y_customer_id) AS y_customer_id,"
                "     toFloat(row.mean_amount) AS mean_amount,"
                "     toFloat(row.std_amount) AS std_amount,"
                "     toFloat(row.mean_nb_tx_per_day) AS mean_nb_tx_per_day,"
                "WHERE CUSTOMER_ID IS NOT NULL"
                "MERGE (c:Customer { CUSTOMER_ID : CUSTOMER_ID,"
                "                    x_customer_id : x_customer_id,"
                "                    y_customer_id : y_customer_id,"
                "                    mean_amount : mean_amount,"
                "                    std_amount : std_amount,"
                "                    mean_nb_tx_per_day : mean_nb_tx_per_day})"
            )
            values, summary = session.execute_write(self._load_csv, query, path)
            avail = summary.result_available_after
            cons = summary.result_consumed_after
            total_time = avail + cons
            print(f"Load customer csv from {path}")
            print(f"Time: {total_time} ms")
            print(f"Results:\n{values}")

    def load_terminal(self, path):
        with self.driver.session() as session:
            query = (
                "LOAD CSV WITH HEADERS FROM $path AS row"
                "WITH toInteger(row.TERMINAL_ID) AS TERMINAL_ID,"
                "     toFloat(row.x_terminal_id) AS x_terminal_id,"
                "     toFloat(row.y_terminal_id) AS y_terminal_id,"
                "WHERE TERMINAL_ID IS NOT NULL"
                "MERGE (t:Terminal { TERMINAL_ID : TERMINAL_ID,"
                "                    x_terminal_id : x_terminal_id,"
                "                    y_terminal_id : y_terminal_id})"
            )
            # Write transactions allow the driver to handle retries and transient errors
            values, summary = session.execute_write(self._load_csv, query, path)
            avail = summary.result_available_after
            cons = summary.result_consumed_after
            total_time = avail + cons
            print(f"Load terminal csv from {path}")
            print(f"Time: {total_time} ms")
            print(f"Results:\n{values}")

    def load_transaction(self, path):
        with self.driver.session() as session:
            query = (
                "LOAD CSV WITH HEADERS FROM $path AS row"
                "WITH toInteger(row.TRANSACTION_ID) AS TRANSACTION_ID,"
                "     datetime(replace(row.TX_DATETIME,' ','T')) AS TX_DATETIME,"
                "     toFloat(row.TX_AMOUNT) AS TX_AMOUNT,"
                "     toBoolean(row.TX_FRAUD) AS TX_FRAUD,"
                "     toInteger(row.CUSTOMER_ID) AS CUSTOMER_ID,"
                "     toInteger(row.TERMINAL_ID) AS TERMINAL_ID,"
                "WHERE TRANSACTION_ID IS NOT NULL"
                "MERGE (t:Customer { TRANSACTION_ID : TRANSACTION_ID,"
                "                    TX_DATETIME : TX_DATETIME,"
                "                    TX_AMOUNT : TX_AMOUNT,"
                "                    TX_FRAUD : TX_FRAUD})"
            )
            # Write transactions allow the driver to handle retries and transient errors
            values, summary = session.execute_write(self._load_csv, query, path)
            avail = summary.result_available_after
            cons = summary.result_consumed_after
            total_time = avail + cons
            print(f"Load transaction csv from {path}")
            print(f"Time: {total_time} ms")
            print(f"Results:\n{values}")

    @ staticmethod
    def _load_csv(tx, query, path):
        result = tx.run(query, path=path)
        try:
            values = result.to_df()
            summary = result.consume()
            return values, summary
                   
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(query=query, exception=exception))
            raise


    def query_1(self, path_output):
        with self.driver.session() as session:
            query = (
                "MATCH (c:Customer)-[:MAKE]->(t:Transaction)"
                "WHERE t.TX_DATETIME >= datetime({ year:2022, month:1, day:1 })"
                "      AND t.TX_DATETIME < datetime({ year:2022, month:5, day:1 })"
                "WITH c, t, datetime.truncate('week', t.TX_DATETIME) AS week"
                "RETURN c.CUSTOMER_ID, week, sum(t.amount) AS weekly_spending"
                "ORDER BY c.CUSTOMER_ID, week;"
            )
            values, summary = session.execute_read(self._run_query, query)
            avail = summary.result_available_after
            cons = summary.result_consumed_after
            total_time = avail + cons
            print(f"Query 1")
            print(f"Time: {total_time} ms")
            print(f"Results:\n{values}")
            
            if not os.path.exists(path_output):
                os.makedirs(path_output)
            values.to_csv(f"{path_output}/Q1.csv", index=False)
            print(f"Results saved in {path_output}/Q1.csv")

    def query_2(self, path_output):
        with self.driver.session() as session:
            query = (
                "MATCH (tm:Terminal)-[:EXECUTE]->(t:Transaction)"
                "WITH tm, t, datetime.truncate('month', t.TX_DATETIME) AS month"
                "WITH tm, month, avg(t.TX_AMOUNT) AS avg_amount"
                "MATCH (tm)-[:EXECUTE]->(f:Transaction)"
                "WHERE f.TX_DATETIME < datetime({ year:2023, month:1, day:1 })"
                "AND (f.TX_AMOUNT > 1.1*avg_amount OR f.TX_AMOUNT < 0.9*avg_amount)"
                "RETURN tm.TERMINAL_ID, f.TRANSACTION_ID, f.TX_AMOUNT"
                "ORDER BY tm.TERMINAL_ID, f.TRANSACTION_ID"
            )
            values, summary = session.execute_read(self._run_query, query)
            avail = summary.result_available_after
            cons = summary.result_consumed_after
            total_time = avail + cons
            print(f"Query 1")
            print(f"Time: {total_time} ms")
            print(f"Results:\n{values}")
            
            if not os.path.exists(path_output):
                os.makedirs(path_output)
            values.to_csv(f"{path_output}/Q2.csv", index=False)
            print(f"Results saved in {path_output}/Q2.csv")

    @ staticmethod
    def _run_query(tx, query, **kwargs):
        result = tx.run(query, kwargs)
        try:
            values = result.to_df()
            summary = result.consume()
            return values, summary
                   
        # Capture any errors along with the query and data for traceability
        except ServiceUnavailable as exception:
            logging.error("{query} raised an error: \n {exception}".format(query=query, exception=exception))
            raise

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
