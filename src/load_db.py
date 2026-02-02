from config import (
    DB_PATH,
    BENCHMARK_FILE
)
import os
import json

import sqlite3


def get_bird_db_path(db_name):
    return os.path.join(DB_PATH, "bird-1", db_name, f"{db_name}.sqlite")


def load_tables(spark_session, db_name):
    load_bird_tables(spark_session, db_name)


def load_bird_tables(spark_session, db_name):
    db_path = get_bird_db_path(db_name)
    print(f"--- Scanning database: {db_path} ---")
    abs_db_path = os.path.abspath(db_path)
    jdbc_url = f"jdbc:sqlite:{abs_db_path}"
    db_connection = sqlite3.connect(db_path)
    cursor = db_connection.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [row[0] for row in cursor.fetchall()]
    db_connection.close()

    if not tables:
        print("Warning: No tables found in the database!")
        return

    for table in tables:
        df = spark_session.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table) \
            .option("driver", "org.sqlite.JDBC") \
            .load()

        df.createOrReplaceTempView(table)
        print(f" -> Registered table: '{table}'")


def load_query_info(query_id: int):

    query_data_file = os.path.join(DB_PATH, "bird-1", BENCHMARK_FILE)

    with open(query_data_file, 'r') as f:
        all_queries = json.load(f)

    query_info = None
    for query_entry in all_queries:
        if query_entry['question_id'] == query_id:
            query_info = query_entry
            break

    if query_info is None:
        raise ValueError(f"Query ID {query_id} not found")

    question = " ".join([
        query_info["question"],
        query_info.get("evidence", "")
    ])
    golden_query = query_info["SQL"]
    difficulty = query_info.get("difficulty", "unknown")

    return question, golden_query, difficulty
