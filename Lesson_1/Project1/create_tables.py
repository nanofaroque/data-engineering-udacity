import psycopg2
from sql_queries import create_table_queries, drop_table_queries, insert_table_queries


def create_database():
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=postgres password=password")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    conn.close()
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=postgres password=password")
    cur = conn.cursor()
    return cur, conn


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    print(insert_table_queries)
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
