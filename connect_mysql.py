import os
import re
import pymysql


class MySQLConnection:
    def __init__(self, host, user, password, database, port):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port

    def __enter__(self):
        self.conn = pymysql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            port=self.port
        )
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is None:
            self.conn.commit()
        self.conn.close()

def query_db(db_connect_info, query, args=(), many=False, one=False):
    host = db_connect_info['host']
    port = db_connect_info['port']
    user = db_connect_info['user']
    password = db_connect_info['password']
    database = db_connect_info['database']

    if not query:
        raise ValueError("The query string cannot be empty.")

    with MySQLConnection(host, user, password, database, port) as conn:
        cur = conn.cursor()
        try:
            if many:
                result = cur.executemany(query, args)
            else:
                result = cur.execute(query, args)

            if re.match(r"(SELECT)", query, re.I):
                rv = cur.fetchall()
                return (rv[0] if rv else None) if one else rv
            else:
                return cur.rowcount  # 영향 받은 행의 수
        except Exception as E:
            print(f"Query Error: {database}, {query}, \n{E}, \n{args}")
            raise

def batch_delete_tables(db_connect_info, table_name, batch_size=1000):
    # 데이터를 삭제할 때 사용할 기본 쿼리
    delete_query = f"DELETE FROM {table_name} LIMIT {batch_size}"

    # 삭제된 행의 총 개수
    total_deleted = 0

    while True:
        # query_db 함수를 이용해 데이터 삭제 실행
        deleted_rows = query_db(db_connect_info, delete_query)
        
        # 삭제된 행이 없으면 반복 종료
        if deleted_rows == 0:
            break
        
        # 삭제된 행의 수를 누적
        total_deleted += deleted_rows

        print(f"{deleted_rows} rows deleted from {table_name}. Total deleted: {total_deleted}")

    # print(f"All rows deleted from {table_name}. Total {total_deleted} rows affected.")

def connection_test(db_connect_info):
    """Test the connection to the MySQL database."""
    try:
        with MySQLConnection(
            host=db_connect_info['host'],
            user=db_connect_info['user'],
            password=db_connect_info['password'],
            database=db_connect_info['database'],
            port=db_connect_info.get('port', 3306)
        ) as conn:
            print("Successfully connected to MySQL database.")
            return True
    except Exception as e:
        print(f"Connection failed: {e}")
        return False
