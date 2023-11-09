import os
import re
import pymssql

class MSSQLConnection:
    def __init__(self, host, user, password, database, port=1433):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.port = port

    def __enter__(self):
        self.conn = pymssql.connect(
            server=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            port=self.port,
            autocommit=False
        )
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.conn.rollback()
        else:
            self.conn.commit()
        self.conn.close()

def query_db(db_connect_info, query, args=(), many=False, one=False):
    if not query:
        raise ValueError("The query string cannot be empty.")

    with MSSQLConnection(
        host=db_connect_info['host'],
        user=db_connect_info['user'],
        password=db_connect_info['password'],
        database=db_connect_info['database'],
        port=db_connect_info.get('port', 1433) # 기본 포트를 1433으로 설정
    ) as conn:
        cur = conn.cursor(as_dict=True) # 결과를 딕셔너리 형태로 받기 위한 설정
        try:
            if many:
                result = cur.executemany(query, args)
            else:
                result = cur.execute(query, args)

            if re.match(r"(SELECT)", query, re.I):
                rv = cur.fetchall()
                return (rv[0] if rv else None) if one else rv
            else:
                conn.commit()  # 명시적으로 커밋
                return cur.rowcount  # 영향 받은 행의 수
        except Exception as E:
            conn.rollback()  # 오류가 발생하면 롤백
            print(f"Query Error: {db_connect_info['database']}, {query}, \n{E}, \n{args}")
            raise

def batch_delete_tables(db_connect_info, table_name, batch_size=1000):
    # 데이터를 삭제할 때 사용할 기본 쿼리
    delete_query = f"DELETE TOP({batch_size}) FROM {table_name}"

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

def connection_test(db_connect_info):
    """Test the connection to the MSSQL database."""
    try:
        with MSSQLConnection(
            host=db_connect_info['host'],
            user=db_connect_info['user'],
            password=db_connect_info['password'],
            database=db_connect_info['database'],
            port=db_connect_info.get('port', 1433)
        ) as conn:
            print("Successfully connected to MSSQL database.")
            return True
    except Exception as e:
        print(f"Connection failed: {e}")
        return False
