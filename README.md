# Database Connection Helpers

이 레포지토리는 다양한 종류의 데이터베이스에 연결하고 쿼리를 수행하기 위한 Python 스크립트를 제공합니다. 현재 MSSQL, MySQL, SQLite 데이터베이스를 지원합니다.

## 파일 설명

- `connect_mssql.py`: MSSQL 데이터베이스에 연결하고 쿼리를 실행하기 위한 스크립트입니다.
- `connect_mysql.py`: MySQL 데이터베이스에 연결하고 쿼리를 실행하기 위한 스크립트입니다.
- `connect_sqlite.py`: SQLite 데이터베이스 파일에 연결하고 쿼리를 실행하기 위한 스크립트입니다.

## 사용 방법

- 각 스크립트 파일은 해당하는 데이터베이스에 연결하는 클래스와 쿼리를 실행하는 함수를 포함하고 있습니다. 다음은 각 스크립트를 사용하는 기본적인 방법입니다. 
- 데이터베이스 연결을 확인하기 위한 `connection_test` 함수를 각 스크립트에 추가했습니다.

### MSSQL
#### MSSQL-사용법
- 아래의 명령어를 이용하여 라이브러리를 다운받습니다.
```
pip install pymssql
```

```python
from connect_mssql import query_db

db_info = {
    'host': 'your_host',
    'user': 'your_user',
    'password': 'your_password',
    'database': 'your_db',
    'port': 1433
}

query = "SELECT * FROM your_table"
results = query_db(db_info, query)
```
#### MSSQL-연결테스트
```python
from connect_mssql import connection_test

db_info = {
    'host': 'your_host',
    'user': 'your_user',
    'password': 'your_password',
    'database': 'your_db',
    'port': 1433
}

if connection_test(db_info):
    print("Connection to MSSQL database is successful.")
else:
    print("Connection to MSSQL database failed.")
```

### MYSQL

#### MYSQL-사용법
- 아래의 명령어를 이용하여 라이브러리를 다운받습니다.
```
pip install pymysql
```

```python
from connect_mysql import query_db

db_info = {
    'host': 'your_host',
    'user': 'your_user',
    'password': 'your_password',
    'database': 'your_db',
    'port': 3306
}

query = "SELECT * FROM your_table"
results = query_db(db_info, query)
```
#### MYSQL-연결테스트
```python
from connect_mysql import connection_test

db_info = {
    'host': 'your_host',
    'user': 'your_user',
    'password': 'your_password',
    'database': 'your_db',
    'port': 3306
}

if connection_test(db_info):
    print("Connection to MySQL database is successful.")
else:
    print("Connection to MySQL database failed.")
```

### SQLite
#### SQLite-사용법
```python
from connect_sqlite import query_db

db_name = 'your_database.db'
query = "SELECT * FROM your_table"
results = query_db(db_name, query)
```
#### SQLite-연결테스트
```python
from connect_sqlite import connection_test

db_name = 'your_database.db'

if connection_test(db_name):
    print("Connection to SQLite database is successful.")
else:
    print("Connection to SQLite database failed.")
```
## 기능
각 스크립트는 SELECT 쿼리를 실행하여 결과를 반환하거나 INSERT, UPDATE, DELETE 등의 쿼리를 실행하여 영향을 받은 행의 수를 반환합니다. 또한, SQLite와 MySQL 스크립트에는 대용량 테이블에서 행을 배치로 삭제하는 기능이 포함되어 있습니다.