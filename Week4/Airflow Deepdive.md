# Airflow Deepdive
- Airflow 설치: https://github.com/keeyong/data-engineering-batch10/blob/main/docs/Airflow%202%20Installation.md

## Airflow Operators, Variables, and Connections

#### Configuration
```python
from datetime import datetime, timedelta
from airflow import DAG
default_args = {
    'owner': 'keeyong',
    'email': ['keeyonghan@hotmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}
```

#### DAG
```python
from airflow import DAG
from datetime import datetime

dag = DAG(
    dag_id = 'my_first_dag',
    start_date = datetime(2022,5,5),
    catchup=False,  # 과거로 부터의 backfill을 진행할 것인지 여부
    tags=['example'],
    schedule_interval = '0 2 * * *',
    max_active_runs: 10     # 동시에 실행할 수 있는 DAG 수
    max_active_tasks: 10    # 동시에 실행할 수 있는 Task 수 
    )
```

#### Operator
```python
from airflow.exceptions import AirflowException
def python_func(**cxt):
    table = cxt["params"]["table"]
    schema = cxt["params"]["schema"]
    ex_date = cxt["execution_date"] # Airflow에서 제공하는 인자

load_nps = PythonOperator(
    dag=dag,
    task_id='task_id',
    # 사용할 함수명
    python_callable=python_func,
    # 매개변수를 활용할 수 있다
    params={
    'table': 'delighted_nps',
    'schema': 'raw_data'
    },
)
```

#### Airflow Variable
- key-value로 이루어진 환경변수를 보관하는 용도
- 웹 UI를 통해 추가할 수 있음
- Key 이름에 특정 단어가 들어가면 암호화 (hostname, token 등)
```python
extract = PythonOperator(
    task_id = 'extract',
    python_callable = extract,
    params = {
        # Variable을 통해서 접근
        'url':  Variable.get("csv_url") 
    },
    dag = dag_second_assignment)
```
- Airflow Connection
- Connection 정보를 Airflow에 등록하여 사용할 수 있음
```python
def get_Redshift_connection(autocommit=False):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()
```
- Xcom 객체
- Airflow 메타 데이터 DB를 활용한다
- 너무 큰 데이터는 활용하지 않도록 주의
```python
def extract(**context):
    link = context["params"]["url"]
    task_instance = context['task_instance']
    execution_date = context['execution_date']

    logging.info(execution_date)
    f = requests.get(link)
    return (f.text)

def transform(**context):
    # extract에서 return된 Value를 활용한다
    text = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")
    lines = text.split("\n")[1:]
    return lines
```
    
#### Airflow cli 명령어
```powershell
airflow tasks list {DAG 이름}
airflow tasks list {DAG 이름} {task 이름} {execution_date}
airflow dags test {DAG 이름} {execution_date}
airflow dags backfill {DAG 이름} -s {start_date} -e {end_date}
```

#### 기타 유의 사항
- 에러가 나타났을 때는 `raise`를 통해서 에러를 발생시키는 것이 좋음
    ```python
    try:
        cur.execute(create_sql)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        raise
    ```
- Task 분리
    - Task가 많음: 전체 DAG가 실행되는데 오래 걸리고 스케줄러에 부하가 감
    - Task가 적음: 모듈화가 안 되고, 실패 시 재실행이 오래 걸림
- CTAS에서 속성을 가져오기
    ```sql
    CREATE TABLE {schema}.temp_table AS SELECT * FROM {schema}.{table} (LIKE {schema}.{table} INCLUDING DEFAULTS);
    ```

#### Airflow 환경 설정 관련
- Airflow의 환경 설정이 들어있는 파일의 이름은?
    - airflow.cfg
- Airflow를 API 형태로 외부에서 조작하고 싶다면 어느 섹션을 변경해야하는가?
    - api 섹션의 auth_backend를 airflow.api.auth.backend.basic_auth로 변경
- Variable에서 변수 값이 encrypt되려면 변수 이름에 어떤 단어들이 들어가야
하는가?
    - password, secret, passwd, authorization, api_key, apikey, access_token
- 이 환경 설정 파일이 수정되었다면 이를 실제로 반영하기 위해서 해야 하는
일은?
    - sudo systemctl restart airflow-webserver
    - sudo systemctl restart airflow-scheduler
- DAGs 폴더에 새로운 Dag를 만들면 언제 실제로 Airflow 시스템에서 이를 알게
되나? 이 스캔주기를 결정해주는 키의 이름이 무엇인가?
    - adagdirlistinterval
