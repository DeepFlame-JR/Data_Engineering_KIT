# 3. ETL/Airflow 소개

### Spark/Athena 시나리오
- Spark: 대규모 데이터 처리용 통합 분석 엔진
- Athena: S3에 있는 데이터를 간편하게 분석할 수 있는 대화형 쿼리 서비스
- 비구조화된 데이터 처리하기
    - LOG > S3 > Spark/Athena > RedShift
- 대용량 데이터 병렬 처리 (feature 계산)

## 데이터 파이프라인
- 데이터를 소스로부터 목적지로 복사하는 작업
    - 보통 코딩(Python or Scala) 혹은 SQL을 통해 이뤄짐
    - 목적지는 Data Warehouse/Data Mart
- `ETL`: Extract Transform and Load (DE가 진행)
    1. 외부와 내부 데이터 소스에서 데이터를 읽어다가 (많은 경우 API를 통하게 됨)
    2. 적당한 데이터 포맷 변환 후 (데이터의 크기가 커지면 Spark등이 필요해짐)
    3. 데이터 웨어하우스 로드
- `ELT`: Extract Load and Transform (데이터 분석가(Analytics Engineer)가 진행)
    1. DW(혹은 DL)로부터 데이터를 읽어 다시 DW에 쓰는 ETL
    2. Raw Data를 읽어서 일종의 리포트 형태나 써머리 형태의 테이블을 다시 만드는 용도
    3. 특수한 형태로는 AB 테스트 결과를 분석하는 데이터 파이프라인도 존재

### Airflow
- 데이터 파이프라인을 도와주는 프레임워크
    - DAG 기반
    - Python 기반
    - ETL 작성 뿐만 아니라, 좋은 ETL을 작성할 수 있는 라이브러리들이 구현됨 (재실행 가능 등...)
- 많은 3-rd party 컴포넌트와 연동이됨 (DB 등...)
- 버전은 최신버전보다는 안정된 버전(많은 회사들이 사용하는 버전)을 사용하는 것이 좋음
    - https://cloud.google.com/composer/docs/concepts/versioning/composer-versions
- DAG
    - Directed Acyclic Graph (사이클이 있으면 안 됨)
    - Task들의 집합
        - Task: Operator의 집합
- 장점
    1. DAG를 통한 태스크 및 종속성 제어
    1. 데이터 파이프라인 관리가 용이
    1. `backfill`이 쉬움
- 단점
    1. 상대적으로 학습이 오래 걸림
    1. 긴 배포 주기 (개발, 배포, 테스트)
        - 컴퓨터에서 DAG를 구축 및 테스트하기 어렵다
    1. Worker 수가 많을 경우 관리가 어려움
        - GCP: “Cloud Composer” 제공
        - AWS: “Managed Workflows for Apache Airflow” 제공

#### 컴포넌트
1. Web Server (Flask로 구현됨)
1. Scheduler
1. Worker
    - Worker Node 추가 → Scale Out (마스터 노드(웹서버, 스케줄러) + 워커 노드)
    - But, Worker가 늘어나면 운영이 어려워짐 (어디선가 고장날 확률이 높음)
    - Executor: 워커가 일하는 방식을 설정
1. Database (Sqlite가 기본으로 설치됨)
1. Queue 
    - 멀티 노드 구성인 경우에만 사용됨
    - Executor에 따라서 달라진다

#### 고려해야할 점 (Best Practices)
- 데이터 파이프라인은 다양한 이유로 인해서 실패할 확률이 높다

1. Full Refresh
    - 데이터가 작을 경우 매번 통채로 복사해서 테이블 만들기
    - 데이터가 크다면 시간 때문에 `Incremental Update`가 필요
        - Incremental Update: 새 레코드 및 수정된 레코드를 추가할 때 데이터 세트의 부분 업데이트를 수행
1. `멱등성` 보장
    - 동일한 입력 데이터로 데이터 파이프라인을 다수 실행해도 최종 테이블의 내용이 달라지지 말아야함
    - 예를 들면 중복 데이터가 생기지 말아야함
1. `재실행`
    - 실패한 데이터 파이프라인은 쉽게 재실행
    - 과거의 데이터를 채우는 `Backfill`이 쉬워야함
    - Airflow의 경우 강점을 가짐
        - catchup=True, start_date, end_date 적절하게 설정
        - incremental update가 필요한 경우에만 사용
        - execution_date: Airflow가 제공해주는 시스템 변수로, 데이터의 날짜와 시간이 들어옴
1. 주기적으로 쓸모없는 데이터들을 삭제
    - 데이터 파이프라인 입력과 출력을 명확히 문서화
1. 사고는 반드시 발생한다
    - 사고시 마다 사고 리포트(post-mortem) 쓰기
        - 동일한 사고가 발생하는 것을 막기 위함
    - 중요 데이터 파이프라인의 입력과 출력을 체크

#### Backfill 
- 상황
    - 2020년 11월 7일 부터 매일매일 데이터를 가져온다고 가정  
    - ETL이 동작하는 날짜? 2020년 11월 8일
    - Airflow에 적용
        - start_date: 2020-11-07 (처음 읽어와야하는 데이터의 날짜)
- Incremental하게 1년치 Backfill
    - 방법 1
        - 실패하지 않으면 문제가 없음
        - 실패한다면 2일전 데이터를 가져오기 힘듬
    ```python
    from datetime import datetime, timedelta

    # 이렇게 수동으로 날짜를 지정할 시에는 실수할 확률이 높다
    y = datetime.now() - timedelta(1)
    yesterday = datetime.strftime(y, '%Y-%m-%d')

    # execution_date를 활용하는 것이 실수 확률이 적다
    day = context['execution_date']
    ```
    - 방법 2
        - Airflow인 경우, 성공/실패의 날짜를 가지고 있음
        - 이를 바탕으로 데이터를 갱신하도록 코드를 작성해야함

#### 예시
```python
from datetime import datetime, timedelta
from airflow import DAG
default_args = {
    'owner': 'keeyong',
    'start_date': datetime(2020, 8, 7, hour=0, minute=00),
    'end_date': datetime(2020, 8, 31, hour=23, minute=00),
    'email': ['keeyonghan@hotmail.com'],
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

test_dag = DAG(
    # DAG name
    "dag_v1", 
    # schedule (same as cronjob)
    # 매일 오전 9시에 실행해라
    schedule_interval="0 9 * * *",
    # common settings
    default_args=default_args
)

start = DummyOperator(dag=dag, task_id="start", *args, **kwargs)
t1 = BashOperator(
    task_id='ls1',
    bash_command='ls /tmp/downloaded',
    retries=3,
    dag=dag)
t2 = BashOperator(
    task_id='ls2',
    bash_command='ls /tmp/downloaded',
    dag=dag)
end = DummyOperator(dag=dag, task_id='end', *args, **kwargs)

start >> [t1, t2] >> end
```

# 4. Airflow Deepdive
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
- 주의! 관리나 테스트가 되지 않아 사고로 이어질 가능성이 있음
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

#### Airflow Connection
- Connection 정보를 Airflow에 등록하여 사용할 수 있음
```python
def get_Redshift_connection(autocommit=False):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()
```

#### Xcom 객체
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

## 운영 서버 데이터 가져오기
- MySQL > Amazon S3 > Redshift
    ```python 
    s3_folder_cleanup = S3DeleteObjectsOperator(
        task_id = 's3_folder_cleanup',
        bucket = s3_bucket,
        keys = s3_key,
        aws_conn_id = "aws_conn_id",
        dag = dag
    )

    mysql_to_s3_nps = MySQLToS3Operator(
        task_id = 'mysql_to_s3_nps',
        query = "SELECT * FROM prod.nps",
        s3_bucket = s3_bucket,
        s3_key = s3_key,
        mysql_conn_id = "mysql_conn_id",
        aws_conn_id = "aws_conn_id",
        verify = False,
        dag = dag
    )

    s3_to_redshift_nps = S3ToRedshiftOperator(
        task_id = 's3_to_redshift_nps',
        s3_bucket = s3_bucket,
        s3_key = s3_key,
        schema = schema,
        table = table,
        copy_options=['csv'],
        method = 'REPLACE',
        redshift_conn_id = "redshift_dev_db",
        aws_conn_id = "aws_conn_id",
        dag = dag
    )

    s3_folder_cleanup >> mysql_to_s3_nps >> s3_to_redshift_nps
    ```

- MySQL 테이블의 Incremental Update 방식
    - 필요한 칼럼이 존재
        - created(timestamp): Optional
        - modified(timestamp)
        - deleted(boolean): 레코드를 삭제하지 않고 deleted를 True로 설정
    - Daily Update
        1. Redshift의 테이블 내용을 temp로 복사
        1. MySQL의 테이블의 레코드 중 수정된 날짜(modified)가 복사 대상이 되는 일자(execution_date)에 해당하는 모든 레코드를 temp로 복사
        ```sql
        SELECT * FROM A WHERE DATE(modified) = DATE(execution_date)
        ```
        1. temp 레코드를 primary key를 기준으로 파티션한 다음 modified 값을 기준으로 DESC 정렬해서 일련 번호가 1인 것들만 다시 테이블로 복사

    ```python
    s3_folder_cleanup = S3DeleteObjectsOperator(
        task_id = 's3_folder_cleanup',
        bucket = s3_bucket,
        keys = s3_key,
        aws_conn_id = "aws_conn_id",
        dag = dag
    )

    mysql_to_s3_nps = MySQLToS3Operator(
        task_id = 'mysql_to_s3_nps',
        query = "SELECT * FROM prod.nps WHERE DATE(created_at) = DATE('{{ execution_date }}')", # {{system variable}}: Airflow의 시스템 변수를 가져온다
        s3_bucket = s3_bucket,
        s3_key = s3_key,
        mysql_conn_id = "mysql_conn_id",
        aws_conn_id = "aws_conn_id",
        verify = False,
        dag = dag
    )

    s3_to_redshift_nps = S3ToRedshiftOperator(
        task_id = 's3_to_redshift_nps',
        s3_bucket = s3_bucket,
        s3_key = s3_key,
        schema = schema,
        table = table,
        copy_options=['csv'],
        redshift_conn_id = "redshift_dev_db",
        primary_key = "id",
        order_key = "created_at",
        dag = dag
    )

    s3_folder_cleanup >> mysql_to_s3_nps >> s3_to_redshift_nps
    ```

## Airflow에서 Backfill하기
- 커맨드라인
    - catchUp이 True로 되어있음
    - execution_date를 활용해서 Incremental update로 구현되어 있음
        - full refresh라면 backfill이 필요하지 않음
    ```powershell
    # start_date 시작
    # end_date는 포함하지 않음
    airflow dags backfill dag_id -s 2018-07-01 -e 2018-08-01
    ```

## Summary Table 구현
```python
def execSQL(**context):
    schema = context['params']['schema'] 
    table = context['params']['table']
    select_sql = context['params']['sql']

    cur = get_Redshift_connection()

    sql = f"""
    DROP TABLE IF EXISTS {schema}.temp_{table};
    CREATE TABLE {schema}.temp_{table} AS 
    """
    sql += select_sql
    cur.execute(sql)

    # temp_table에 결과가 하나라도 있는지 체크
    # 결과가 없으면 에러 발생
    cur.execute(f"""SELECT COUNT(1) FROM {schema}.temp_{table}""")
    count = cur.fetchone()[0]
    if count == 0:
        raise ValueError(f"{schema}.{table} didn't have any record")

    try:
        # 원래 있던 테이블을 제거하고, temp 테이블으로 변경
        sql = f"""
        DROP TABLE IF EXISTS {schema}.{table};
        ALTER TABLE {schema}.temp_{table} RENAME to {table};
        """
        sql += "COMMIT;"
        cur.execute(sql)
    except Exception as e:
        cur.execute("ROLLBACK")
        raise AirflowException("")

dag = DAG(
    dag_id = "Build_Summary",
    start_date = datetime(2021,12,10),
    schedule_interval = '@once',    # 주기적으로 실행하지 않고, 필요할 때만 실행됨
    catchup = False
)

execsql = PythonOperator(
    task_id = 'execsql',
    python_callable = execSQL,
    params = {
        'schema' : 'keeyong',
        'table': 'channel_summary',
        'sql' : """SELECT
	      DISTINCT A.userid,
        FIRST_VALUE(A.channel) over(partition by A.userid order by B.ts rows between unbounded preceding and unbounded following) AS First_Channel,
        LAST_VALUE(A.channel) over(partition by A.userid order by B.ts rows between unbounded preceding and unbounded following) AS Last_Channel
        FROM raw_data.user_session_channel A
        LEFT JOIN raw_data.session_timestamp B ON A.sessionid = B.sessionid;"""
    },
    provide_context = True,
    dag = dag
)
```

# 6. Productionizing Airflow

#### Docker
- Image
    - 응용 프로그램 뿐만 아니라 모든 다른 환경까지 포함한 소프트웨어 패키지
- Container
    - Docker Image를 Docker Enginer에서 실행한 것
    - Docker Engine만 실행하면 그 위에서 다양한 소프트웨ㅔ어들을 충돌없이 실행 가능
- Kubernetes
    - 컨테이너 기반 서비스 배포/스케일/관리 자동화
    - 다수 서버의 컨테이너 기반 프로그램을 관리
        - Pod: 같은 디스크와 네트워크를 공유하는 1개 이상의 컨테이너 집합
- Airflow와 Kubernetes
    - Dag 수가 많아지면 Worker 노드에서 task들을 실행하는 것이 어려워짐
        - 노드간에 충돌 발생
        - 다수의 워커 노드들을 Airflow 전용으로 쓰는 것이 낭비가 될 수 있음
    - Airflow에서 K8S를 Worker 노드 대용으로 사용
        - KubernetesExecutor 먼저 사용
        - Airflow task 코드를 Docker 이미지로 만듬

#### DBT (Data Build Tool)
- ELT용 오픈소스
- 다양한 DW 지원
    - Redshift, Snowflake, Bigquery, Spark
- 다수의 컴포넌트로 구성
    - Models
        1. 데이터 모델 
        1. data lineage 추적 가능 (특정 필드의 값이 어느 테이블에서 온 것인가?)
        1. data discovery tool (업무에 사용할만한 데이터/대시보드를 찾는다)
    - Seeds: 입력 데이터
    - Tests: 데이터 체크
    - Snapshots: 스냅샷
- https://www.startdataengineering.com/post/dbt-data-build-tool-tutorial/
- 

## 운영시 Airflow Configuration 
- `airflow.cfg`를 변경한다.
    - dag_dir_list_interval: dags_folder를 Airflow가 얼마나 자주 스캔하는지 명시
    - Database Upgrade: Sqlite > Postgres or MySQL (DB 주기적 백업 필요)
    - Executor 변경: LocalExecuter(Single Server), KubernetesExecutor(Cluster)
- 보안 강화
    - 인증 & 비밀번호 사용
    - Logs 파일 관리를 잘 하자! 
        - 꽉 차면 Airflow가 동작하지 않음
    - 일정 기간(1달 정도) 유지 하거나, 저렴한 아카이브에 저장
- Health-Check 모니터링 
    - api를 활성화하고, Health Check API를 모니터링 툴과 연동
    - DataDog, Grafana 등 사용이 일반적
- 스케일 업을 할 때 Cloud Airflow Option을 사용하는 것이 편함
    - GCP Cloud Compose, AWS MWAA
    - 가격이 비싸다

#### Slack 연동하기
- Task가 실패하거나, 끝난 경우 알림을 준다
- 방법 (https://api.slack.com/messaging/webhooks)
    1. Create New App > From scratch
    1. Incoming Webhooks > Activate Incoming Webhooks (On)
    1. Add New Webhook to Workspace > 채널 설정 > 제공되는 URL 카피

    ```python
    dag_second_assignment = DAG(
        dag_id = 'test',
        start_date = datetime(2022,10,6),
        schedule_interval = '0 2 * * *', 
        max_active_runs = 1,
        catchup = False,
        default_args = {
            'retries': 1,
            'retry_delay': timedelta(minutes=3),
            'on_failure_callback': slack.on_failure_callback, # Dag가 실패했을 때 실행되는 함수
        }
    )

    def on_failure_callback(context):
    text = str(context['task_instance'])
    text += "```" + str(context.get('exception')) +"```"
    send_message_to_a_slack_channel(text, ":scream:")

    # def send_message_to_a_slack_channel(message, emoji, channel, access_token):
    def send_message_to_a_slack_channel(message, emoji):
        # url = "https://slack.com/api/chat.postMessage"
        url = "https://hooks.slack.com/services/"+Variable.get("slack_url")
        headers = {
            'content-type': 'application/json',
        }
        data = { "username": "Data GOD", "text": message, "icon_emoji": emoji }
        r = requests.post(url, json=data, headers=headers)
        return r
    ```

#### API 활용
- Airflow API 활성화 과정
    1. `airflow.cfg` >> `auth_backend = airflow.api.auth.backend.basic_auth`
    1. `sudo systemctl restart airflow-webserver`
    1. `sudo su airflow` >> `airflow config get-value api auth_backend`
    1. Web UI에서 새로운 사용자 추가 (Security > List Users)
- Health API 호출
    - `curl -X GET --user "Monitor:MonitorUser1" http://[AirflowServer]:8080/health`
- API를 통해서 외부에서 DAG 리스트를 받거나, 실행할 수 있음

#### Airflow 로그 파일 삭제하기
- 로그 파일이 쌓이는 디렉토리를 airflow.cfg에서 확인 가능
    ```powershell
    [logging]
    # The folder where airflow should store its log files
    # This path must be absolute
    base_log_folder = /var/lib/airflow/logs
    [scheduler]
    child_process_log_directory = /var/lib/airflow/logs/scheduler
    ```
- 제거하는 커맨드를 작성하여 Dag로 구성
    ```python
    def return_bash_cleanup_command(log_dir, depth, days):
        return  "find {log_dir} -mindepth {depth} -mtime +{days} -delete && find {log_dir} -type d -empty -delete".format(
            log_dir=log_dir,
            days=days,
            depth=depth
        )


    def return_bash_cleanup_for_scheduler_command(log_dir, depth, days):
        return "find {log_dir} -mindepth {depth} -mtime +{days} -exec rm -rf '{{}}' \;".format(
            log_dir=log_dir,
            days=days,
            depth=depth
        )
    ```

#### Airflow 메타데이터 백업
- DB가 외부에 있다면 주기적인 백업 셋업 (AWS RDS)
- Airflow와 같은 서버에 메타 DB가 있다면 DAG 등을 이용해 주기 백업 실행
    ```python
    def main(**context):
        # 백업할 위치
        s3_bucket = Variable.get("data_s3_bucket")  
        folder = 'airflow_backup'
        dbname = context["params"]["dbname"]

        datem = datetime.datetime.today().strftime("%Y-%m-%d")
        year_month_day = str(datem)
        filename = dbname + "-" + year_month_day + ".sql"
        local_filename = Variable.get('local_data_dir') + filename
        s3_key = f"{folder}/{filename}"
        
        # 백업을 위한 pg_dump 명령어를 실행한다 > File이 생성된다
        cmd = f"pg_dump {dbname}"
        file_ops.run_cmd_with_direct(cmd, local_filename)
        
        if os.stat(local_filename).st_size == 0:
            raise AirflowException(local_filename + " is empty")

        # 생성된 File을 S3로 전송한다
        s3_hook = S3Hook('aws_s3_default')
        s3_hook.load_file(
            filename=local_filename,
            key=s3_key,
            bucket_name=s3_bucket,
            replace=True
        )

        # Local 파일은 제거
        os.remove(local_filename)
    ```

### Dag Dependecies

#### TriggerDagOperator
- Dag A가 Dag B를 트리거링

```python
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
trigger_B = TriggerDagRunOperator(
    task_id="trigger_B",
    trigger_dag_id="Dag B" # 트리거하려는DAG이름
    )

trigger_B = TriggerDagRunOperator(
    task_id="trigger_B",
    trigger_dag_id="Dag B" # 트리거하려는DAG이름
    
    # DAG B에 넘기고 싶은 정보. DAG B에서는 Jinja 템플릿(dag_run.conf["path"])으로 접근 가능.
    # DAG B PythonOperator(**context)에서라면 kwargs['dag_run'].conf.get('conf')
    conf={ 'path': '/opt/ml/conf' },
    # Jinja 템플릿을 통해 DAG A의 execution_date를 전달
    execution_date="{{ ds }}",
    # execution_date에 이미 실행됐더라도 재실행할지 여부
    reset_dag_run=True,
    # DAG B가 끝날 때까지 기다릴지 여부를 결정. 디폴트값은 False
    wait_for_completion=True 
    )
```

#### ExternalTaskSensor
- Dag B가 Dag A가 종료되었는지 계속 확인한 후, 스스로 트리거링
    - 같은 schedule_interval을 사용
    - Execution Date가 동일해야함
```python
from airflow.sensors.external_task import ExternalTaskSensor
waiting_for_end_of_dag_a = ExternalTaskSensor(
    task_id='waiting_for_end_of_dag_a',
    external_dag_id='Dag A',    # 기다릴 Dag
    external_task_id='end',     # 기다릴 Task
    timeout=5*60,       # 정해진 시간동안 기다린다
    mode='reschedule'   
    # Dag가 서로 다른 schedule_interval을 다른다면 상세 설정 필요 (Dag A가 Dag B보다 5분 먼저 실행됨)
    execution_delta=timedelta(minutes=5)  
    )
```

#### BranchPythonOperator
- 뒤에 실행되어야할 Task를 동적으로 결정해줌
```python 
# 개발에서는 Dag B를 트리거하지 않아도 된다
def skip_or_cont_trigger():
    if Variable.get("mode", "dev") == "dev":
        return []
    else:
        return ["trigger_b"]  # 리스트 형태로 여러 개 트리거 가능

branching = BranchPythonOperator(
    task_id='branching',
    python_callable=skip_or_cont_trigger,
)
```