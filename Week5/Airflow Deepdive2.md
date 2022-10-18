# Airflow Deepdive 2

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