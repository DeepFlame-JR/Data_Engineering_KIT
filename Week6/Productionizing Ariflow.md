# Productionizing Airflow

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