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
    - Worker Node 추가 → Scale Out
    - Worker가 늘어나면 운영이 어려워짐 (어디선가 고장날 확률이 높음)
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
    y = datetime.now() - timedelta(1)
    yesterday = datetime.strftime(y, '%Y-%m-%d')

    # yesterday에 해당하는 데이터를 소스에서 읽어옴
    # 예를 들어 프로덕션 DB의 특정 테이블에서 읽어온다면
    sql = f"SELECT * FROM table WHERE DATE(ts) = '{yesterday}'"
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


