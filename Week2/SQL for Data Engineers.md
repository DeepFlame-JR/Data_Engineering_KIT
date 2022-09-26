# 2. SQL for Data Engineers

## SQL?
- RDBMS의 데이터를 관리 및 처리하기 위해 설계된 특수목적의 프로그래밍 언어
- 장점
    - 구조화 데이터를 다루는데 가장 좋은 언어 (코딩보다 좋다)
- 단점
    - 비구조화 데이터를 다루기 어려움
        - 작은 데이터: Pandas / 큰 데이터: Spark
    - 표준 문법이 존재하지 않는다. (다양한 SQL 문법 존재)
-  기본
    - 세미클론으로 분리
    - 주석 (--)
    - 테이블 이름 작성 규칙이 있는 것이 좋음

### 구분
- DDL(Data Definition Language, 데이터 정의어)  
    DB를 정의하는 언어. 테이블을 생성/제거/변경.
    - `CREATE`: DB, Table 등을 생성
    - `ALTER`: Table을 수정
    - `DROP`: DB, Table 등을 삭제 
- DML(Data Manipulation Language, 데이터 조작어): 테이블 레코드 조작
    - `SELECT`: 데이터 조회
    - `INSERT INTO`: 데이터 삽입
    - `UPDATE FROM`: 데이터 수정
    - `DELETE FROM`: 데이터 삭제
        - `TRUNCATE`: 테이블 초기화 (WHERE, Transaction 지원 X)

## Basic SQL
- 만약 반대 로직을 구현하고 싶다면 함수 앞에 `NOT`을 활용

- **WHERE절**
    ```sql
    -- IN
    WHERE channel in (‘Google’, ‘Youtube’)
    WHERE channel = ‘Google’ OR channel = ‘Youtube’
    NOT IN

    -- LIKE(대소문자 비교) and ILIKE(대소문자 무시)
    WHERE channel LIKE ‘G%’ -> ‘G*’
    WHERE channel LIKE ‘%o%’ -> ‘*o*’
    NOT LIKE or NOT ILIKE

    -- BETWEEN
    WHERE BETWEEN '20220101' AND '20221231'
    ```
- **String 함수**
    ```sql
    SELECT      
        LEFT(str, N)
        REPLACE(str, 'exp1', 'exp2')
        UPPER(str)
        LOWER(str)
        LEN(str)
        LPAD(str, N, 'exp')  -- 총 문자 길이 N으로 지정, 빈 칸은 exp로 채운다
        SUBSTRING(str, pos, len)  -- str에서 pos 번째 위치에서 len 개의 문자를 읽음
    FROM Table
    ```
- **ORDER BY**
    ```sql
    ORDER BY 1 ASC  --Default
    ORDER BY 1 DESC
    ORDER BY 1 DESC, 2, 3 -- 여러개
    
    -- NULL value ordering (기본값은 SQL마다 다름)
    ORDER BY 1 DESC NULLS FIRST; -- NULL값이 가장 앞에 옴
    ORDER BY 1 DESC NULLS LAST; -- NULL값이 맨뒤로 이동
    ```
- **Type Cast**
    ```sql
    -- DATE Conversion
    DATE('20220101') -- 결과: 2022-01-01
    DATE_TRUNC('day', created) AS day
    CONVERT_TIMEZONE('America/Los_Angeles', ts)
    
    -- Type Casting
    col1::int -- col1필드를 int로 type casting
    cast(col1 as int)

    -- TO_CHAR, TO_TIMESTAMP
    ```
- **NULL**
    - '값이 존재하지 않음'을 의미
    - NULL이 들어간 연산은 모두 NULL
    ```sql
    SELECT * 
    FROM Table
    WHERE col1 IS NULL or IS NOT NULL

    SELECT * FROM Table WHERE col1 IS True
    SELECT * FROM Table WHERE col1 IS False -- 둘의 값은 NULL 때문에 같지 않을 수 있음
    
    SELECT NULLIF(col1, 0) FROM Table -- col1 값이 NULL이라면 0
    SELECT COALESCE(col1, col2, col3) FROM Table -- 처음 만나는 NULL이 아닌 값 반환
    ```
- **COUNT**
    - NULL이 아닌 값에 대해 count를 한다
    ```sql
    -- 예시 TABLE [NULL, 1, 1, 0, 0, 4, 3]
    SELECT COUNT(1) FROM TABLE -- 7
    SELECT COUNT(val) FROM TABLE -- 6
    SELECT COUNT(DISTINCT val) FROM TABLE -- 4
    ```

## JOIN
- 두 개의 테이블을 조건에 맞게 합침
- 테이블의 중복 레코드가 없고 Primary Key의 Uniqueness가 보장됨 체크
- 문법
    ```sql
    SELECT A.*, B.*
    FROM raw_data.table1 A
    ____ JOIN raw_data.table2 B ON A.key1 = B.key1 and A.key2 = B.key2
    WHERE A.ts >= '2019-01-01';
    ```
- 종류
    - `INNER JOIN`: 양쪽 테이블에서 매치가되는 레코드만 리턴
    - `LEFT JOIN`: 왼쪽 테이블의 모든 레코드들을 리턴. 오른쪽 테이블의 필드는 매칭되는 경우만 채워짐
    - `RIGHT JOIN`: LEFT JOIN의 반대
    - `FULL OUTER JOIN`: 양쪽 테이블의 모든 레코드를 리턴
    - `SELF JOIN`: 동일한 테이블을 alias를 달리해서 자기자신과 조인
    - `CROSS JOIN`: 양쪽 테이블의 모든 레코드들의 조합을 리턴
    <img src="https://t1.daumcdn.net/cfile/tistory/1451913F4F021EB826" width="700">