# Data Engineering 총정리

## Ariflow CI/CD
1. Airflow Deployment Plugin
    - Webserver에서 설정해서 사용 (https://github.com/VorTECHsa/airflow-deploy-plugin)
    - 푸시하고 싶은 브랜치 선택 및 배포
1. Git Pull DAG
    - GitHub Repository의 deply 키를 갖고, bash 스크립트를 통해 git pull을 수행해주는 DAG를 구현
1. Github Actions (권장)
    - GitHub Repository에 있는 코드를 빌드/테스트하고 배포할 수 있게 해주는 서비스
    - PR Merge와 같은 이벤트에 의해 트리거됨
    - 튜토리얼
        - Github 일반 튜토리얼 (https://aerocode.net/375)

## AWS/Airflow 보안
- Identity and Acess Management 
- 사용자 권한을 제어
    - 특정 서비스에 대한 권한을 정의
    - 이 Role을 개인이나 그룹에 부여

## Airflow on Kubernetes
- K8S를 배우는 것이 쉽지 않음
- 대안: Helm을 이용해 K8S 위에 프로그램을 실행하는 것이 일반적
    - Helm으로 실행하는 프로그램을 Helm Chart
- 실습: https://marclamberti.com/blog/airflow-on-kubernetes-get-started-in-10-mins/
    - 다수의 Docker Container를 K8S 노드처럼 설정. 이를 위해 KinD(K8S 자체를 설정하는 것을 단순화하는 툴)사용
    ```
    1. K8S 클러스터를 KinD로 생성
    2. Airflow 자체를 K8S 클러스터 위에 실행
    3. Airflow가 K8S 클러스터를 Executor로 사용하도록 설정
    4. Kubernetes에 Airflow 이미지를 통해 설치
    5. GitSync를 활용하여 DAG를 배포한다 
    6. Logs 관리 (K8S를 통한 Log는 제대로 기록되지 않는 경우가 있음)
    ```

## 면접

#### 면접시 준비할 것
- 이력서에 적은 모든 내용에 대해 숙지할 것
- 자신감을 바탕으로 대답할 것
    - 단 모르는 것은 모른다고 할 것. 필요하다면 공부해서 사용할 수 있다고 할 것!
- 행동양식 관련 질문들 대비
- 프로젝트에 대한 답변은 단답형이 아닌 아래 룰에 맞춰 답변
    - STAR (Situation, Task, Action, Results)
- 내가 할 질문 준비할 것
    - 조인하게 되면 처음에 무슨 일을 하게 되나요? (hiring manager에게
물어봐야할 질문)
    - 지금 진행 중인 중요한 프로젝트는 무엇이 있나요?
    - 이 회사에 일을 잘 하는 사람들은 어떤 특징을 갖고 있나요?
    - 지금 회사에서 제일 좋았던 점 그리고 안 좋았던 점 하나를 설명해주실 수
있나요?
    - 어떻게 돈을 버나요?
    - 지금 사용 중인 데이터 관련 기술 스택에 대해 설명해주실 수 있나요?

#### 과거 행동과 관련된 질문
- Collaboration
    - 다른 팀에 속한 다수의 사람들과 했던 프로젝트에 대해서 이야기해주세요
    - 지금까지 가장 같이 일하기 힘들었던 사람들은 누구이고 왜 힘들었나요?
- Ownership
    - 프로젝트 중에 문제가 생겼는데 그걸 주도적으로 해결한 경우가 있다면 설명해주세요
    - 프로젝트를 처음부터 끝까지 진행한 경우가 있다면 이에 대해 설명해주세요
- Impact
    - 지금까지 한 프로젝트 중에 가장 임팩트가 컸던 프로젝트에 대해 설명해주세요
- Craftsmanship
    - 본인의 책임이 아니었음에도 불구하고 어떤 문제를 직접 해결하려고 했었던 적이 있다면 설명해주세요