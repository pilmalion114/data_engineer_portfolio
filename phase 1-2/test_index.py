import pymysql
import time

print("=" * 60)
print("🔬 인덱스 성능 비교 실험")
print("=" * 60)

## 1. DB 연결
conn = pymysql.connect(
    host='127.0.0.1',
    user='root',
    password='1234',
    database='shop_db'
)

cursor = conn.cursor()

# 테스트 쿼리들 -> 튜플 리스트로 만듦. 2차원 배열 구조랑 비슷하고(이는 또 테이블 형식과 유사하고), 하지만 튜플은 2차원 배열과 다르게 수정이 불가능하다. 
# 왜 튜플을 사용할까? -> 일단 가장 큰 이유는, 1. DB가 튜플로 반환함.(조회 결과는 수정되면 안 됨. 성능 최적화. 표준 관례의 이유로..) 2. 튜플이 리스트보다 (메모리 적게 쓰고, 생성 속도 빠르고, 반복 속도 빠름). 3. 튜플은 딕셔너리 키로 사용 가능(리스트는 불가능) 4. 언패킹이 편함. 등등...
test_queries = [
    ("특정 사용자 조회", "select * from user_logs where user_id = 5000"),
    ("사용자별 카운트", "select user_id, count(*) from user_logs where user_id between 1000 and 2000 group by user_id"),
    ("최근 로그 조회", "select * from user_logs where user_id=1234 order by created_at desc limit 10 ")
]

print("\n" + "=" * 60)
print("📊 1단계: 인덱스 없이 실행")
print("=" * 60)

results_without_index = []

for name, query in test_queries:
    print(f"\n🔍 테스트: {name}")
    print(f"쿼리: {query[:60]}...") # 60글자만 출력

    # 시간 측정
    start = time.time()
    cursor.execute(query) # 실제 실행하는 코드
    result = cursor.fetchall()
    elapsed = time.time() - start

    results_without_index.append(elapsed)
    print(f"⏱️  실행 시간: {elapsed:.4f}초")
    print(f"📝 결과 수: {len(result)}건") # 행의 갯수. 


# 인덱스 생성
print("\n" + "=" * 60)
print("🔧 2단계: 인덱스 생성")
print("=" * 60)

print("\n인덱스 생성 중...")
cursor.execute("CREATE INDEX idx_user_id on user_logs(user_id)") # 인덱스 생성 코드
conn.commit()
print("✅ 인덱스 생성 완료: idx_user_id (user_id 컬럼)")

# 인덱스 확인
cursor.execute("show index from user_logs")
indexes = cursor.fetchall()
print("\n📋 생성된 인덱스:")

for idx in indexes:
    print(f" - {idx[2]} (칼럼: {idx[4]})") # 이제서야 얘기하지만, 일일이 claude에게 질문하지 말고, 결과 직접 돌려서 나온 결과값들로 얻고 싶은 열들 직접 추리면 된다. 하지만, 이번 실습에서는 claude에게 질문을 통해 얻어갈 거임.
    #idx[2]: 인덱스 이름, idx[4]: 칼럼 이름


# 인덱스 있을 때 실행
print("\n" + "=" * 60)
print("📊 3단계: 인덱스 있을 때 실행")
print("=" * 60)

results_with_index = []

for name, query in test_queries:
    print(f"\n🔍 테스트: {name}")
    print(f"쿼리: {query[:60]}...")
    
    # 시간 측정
    start = time.time()
    cursor.execute(query)
    result = cursor.fetchall()
    elapsed = time.time() - start
    
    results_with_index.append(elapsed)
    print(f"⏱️  실행 시간: {elapsed:.4f}초")
    print(f"📝 결과 수: {len(result)}건")


# 결과 비교
print("\n" + "=" * 60)
print("📈 4단계: 성능 비교 결과")
print("=" * 60)

print("\n┌─────────────────────────┬──────────────┬──────────────┬───────────┐")
print("│ 테스트                  │ 인덱스 없음  │ 인덱스 있음  │ 속도 향상 │")
print("├─────────────────────────┼──────────────┼──────────────┼───────────┤")

for i, (name, _) in enumerate(test_queries): # i는 앞에 for 구문으로 돌린 3개의 쿼리를 의미함.(즉, i는 3임). (name, _)는 query에서 튜플의 요소들을 의미함. enumerate는 인덱스(숫자)를 추가하여 (인덱스,요소) 쌍으로 만들어주는 함수임.
    without = results_without_index[i]
    with_idx = results_with_index[i]
    improvement = without/with_idx if with_idx > 0 else 0

    print(f"│ {name:23s} │ {without:11.4f}초 │ {with_idx:11.4f}초 │ {improvement:8.1f}배 │")
    # name:23s -> 빈칸 포함 총 23칸으로 문자열로 출력. without:11.4f/with_idx:11.4f -> 11칸 너비로, 소숫점 4째자리까지 실수 형태로. improvement:8.1f -> 8칸 너비로, 소숫점 첫째자리까지 실수 형태로.

print("└─────────────────────────┴──────────────┴──────────────┴───────────┘")


# 평균 계산
avg_without = sum(results_without_index) / len(results_without_index)
avg_with = sum(results_with_index) / len(results_with_index)
avg_improvement = avg_without / avg_with if avg_with > 0 else 0

print(f"\n📊 평균 성능:")
print(f"  인덱스 없음: {avg_without:.4f}초")
print(f"  인덱스 있음: {avg_with:.4f}초")
print(f"  평균 {avg_improvement:.1f}배 빠름! 🚀")

cursor.close()
conn.close()


print("\n" + "=" * 60)
print("🎉 실험 완료!")
print("=" * 60)

