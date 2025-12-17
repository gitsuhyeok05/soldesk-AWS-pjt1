import os
import boto3
import pymysql
import csv

# 환경 변수
s3 = boto3.client('s3')

DB_HOST = os.environ['DB_HOST']
DB_USER = os.environ['DB_USER']
DB_PASS = os.environ['DB_PASS']
DB_DB   = os.environ['DB_DB']
S3_BUCKET = os.environ['S3_BUCKET']

def lambda_handler(event, context):
    # S3 이벤트 정보 가져오기
    record = event['Records'][0]
    bucket_name = record['s3']['bucket']['name']
    object_key  = record['s3']['object']['key']
    
    print(f"S3 이벤트 발생 - 버킷: {bucket_name}, 키: {object_key}")

    # S3 객체 Metadata 가져오기
    response = s3.head_object(Bucket=bucket_name, Key=object_key)
    metadata = response.get('Metadata', {})
    action_code = metadata.get('action_code')
    player_id = metadata.get('player_id')

    if action_code is not None:
        action_code = int(action_code)  # string → int 변환
        print(f"action_code: {action_code}")
    else:
        print("action_code Metadata 없음")

    if player_id is not None:
        player_id = int(player_id) 
        print(f"player_id: {player_id}")
    else:
        print("player_id Metadata 없음")

    # CSV 다운로드
    csv_file_path = f"/tmp/{object_key.split('/')[-1]}"
    s3.download_file(bucket_name, object_key, csv_file_path)
    print(f"CSV 다운로드 완료: {csv_file_path}")

     # --- RDS 연결 ---
    conn = pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASS,
        db=DB_DB,
        port=3306,
        cursorclass=pymysql.cursors.DictCursor
    )

    try:
        with conn.cursor() as cursor:
            # CSV 읽어서 DB 업데이트
            with open(csv_file_path, newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # 예: gold와 last_action 업데이트
                    gold = int(row.get('gold', 0))
                    last_action = row.get('last_action', 'unknown')

                    sql = "UPDATE gamedatas SET gold=%s, last_action=%s WHERE id=%s"
                    cursor.execute(sql, (gold, last_action, player_id))
                    print(f"DB 업데이트 완료: player_id={player_id}, gold={gold}, last_action={last_action}")

            conn.commit()
    finally:
        conn.close()

    try:
        with conn.cursor() as cursor:
            # 현재 골드 조회
            cursor.execute("SELECT gold FROM gamedatas WHERE user_id=%s", (player_id,))
            row = cursor.fetchone()
            if not row:
                return {"statusCode": 404, "body": f"user_id {player_id} not found"}

            gold = row['gold']
            last_action = ""

            # action_code에 따라 처리
            match action_code:
                case 1:
                    last_action = "feed"
                case 2:
                    last_action = "play"
                case 3:
                    last_action = "clean"
                case 4:
                    gold -= 300   # 골드 감소
                    last_action = "buy_item_-300"
                case 5:
                    last_action = "buy_item_fail"
                case 6:
                    gold += 100  # 골드 증가
                    last_action = "gold_+100"
                case _:
                    last_action = f"action_{action_code}"

            # DB 업데이트
            sql = "UPDATE gamedatas SET gold=%s, last_action=%s WHERE user_id=%s"
            cursor.execute(sql, (gold, last_action, player_id))
            conn.commit()
            print(f"DB 업데이트 완료: user_id={player_id}, gold={gold}, last_action={last_action}")
    finally:
        conn.close()

    return {
        'statusCode': 200,
        'body': f"S3 파일 처리 완료: {object_key}, action_code={action_code}"
    }
