import paho.mqtt.client as mqtt
import pymysql
import json
import os
import time
import sys
from datetime import datetime

# ---------------------------------------------------------
# [설정] 환경 변수 로드 (Docker Compose에서 주입됨)
# ---------------------------------------------------------
MQTT_BROKER = os.getenv("MQTT_BROKER", "mosquitto")
MQTT_PORT = int(os.getenv("MQTT_PORT", 1883))
MQTT_TOPIC = os.getenv("MQTT_TOPIC", "factory/agv/+")

DB_HOST = os.getenv("DB_HOST", "iot-mysql")
DB_USER = os.getenv("DB_USER", "test_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "1234abcd!@#$")
DB_NAME = os.getenv("DB_NAME", "iot-db")

# [설정] 제어 명령을 보낼 토픽의 기본 경로
CONTROL_TOPIC_BASE = "factory/control"

# ---------------------------------------------------------
# [데이터베이스] 함수들
# ---------------------------------------------------------
def get_db_connection():
    """MySQL 데이터베이스 연결 객체를 반환 (재시도 로직 포함)"""
    return pymysql.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor,
        connect_timeout=10
    )

def init_db():
    """DB 연결을 시도하고 테이블이 없으면 생성"""
    print(f"⏳ [System] MySQL({DB_HOST}) 접속 시도 중...")

    while True:
        try:
            conn = get_db_connection()
            cursor = conn.cursor()

            # 테이블 생성
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS agv_logs (
                id INT AUTO_INCREMENT PRIMARY KEY,
                agv_id VARCHAR(50) NOT NULL,
                battery INT,
                status VARCHAR(50),
                recorded_at DATETIME DEFAULT CURRENT_TIMESTAMP
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
            """
            cursor.execute(create_table_sql)
            conn.commit()
            conn.close()
            print("💾 [System] DB 연결 성공 및 테이블 준비 완료!")
            break

        except pymysql.MySQLError as e:
            print(f"⚠️ DB 연결 실패 (3초 후 재시도): {e}")
            time.sleep(3)

def save_to_db(data):
    """수신된 데이터를 DB에 저장"""
    conn = None
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 현재 시간 (파이썬 컨테이너 기준)
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        sql = "INSERT INTO agv_logs (agv_id, battery, status, recorded_at) VALUES (%s, %s, %s, %s)"
        
        # 딕셔너리에서 안전하게 값 추출 (없으면 기본값)
        agv_id = data.get("agv_id", "Unknown")
        battery = data.get("battery", 0)
        status = data.get("status", "Unknown")
        
        cursor.execute(sql, (agv_id, battery, status, now))
        conn.commit()

        print(f"✅ [DB저장] {agv_id} | {status} | {battery}%")

    except Exception as e:
        print(f"❌ DB 저장 에러: {e}")

    finally:
        if conn: conn.close()

# ---------------------------------------------------------
# 데이터 분석 및 제어 명령 전송 함수
# ---------------------------------------------------------
def process_and_control(client, data):
    """
    수신된 데이터를 분석하고, 필요시 AGV에게 명령을 내림
    """

    agv_id = data.get("agv_id", "Unknown")
    battery = data.get("battery", 0)
    status = data.get("status", "Unknown") # 현재 상태 확인
    
    # 배터리가 부족하더라도, 이미 '충전중'이거나 '복귀중'이면 명령을 안 보냄!
    # 오직 'MOVING' (일하고 있을 때) 상태일 때만 복귀 명령을 내려야 함.
    if battery < 20 and status == "MOVING":
        print(f"⚡ [판단] {agv_id} 배터리 부족({battery}%) -> 복귀 명령 생성")
        
        # 보낼 명령 데이터 생성 (JSON)
        command_payload = {
            "target_id": agv_id,
            "command": "RETURN_TO_BASE",
            "reason": "Low Battery",
            "timestamp": time.time()
        }
        
        # ID 추출 (factory/agv/001 -> 001)
        short_id = agv_id.split('/')[-1]
        target_topic = f"{CONTROL_TOPIC_BASE}/{short_id}"
        
        # 명령 전송 (Publish)
        client.publish(target_topic, json.dumps(command_payload))
        print(f"📡 [명령 발송] To: {target_topic} | Cmd: RETURN_TO_BASE")

# ---------------------------------------------------------
# [MQTT] 이벤트 핸들러 (Paho v2.0 대응)
# ---------------------------------------------------------
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        print(f"🔌 [MQTT] 브로커({MQTT_BROKER}) 연결 성공!")
        client.subscribe(MQTT_TOPIC)
        print(f"👂 [MQTT] 구독 시작: {MQTT_TOPIC}")
    else:
        print(f"❌ [MQTT] 연결 실패 (Code: {rc})")

def on_message(client, userdata, message):
    try:
        payload = message.payload.decode("utf-8")

        # JSON 파싱
        data = json.loads(payload)
        
        # DB 저장
        save_to_db(data)

        # 분석 및 제어
        process_and_control(client, data)
        
    except json.JSONDecodeError:
        print(f"⚠️ JSON 형식이 아님: {payload}")
    except Exception as e:
        print(f"⚠️ 메시지 처리 중 오류: {e}")

# ---------------------------------------------------------
# [Main] 프로그램 실행
# ---------------------------------------------------------
if __name__ == "__main__":
    # 1. DB 준비 (DB가 켜질 때까지 여기서 대기함)
    init_db()

    # 2. MQTT 클라이언트 설정
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_connect = on_connect
    client.on_message = on_message

    # 3. 브로커 연결 시도
    print(f"🚀 AGV 매니저 가동 시작...")
    try:
        client.connect(MQTT_BROKER, MQTT_PORT)
        client.loop_forever()
    except Exception as e:
        print(f"❌ 치명적 오류 발생: {e}")