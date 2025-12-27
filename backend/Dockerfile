# [Base Image] Python 3.14 + Debian 12 (bookworm) 기반의 초경량 이미지
FROM python:3.14-slim

# [Env] 파이썬 로그가 버퍼링 없이 즉시 출력되도록 설정 (도커 로그 확인용 필수)
ENV PYTHONUNBUFFERED=1

# [Workdir] 컨테이너 내부 작업 공간 설정
WORKDIR /app

# [Dependencies] 
# 1. requirements.txt 파일만 먼저 복사 (캐시 효율성을 위해)
COPY requirements.txt .

# 2. pip 업그레이드 및 라이브러리 설치
# --no-cache-dir: 설치 후 캐시 파일을 삭제하여 이미지 크기를 줄임
RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# [Source] 나머지 소스 코드(collector.py 등) 전체 복사
COPY . .

# [Command] 컨테이너 실행 시 수집기 가동
CMD ["python", "agv_manager.py"]