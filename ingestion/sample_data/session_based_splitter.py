import os
import json
import hashlib
import re
from datetime import datetime, timezone

# =========================
# 설정값
# =========================
INPUT_DIR = r"C:\Users\ysjun5656\.ir_datasets\tripclick\logs"
OUTPUT_BASE_DIR = r"C:\Users\ysjun5656\Desktop\Lectures\메타코드\데이터 파이프라인 프로젝트\dataset\output"
NUM_PARTITIONS = 2

# =========================
# 정규식
# =========================
date_pattern = re.compile(r"/Date\((\d+)\)/")
filename_day_pattern = re.compile(r"(\d{4})-(\d{2})-(\d{2})\.json")
filename_month_pattern = re.compile(r"(\d{4})-(\d{2})\.json")

# =========================
# 유틸 함수
# =========================
def parse_tripclick_timestamp(date_str: str):
    """
    /Date(1451671878697)/ -> ISO-8601 UTC timestamp, YYYY-MM-DD
    """
    match = date_pattern.search(date_str)
    if not match:
        return None, None

    epoch_ms = int(match.group(1))
    dt = datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
    return dt.isoformat(), dt.date().isoformat()


def session_hash_partition(session_id: str, num_partitions=NUM_PARTITIONS):
    """
    SessionId 기반 안정적 해시 파티셔닝
    """
    if not session_id:
        return 0  # fallback
    h = hashlib.md5(session_id.encode("utf-8")).hexdigest()
    return int(h, 16) % num_partitions


def extract_date_from_filename(filename: str):
    """
    파일명에서 날짜 추출
    - YYYY-MM-DD.json
    - YYYY-MM.json
    """
    m_day = filename_day_pattern.match(filename)
    if m_day:
        return {
            "year": m_day.group(1),
            "month": m_day.group(2),
            "day": m_day.group(3),
        }

    m_month = filename_month_pattern.match(filename)
    if m_month:
        return {
            "year": m_month.group(1),
            "month": m_month.group(2),
            "day": None,
        }

    return None


def build_output_filename(date_info):
    """
    날짜 기반 출력 파일명 생성
    """
    if date_info["day"]:
        return f"{date_info['year']}-{date_info['month']}-{date_info['day']}_part-000.json"
    else:
        return f"{date_info['year']}-{date_info['month']}_part-000.json"


# =========================
# 메인 처리 로직
# =========================
def main():
    os.makedirs(OUTPUT_BASE_DIR, exist_ok=True)

    for filename in os.listdir(INPUT_DIR):
        if not filename.endswith(".json"):
            continue

        date_info = extract_date_from_filename(filename)
        if not date_info:
            print(f"⚠️ 날짜 형식 아님, 스킵: {filename}")
            continue

        input_path = os.path.join(INPUT_DIR, filename)
        print(f"▶ 처리 시작: {filename}")

        output_files = {}

        with open(input_path, "r", encoding="utf-8") as f:
            for line in f:
                record = json.loads(line)

                # timestamp 파싱
                event_ts, event_date = parse_tripclick_timestamp(
                    record.get("DateCreated", "")
                )
                record["event_ts"] = event_ts
                record["event_date"] = event_date

                # 파티션 계산
                session_id = record.get("SessionId", "")
                partition = session_hash_partition(session_id)

                # 출력 디렉토리 구성
                base_dir = os.path.join(
                    OUTPUT_BASE_DIR,
                    f"server={partition}",
                    f"year={date_info['year']}",
                    f"month={date_info['month']}",
                )

                if date_info["day"]:
                    base_dir = os.path.join(base_dir, f"day={date_info['day']}")

                os.makedirs(base_dir, exist_ok=True)

                # 🔥 날짜 포함 파일명
                out_filename = build_output_filename(date_info)
                output_path = os.path.join(base_dir, out_filename)

                if output_path not in output_files:
                    output_files[output_path] = open(
                        output_path, "a", encoding="utf-8"
                    )

                output_files[output_path].write(
                    json.dumps(record, ensure_ascii=False) + "\n"
                )

        # 파일 핸들 정리
        for fh in output_files.values():
            fh.close()

        print(f"✅ 처리 완료: {filename}")

    print("🎉 전체 파일 처리 완료")


if __name__ == "__main__":
    main()
