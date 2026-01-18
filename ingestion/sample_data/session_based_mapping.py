import os
import json
import hashlib
import re
from datetime import datetime, timezone, timedelta

# =========================
# 설정값
# =========================
INPUT_DIR = r"\.ir_datasets\tripclick\logs"
OUTPUT_BASE_DIR = r"\dataset\output1"
NUM_PARTITIONS = 2

TARGET_START = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
TARGET_DAYS = 31
TARGET_SECONDS = TARGET_DAYS * 24 * 60 * 60

# =========================
# 정규식
# =========================
date_pattern = re.compile(r"/Date\((\d+)\)/")

# =========================
# 유틸 함수
# =========================
def parse_tripclick_epoch(date_str: str):
    """
    /Date(1451671878697)/ -> epoch seconds (float)
    """
    m = date_pattern.search(date_str)
    if not m:
        return None
    return int(m.group(1)) / 1000.0


def session_hash_partition(session_id: str, num_partitions=NUM_PARTITIONS):
    if not session_id:
        return 0
    h = hashlib.md5(session_id.encode("utf-8")).hexdigest()
    return int(h, 16) % num_partitions


# =========================
# 1️⃣ 전체 데이터 최소/최대 timestamp 계산
# =========================
def scan_min_max_epoch():
    min_ts, max_ts = None, None

    for filename in os.listdir(INPUT_DIR):
        if not filename.endswith(".json"):
            continue

        with open(os.path.join(INPUT_DIR, filename), "r", encoding="utf-8") as f:
            for line in f:
                record = json.loads(line)
                ts = parse_tripclick_epoch(record.get("DateCreated", ""))
                if ts is None:
                    continue

                min_ts = ts if min_ts is None else min(min_ts, ts)
                max_ts = ts if max_ts is None else max(max_ts, ts)

    return min_ts, max_ts


# =========================
# 2️⃣ 메인 처리 로직
# =========================
def main():
    os.makedirs(OUTPUT_BASE_DIR, exist_ok=True)

    print("🔍 전체 데이터 timestamp 스캔 중...")
    min_ts, max_ts = scan_min_max_epoch()

    if min_ts is None or max_ts is None:
        raise RuntimeError("timestamp 스캔 실패")

    source_seconds = max_ts - min_ts
    print(f"✅ 원본 기간: {source_seconds/86400:.1f} days")

    open_files = {}

    for filename in os.listdir(INPUT_DIR):
        if not filename.endswith(".json"):
            continue

        input_path = os.path.join(INPUT_DIR, filename)
        print(f"▶ 처리 시작: {filename}")

        with open(input_path, "r", encoding="utf-8") as f:
            for line in f:
                record = json.loads(line)

                orig_ts = parse_tripclick_epoch(record.get("DateCreated", ""))
                if orig_ts is None:
                    continue

                # 🔥 선형 시간 압축
                offset_ratio = (orig_ts - min_ts) / source_seconds
                new_ts = TARGET_START + timedelta(seconds=offset_ratio * TARGET_SECONDS)

                record["event_ts"] = new_ts.isoformat()
                record["event_date"] = new_ts.date().isoformat()

                # 서버 분할
                session_id = record.get("SessionId", "")
                server_id = session_hash_partition(session_id)

                # 출력 경로 (🔥 파일명 기준 날짜 완전 제거)
                base_dir = os.path.join(
                    OUTPUT_BASE_DIR,
                    f"server={server_id}",
                    f"date={record['event_date']}",
                )
                os.makedirs(base_dir, exist_ok=True)

                output_path = os.path.join(base_dir, "events.json")

                if output_path not in open_files:
                    open_files[output_path] = open(output_path, "a", encoding="utf-8")

                open_files[output_path].write(
                    json.dumps(record, ensure_ascii=False) + "\n"
                )

        print(f"✅ 처리 완료: {filename}")

    # 파일 핸들 정리
    for fh in open_files.values():
        fh.close()

    print("🎉 전체 데이터 처리 완료 (2026-01 매핑 완료)")


if __name__ == "__main__":
    main()
