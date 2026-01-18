import os
import json
from datetime import datetime, timedelta, timezone
from glob import glob

# =========================
# 설정
# =========================
INPUT_BASE = r"\dataset\output1"
OUTPUT_BASE = r"\dataset\output2"

DAY_SECONDS = 24 * 60 * 60
TARGET_SECONDS = 60 * 60  # 1시간 (15~16)

TARGET_HOUR = 15  # 15:00

# =========================
# 유틸
# =========================
def shift_to_peak_hour(event_ts: datetime):
    """
    하루(00~24시)를 15~16시로 선형 압축
    """
    day_start = event_ts.replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    target_start = day_start.replace(hour=TARGET_HOUR)

    offset_sec = (event_ts - day_start).total_seconds()
    new_ts = target_start + timedelta(
        seconds=offset_sec * (TARGET_SECONDS / DAY_SECONDS)
    )
    return new_ts


# =========================
# 메인 처리
# =========================
def main():
    for server_dir in glob(os.path.join(INPUT_BASE, "server=*")):
        server_name = os.path.basename(server_dir)

        for date_dir in glob(os.path.join(server_dir, "date=*")):
            date_name = os.path.basename(date_dir)

            input_file = os.path.join(date_dir, "events.json")
            if not os.path.exists(input_file):
                continue

            # 출력 디렉토리 생성
            output_dir = os.path.join(OUTPUT_BASE, server_name, date_name)
            os.makedirs(output_dir, exist_ok=True)

            output_file = os.path.join(output_dir, "events.json")

            print(f"▶ 변환 중: {server_name} / {date_name}")

            with open(input_file, "r", encoding="utf-8") as fin, \
                 open(output_file, "w", encoding="utf-8") as fout:

                for line in fin:
                    record = json.loads(line)

                    orig_ts = datetime.fromisoformat(
                        record["event_ts"].replace("Z", "+00:00")
                    )

                    new_ts = shift_to_peak_hour(orig_ts)

                    record["event_ts"] = new_ts.isoformat()
                    record["event_date"] = new_ts.date().isoformat()

                    fout.write(json.dumps(record, ensure_ascii=False) + "\n")

            print(f"✅ 완료: {output_file}")

    print("🎉 전체 15~16시 시간 압축 변환 완료")


if __name__ == "__main__":
    main()
