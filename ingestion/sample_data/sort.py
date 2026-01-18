import os
import json
from glob import glob
from datetime import datetime

# =========================
# 경로 설정
# =========================
INPUT_BASE = r"\dataset\output2"
OUTPUT_BASE = r"\dataset\output3"

# =========================
# 유틸
# =========================
def parse_ts(ts: str) -> datetime:
    """
    ISO8601 문자열 → datetime (UTC)
    """
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


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

            print(f"▶ 정렬 중: {server_name} / {date_name}")

            # 1️⃣ 로드
            records = []
            with open(input_file, "r", encoding="utf-8") as f:
                for line in f:
                    record = json.loads(line)
                    record["_parsed_ts"] = parse_ts(record["event_ts"])
                    records.append(record)

            if not records:
                continue

            # 2️⃣ event_ts 기준 정렬
            records.sort(key=lambda r: r["_parsed_ts"])

            # 3️⃣ 출력 디렉토리 생성
            output_dir = os.path.join(OUTPUT_BASE, server_name, date_name)
            os.makedirs(output_dir, exist_ok=True)

            output_file = os.path.join(output_dir, "events.json")

            # 4️⃣ 저장
            with open(output_file, "w", encoding="utf-8") as f:
                for r in records:
                    r.pop("_parsed_ts", None)
                    f.write(json.dumps(r, ensure_ascii=False) + "\n")

            print(f"✅ 완료: {output_file}")

    print("🎉 전체 정렬 완료 (output3 생성)")


if __name__ == "__main__":
    main()
