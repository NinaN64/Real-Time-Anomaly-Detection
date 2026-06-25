import json
import os
from datasets import load_dataset

TARGET_PER_BUCKET = 5000
OUTPUT_PATH = "Data/agnews.jsonl"

LABEL_MAP = {
    1: "agnews_world",
    2: "agnews_sports",
    3: "agnews_business",
    4: "agnews_scitech",
}

def main():
    os.makedirs("Data", exist_ok=True)

    print("Loading AG News dataset...")
    dataset = load_dataset("sh0416/ag_news", split="train", streaming=True)

    counts = {bucket: 0 for bucket in LABEL_MAP.values()}
    total_written = 0

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        for item in dataset:
            if all(v >= TARGET_PER_BUCKET for v in counts.values()):
                break

            label = item.get("label")
            bucket = LABEL_MAP.get(label)
            if bucket is None:
                continue
            if counts[bucket] >= TARGET_PER_BUCKET:
                continue

            title = item.get("title", "").strip()
            description = item.get("description", "").strip()
            text = f"{title} {description}".strip()

            if len(text) < 50:
                continue

            record = {"text": text, "category": bucket}
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

            counts[bucket] += 1
            total_written += 1

            if total_written % 500 == 0:
                print(f"  Written {total_written} | {counts}")

    print(f"\nDone. Output: {OUTPUT_PATH}")
    for bucket, count in counts.items():
        print(f"  {bucket}: {count} docs")

if __name__ == "__main__":
    main()