import json
import os
from datasets import load_dataset

TARGET_PER_BUCKET = 5000
MIN_LENGTH = 150
MAX_LENGTH = 2000
OUTPUT_PATH = "Data/yahoo.jsonl"

LABEL_MAP = {
    0: "yahoo_society",
    1: "yahoo_science",
    2: "yahoo_health",
    3: "yahoo_education",
    4: "yahoo_computers",
    5: "yahoo_sports",
    6: "yahoo_business",
    7: "yahoo_entertainment",
    8: "yahoo_relationships",
    9: "yahoo_politics",
}


def clean_text(text):
    if not text:
        return ""
    text = text.replace("\n", " ").replace("\r", " ").replace("<br />", " ")
    text = " ".join(text.split())
    return text.strip()


def main():
    os.makedirs("Data", exist_ok=True)

    print("Loading Yahoo Answers dataset...")
    dataset = load_dataset(
        "community-datasets/yahoo_answers_topics",
        "yahoo_answers_topics",
        split="train",
        streaming=True,
    )

    counts = {bucket: 0 for bucket in LABEL_MAP.values()}
    total_written = 0

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        for item in dataset:
            if all(v >= TARGET_PER_BUCKET for v in counts.values()):
                break

            label = item.get("topic")
            bucket = LABEL_MAP.get(label)
            if bucket is None:
                continue
            if counts[bucket] >= TARGET_PER_BUCKET:
                continue

            title = clean_text(item.get("question_title", ""))
            content = clean_text(item.get("question_content", ""))
            answer = clean_text(item.get("best_answer", ""))
            text = f"{title} {content} {answer}".strip()

            if len(text) < MIN_LENGTH:
                continue
            if len(text) > MAX_LENGTH:
                text = text[:MAX_LENGTH]

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