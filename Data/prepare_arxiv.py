import json
import os
import re
from datasets import load_dataset

TARGET_PER_BUCKET = 5000
MIN_LENGTH = 100
OUTPUT_PATH = "Data/arxiv.jsonl"

BUCKET_PREFIXES = {
    "arxiv_cs": ["cs."],
    "arxiv_physics": ["physics.", "cond-mat.", "quant-ph", "hep-", "astro-ph", "gr-qc", "nucl-"],
    "arxiv_math": ["math."],
}


def classify_paper(categories):
    if isinstance(categories, list):
        cats_lower = " ".join(categories).lower()
    else:
        cats_lower = categories.lower()
    for bucket, prefixes in BUCKET_PREFIXES.items():
        if any(cats_lower.startswith(p) or f" {p}" in cats_lower for p in prefixes):
            return bucket
    return None


def clean_abstract(text):
    text = re.sub(r"\$[^$]*\$", "", text)
    text = re.sub(r"\\\w+\{[^}]*\}", "", text)
    text = re.sub(r"\\\w+", "", text)
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip()


def main():
    os.makedirs("Data", exist_ok=True)

    print("Loading arxiv dataset...")
    dataset = load_dataset(
        "gfissore/arxiv-abstracts-2021",
        streaming=True,
        split="train",
    )

    counts = {bucket: 0 for bucket in BUCKET_PREFIXES}
    total_written = 0

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        for paper in dataset:
            abstract = clean_abstract(paper.get("abstract", ""))
            categories = paper.get("categories", "")

            if len(abstract) < MIN_LENGTH:
                continue

            bucket = classify_paper(categories)
            if bucket is None:
                continue
            if counts[bucket] >= TARGET_PER_BUCKET:
                continue

            record = {"text": abstract, "category": bucket}
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

            counts[bucket] += 1
            total_written += 1

            if total_written % 500 == 0:
                print(f"  Written {total_written} docs | {counts}")

            if all(v >= TARGET_PER_BUCKET for v in counts.values()):
                break

    print(f"\nDone. Output: {OUTPUT_PATH}")
    for bucket, count in counts.items():
        print(f"  {bucket}: {count} docs")


if __name__ == "__main__":
    main()