import json
import os
import re

TARGET_PER_BUCKET = 5000
MIN_LENGTH = 100
OUTPUT_PATH = "Data/arxiv.jsonl"

BUCKET_MAP = {
    "cs":      "arxiv_cs",
    "math":    "arxiv_math",
    "q-bio":   "arxiv_qbio",
    "econ":    "arxiv_econ",
    "astro-ph":"arxiv_astro",
    "stat":    "arxiv_stat",
}

KAGGLE_PATH = r"C:\Users\ninoc\.cache\kagglehub\datasets\Cornell-University\arxiv\versions\291\arxiv-metadata-oai-snapshot.json"


def classify_paper(categories_str):
    if not categories_str:
        return None
    primary = categories_str.strip().split()[0].lower()
    for prefix, bucket in BUCKET_MAP.items():
        if primary.startswith(prefix):
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

    counts = {bucket: 0 for bucket in BUCKET_MAP.values()}
    total_written = 0

    print(f"Reading {KAGGLE_PATH} ...")

    with open(KAGGLE_PATH, "r", encoding="utf-8") as src, \
         open(OUTPUT_PATH, "w", encoding="utf-8") as out:

        for line in src:
            if all(v >= TARGET_PER_BUCKET for v in counts.values()):
                break

            try:
                paper = json.loads(line)
            except json.JSONDecodeError:
                continue

            abstract = clean_abstract(paper.get("abstract", ""))
            if len(abstract) < MIN_LENGTH:
                continue

            bucket = classify_paper(paper.get("categories", ""))
            if bucket is None:
                continue
            if counts[bucket] >= TARGET_PER_BUCKET:
                continue

            record = {"text": abstract, "category": bucket}
            out.write(json.dumps(record, ensure_ascii=False) + "\n")

            counts[bucket] += 1
            total_written += 1

            if total_written % 500 == 0:
                print(f"  Written {total_written} | {counts}")

    print(f"\nDone. Output: {OUTPUT_PATH}")
    for bucket, count in counts.items():
        print(f"  {bucket}: {count} docs")


if __name__ == "__main__":
    main()