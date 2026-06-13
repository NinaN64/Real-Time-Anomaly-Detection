import json
import os
import re
from datasets import load_dataset

TARGET_PER_BUCKET = 5000
MIN_LENGTH = 300
OUTPUT_PATH = "Data/wikipedia.jsonl"

BUCKETS = {
    "wikipedia_science": [
        "physics", "chemistry", "biology", "astronomy", "mathematics",
        "science", "quantum", "molecule", "atom", "species", "evolution",
        "genetics", "neuroscience", "ecology", "geology", "climate",
        "enzyme", "protein", "cell", "dna", "rna", "virus", "bacteria",
        "telescope", "galaxy", "planet", "star", "element", "reaction",
        "compound", "laboratory", "experiment", "theory",
    ],
    "wikipedia_sports": [
        "football", "soccer", "basketball", "baseball", "tennis",
        "cricket", "rugby", "golf", "athletics", "swimming", "cycling",
        "boxing", "wrestling", "volleyball", "hockey", "skiing",
        "olympics", "championship", "tournament", "league", "cup",
        "player", "team", "coach", "stadium", "match", "athlete",
        "sport", "racing", "marathon",
    ],
    "wikipedia_politics": [
        "politics", "government", "parliament", "congress", "senate",
        "election", "president", "minister", "democracy", "republic",
        "constitution", "legislation", "policy", "treaty", "diplomacy",
        "war", "military", "revolution", "party", "vote", "campaign",
        "law", "court", "justice", "rights", "sovereignty", "nation",
        "state", "federal", "political", "administration",
    ],
}


def classify_article(title, text):
    title_lower = title.lower()
    first_para = text[:500].lower()
    combined = title_lower + " " + first_para
    for bucket, keywords in BUCKETS.items():
        if any(kw in combined for kw in keywords):
            return bucket
    return None


def clean_text(text):
    text = re.sub(r"\n{3,}", "\n\n", text)
    text = re.sub(r" {2,}", " ", text)
    return text.strip()


def main():
    os.makedirs("Data", exist_ok=True)

    print("Loading Wikipedia dataset...")
    dataset = load_dataset(
        "wikimedia/wikipedia",
        "20231101.en",
        streaming=True,
        split="train",
    )

    counts = {bucket: 0 for bucket in BUCKETS}
    total_written = 0

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        for article in dataset:
            title = article.get("title", "")
            text = clean_text(article.get("text", ""))

            if len(text) < MIN_LENGTH:
                continue

            bucket = classify_article(title, text)
            if bucket is None:
                continue
            if counts[bucket] >= TARGET_PER_BUCKET:
                continue

            excerpt = text[:1000]
            record = {"text": excerpt, "category": bucket}
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
