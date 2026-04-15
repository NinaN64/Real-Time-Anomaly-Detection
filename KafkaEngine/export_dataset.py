import argparse
import json
import os

from sklearn.datasets import fetch_20newsgroups


def export(output_path: str, shuffle: bool = True, seed: int = 42) -> None:
    print("Fetching 20 Newsgroups dataset (this may take a moment)...")

    dataset = fetch_20newsgroups(
        subset="all",
        remove=("headers", "footers", "quotes"),
        shuffle=shuffle,
        random_state=seed,
    )

    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    category_counts: dict[str, int] = {}
    skipped = 0

    with open(output_path, "w", encoding="utf-8") as f:
        for text, target_idx in zip(dataset.data, dataset.target):
            category = dataset.target_names[target_idx]
            cleaned = text.strip()

            if len(cleaned) < 50:
                skipped += 1
                continue

            record = {"text": cleaned, "category": category}
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
            category_counts[category] = category_counts.get(category, 0) + 1

    total = sum(category_counts.values())
    print(f"\nExported {total} documents to '{output_path}' ({skipped} skipped as too short)\n")
    print("Documents per category:")
    for cat in sorted(category_counts):
        print(f"  {cat:<40} {category_counts[cat]:>5} docs")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Export 20 Newsgroups to JSONL")
    parser.add_argument(
        "--output", default="Data/20newsgroups.jsonl",
        help="Output file path (default: Data/20newsgroups.jsonl)"
    )
    parser.add_argument(
        "--no-shuffle", action="store_true",
        help="Disable shuffling (keep original dataset order)"
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    args = parser.parse_args()

    export(args.output, shuffle=not args.no_shuffle, seed=args.seed)