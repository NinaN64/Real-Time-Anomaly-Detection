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
        "political", "administration", "sovereignty", "nation",
        "federal", "state", "rights", "law", "court",
    ],
    "wikipedia_music": [
        "music", "song", "album", "band", "singer", "musician",
        "composer", "orchestra", "symphony", "opera", "jazz", "rock",
        "pop", "hip-hop", "guitar", "piano", "violin", "drums",
        "concert", "record", "label", "genre", "melody", "rhythm",
        "lyrics", "vocalist", "instrument", "soundtrack", "debut",
    ],
    "wikipedia_technology": [
        "software", "hardware", "computer", "internet", "programming",
        "algorithm", "database", "network", "cybersecurity",
        "artificial intelligence", "machine learning", "semiconductor",
        "processor", "operating system", "smartphone", "robot",
        "automation", "encryption", "startup", "silicon", "transistor",
        "bandwidth", "compiler", "firmware",
    ],
    "wikipedia_history": [
        "ancient", "medieval", "century", "empire", "dynasty",
        "civilization", "archaeological", "historian", "chronicle",
        "conquest", "colonization", "independence", "uprising", "siege",
        "monarchy", "feudal", "renaissance", "reformation",
        "excavation", "artifact", "manuscript", "heritage", "ruin",
        "pharaoh", "gladiator", "crusade", "viking", "samurai",
    ],
    "wikipedia_medicine": [
        "disease", "syndrome", "diagnosis", "treatment", "surgery",
        "medicine", "physician", "hospital", "pharmaceutical", "vaccine",
        "cancer", "infection", "epidemic", "patient", "symptom",
        "therapy", "clinical", "anatomy", "pathology", "psychiatry",
        "drug", "dose", "prognosis", "mortality", "disorder",
        "antibiotic", "immune", "chronic", "acute", "mental health",
    ],
    "wikipedia_geography": [
        "mountain", "river", "ocean", "continent", "country",
        "capital", "city", "island", "desert", "forest",
        "population", "territory", "border", "region", "lake",
        "valley", "peninsula", "plateau", "coast", "elevation",
        "basin", "tributary", "strait", "cape", "delta", "tundra",
        "latitude", "longitude", "topography", "climate zone",
    ],
    "wikipedia_law": [
        "law", "legal", "court", "judge", "trial", "lawyer", "attorney",
        "statute", "constitutional", "judicial", "verdict",
        "plaintiff", "defendant", "criminal", "civil", "jurisdiction",
        "supreme court", "appeal", "sentence", "prosecution",
        "regulation", "compliance", "lawsuit", "legislation",
        "conviction", "acquittal", "testimony", "evidence", "charter",
    ],
    "wikipedia_film": [
        "film", "movie", "cinema", "director", "actor", "actress",
        "screenplay", "box office", "documentary", "animated", "sequel",
        "studio", "premiere", "release", "cast", "genre", "thriller",
        "comedy", "drama", "horror", "blockbuster", "oscar",
        "cinematography", "production", "screenplay", "scene",
        "filming", "distributor", "trailer", "remake",
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
            if all(v >= TARGET_PER_BUCKET for v in counts.values()):
                break

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