import json

def get_all_match_ids(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
        matches = data.get("fixtures", {}).get("allMatches", [])

        # print(f"length of matches: {len(matches)}")

        match_ids = []
        for match in matches:
            match_ids.append(match.get("id"))
    return match_ids