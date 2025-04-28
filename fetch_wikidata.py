import json
import requests
import time

# ------------------------------
# 配置参数
# ------------------------------
NUM_ENTITIES = 500
ENTITY_SEED = ["Q5", "Q114", "Q2", "Q30", "Q148", "Q76", "Q142", "Q183", "Q6256", "Q43229"]
SAVE_PATH = "wikidata_entities0.json"
HEADERS = {
    "User-Agent": "Wikidata-Experiment/1.0"
}
WIKIDATA_API = "https://www.wikidata.org/wiki/Special:EntityData/{}.json"


# ------------------------------
# 获取随机实体ID
# ------------------------------
def get_random_entity_ids(seed_ids, target_count):
    visited = set(seed_ids)
    queue = list(seed_ids)
    all_ids = list(seed_ids)
    while len(all_ids) < target_count and queue:
        current_qid = queue.pop(0)
        url = WIKIDATA_API.format(current_qid)
        try:
            response = requests.get(url, headers=HEADERS, timeout=10)
            if response.status_code != 200:
                continue
            data = response.json()
            entity = data["entities"][current_qid]
            claims = entity.get("claims", {})
            for pid in claims:
                for claim in claims[pid]:
                    if "datavalue" in claim["mainsnak"]:
                        val = claim["mainsnak"]["datavalue"]["value"]
                        if isinstance(val, dict) and "id" in val:
                            linked_qid = val["id"]
                            if linked_qid not in visited:
                                visited.add(linked_qid)
                                queue.append(linked_qid)
                                all_ids.append(linked_qid)
                                if len(all_ids) >= target_count:
                                    break
                if len(all_ids) >= target_count:
                    break
        except Exception:
            continue
        time.sleep(0.1)
    return all_ids[:target_count]


# ------------------------------
# 获取实体JSON数据
# ------------------------------
def fetch_entity_data(qids):
    all_data = {}
    for i, qid in enumerate(qids):
        try:
            url = WIKIDATA_API.format(qid)
            response = requests.get(url, headers=HEADERS, timeout=10)
            if response.status_code == 200:
                data = response.json()
                all_data[qid] = data
        except Exception as e:
            print(f"Error fetching {qid}: {e}")
        time.sleep(0.1)
        if i % 50 == 0:
            print(f"[{i}/{len(qids)}] Downloaded...")
    return all_data


# ------------------------------
# 主函数
# ------------------------------
def main():
    print("Step 1: 获取实体 QID 列表...")
    qids = get_random_entity_ids(ENTITY_SEED, NUM_ENTITIES)
    print(f"共获取 {len(qids)} 个实体")

    print("Step 2: 下载实体 JSON 数据...")
    entity_data = fetch_entity_data(qids)
    print(f"成功获取 {len(entity_data)} 个实体 JSON 数据")

    print(f"Step 3: 保存数据到 {SAVE_PATH}")
    with open(SAVE_PATH, "w", encoding="utf-8") as f:
        json.dump(entity_data, f, ensure_ascii=False, indent=2)

    print("✅ 数据抓取完成")


if __name__ == "__main__":
    main()
