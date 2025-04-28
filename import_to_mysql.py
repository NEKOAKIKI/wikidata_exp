import json
import pymysql
from typing import Dict, Any, Set, List, Tuple

def get_preferred_text(data_dict: Dict[str, Dict], preferred_langs: List[str]) -> str:
    """通用获取优先语言的文本"""
    if not data_dict:
        return ""

    # 先尝试首选语言
    for lang in preferred_langs:
        if lang in data_dict and 'value' in data_dict[lang]:
            return data_dict[lang]['value']

    # 没有首选语言则返回第一个可用文本
    first_item = next(iter(data_dict.values()), {})
    return first_item.get('value', "")

def process_entity(entity_id: str, entity_info: Dict,
                  entity_rows: Set[Tuple], triple_rows: List[Tuple]) -> None:
    """处理单个实体数据"""
    # 优化后的标签提取：优先中文→英文→其他语言
    labels = entity_info.get('labels', {})
    descriptions = entity_info.get('descriptions', {})

    label = get_preferred_text(labels, ['zh', 'zh-cn', 'zh-hans', 'en'])
    description = get_preferred_text(descriptions, ['zh', 'zh-cn', 'zh-hans', 'en'])

    # 确保至少有一个非空标识
    display_label = label or entity_id  # 如果标签为空则使用ID作为后备

    entity_rows.add((entity_id, display_label, description))

    # 处理claims（不再需要property表）
    claims = entity_info.get('claims', {})
    for prop_id, claim_list in claims.items():
        for claim in claim_list:
            mainsnak = claim.get('mainsnak', {})
            if mainsnak.get('snaktype') != 'value':
                continue

            datavalue = mainsnak.get('datavalue', {})
            dtype = datavalue.get('type')
            value = datavalue.get('value', {})

            if dtype == 'wikibase-entityid':
                target_id = value.get('id')
                if target_id:
                    triple_rows.append((entity_id, prop_id, target_id, None, 'entity'))
            elif dtype in ['string', 'time', 'quantity', 'monolingualtext']:
                literal_value = str(value.get('text', value)) if dtype == 'monolingualtext' else str(value)
                triple_rows.append((entity_id, prop_id, None, literal_value, 'literal'))

def import_wikidata(json_path: str, db_config: Dict[str, str]) -> None:
    """主导入函数"""
    # 加载JSON数据
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            print(f"正在加载JSON文件: {json_path}")
            all_entities = json.load(f)
            print(f"成功加载 {len(all_entities)} 个顶级实体")
    except Exception as e:
        print(f"加载JSON文件失败: {e}")
        return

    # 准备数据容器
    entity_rows = set()
    triple_rows = []

    # 处理所有实体
    for outer_id, outer_content in all_entities.items():
        entities = outer_content.get('entities', {})
        print(f"处理 {outer_id}，包含 {len(entities)} 个子实体")

        for entity_id, entity_info in entities.items():
            process_entity(entity_id, entity_info, entity_rows, triple_rows)

    print(f"处理完成: {len(entity_rows)} 实体, {len(triple_rows)} 三元组")

    # 连接数据库
    try:
        connection = pymysql.connect(**db_config)
        cursor = connection.cursor()
        print("数据库连接成功")
    except Exception as e:
        print(f"数据库连接失败: {e}")
        return

    try:
        # 只创建必要的表（移除了property表）
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS entity (
            id VARCHAR(20) PRIMARY KEY,
            label VARCHAR(255) CHARACTER SET utf8mb4,
            description TEXT CHARACTER SET utf8mb4
        ) CHARACTER SET = utf8mb4;
        """)

        cursor.execute("""
        CREATE TABLE IF NOT EXISTS triple (
            id INT AUTO_INCREMENT PRIMARY KEY,
            subject_id VARCHAR(20),
            predicate_id VARCHAR(20),  -- 直接存储属性ID如"P31"
            object_entity_id VARCHAR(20),
            object_literal TEXT CHARACTER SET utf8mb4,
            object_type VARCHAR(10),
            FOREIGN KEY (subject_id) REFERENCES entity(id),
            FOREIGN KEY (object_entity_id) REFERENCES entity(id)
        ) CHARACTER SET = utf8mb4;
        """)

        # 先插入所有实体
        print(f"插入 {len(entity_rows)} 个实体...")
        cursor.executemany(
            "INSERT IGNORE INTO entity (id, label, description) VALUES (%s, %s, %s)",
            list(entity_rows)
        )
        print(f"实体插入完成，影响 {cursor.rowcount} 行")

        # 方案3：批量处理，先过滤无效数据
        # 获取所有有效的entity_id
        cursor.execute("SELECT id FROM entity")
        valid_ids = {row[0] for row in cursor.fetchall()}

        # 过滤三元组数据：只保留object_entity_id有效或为NULL的记录
        valid_triples = [
            triple for triple in triple_rows
            if triple[2] is None or triple[2] in valid_ids
        ]

        print(f"原始三元组数量: {len(triple_rows)}, 有效三元组数量: {len(valid_triples)}")
        print(f"将跳过 {len(triple_rows) - len(valid_triples)} 条无效记录")

        # 批量插入有效的三元组
        print(f"插入 {len(valid_triples)} 个有效三元组...")
        cursor.executemany("""
            INSERT INTO triple
            (subject_id, predicate_id, object_entity_id, object_literal, object_type)
            VALUES (%s, %s, %s, %s, %s)
        """, valid_triples)
        print(f"三元组插入完成，影响 {cursor.rowcount} 行")

        connection.commit()
        print("✅ 数据导入成功！")

    except Exception as e:
        connection.rollback()
        print(f"❌ 导入失败: {e}")
        import traceback
        traceback.print_exc()
    finally:
        cursor.close()
        connection.close()

# 配置信息
config = {
    "host": "localhost",
    "user": "root",
    "password": "123456",
    "database": "wikidata_db",
    "charset": "utf8mb4"
}

# 执行导入
import_wikidata("wikidata_entities.json", config)