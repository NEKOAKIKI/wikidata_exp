import json

def export_json_to_rdf(json_path: str, rdf_path: str) -> None:
    """将wikidata的json数据导出为gStore可导入的N-Triples格式"""
    def escape_literal(value: str) -> str:
        """对literal进行简单转义"""
        return value.replace('"', '\\"')

    try:
        with open(json_path, "r", encoding="utf-8") as f:
            all_entities = json.load(f)
    except Exception as e:
        print(f"❌ 读取JSON失败: {e}")
        return

    with open(rdf_path, "w", encoding="utf-8") as rdf_file:
        for outer_id, outer_content in all_entities.items():
            entities = outer_content.get('entities', {})

            for entity_id, entity_info in entities.items():
                subject_uri = f"<http://www.wikidata.org/entity/{entity_id}>"

                # 标签
                labels = entity_info.get('labels', {})
                label = ""
                for lang in ['zh', 'zh-cn', 'zh-hans', 'en']:
                    if lang in labels:
                        label = labels[lang]['value']
                        break
                if label:
                    rdf_file.write(f'{subject_uri} <rdfs:label> "{escape_literal(label)}"@en .\n')

                # 描述
                descriptions = entity_info.get('descriptions', {})
                description = ""
                for lang in ['zh', 'zh-cn', 'zh-hans', 'en']:
                    if lang in descriptions:
                        description = descriptions[lang]['value']
                        break
                if description:
                    rdf_file.write(f'{subject_uri} <rdfs:comment> "{escape_literal(description)}"@en .\n')

                # 声明claims
                claims = entity_info.get('claims', {})
                for prop_id, claim_list in claims.items():`
                    for claim in claim_list:
                        mainsnak = claim.get('mainsnak', {})
                        if mainsnak.get('snaktype') != 'value':
                            continue

                        datavalue = mainsnak.get('datavalue', {})
                        dtype = datavalue.get('type')
                        value = datavalue.get('value', {})

                        predicate_uri = f"<http://www.wikidata.org/prop/direct/{prop_id}>"

                        if dtype == 'wikibase-entityid':
                            target_id = value.get('id')
                            if target_id:
                                object_uri = f"<http://www.wikidata.org/entity/{target_id}>"
                                rdf_file.write(f'{subject_uri} {predicate_uri} {object_uri} .\n')
                        elif dtype in ['string', 'time', 'quantity', 'monolingualtext']:
                            literal_value = value.get('text', value) if dtype == 'monolingualtext' else value
                            if isinstance(literal_value, dict):
                                literal_value = literal_value.get('amount', '')
                            if literal_value:
                                rdf_file.write(f'{subject_uri} {predicate_uri} "{escape_literal(str(literal_value))}" .\n')

    print(f"✅ RDF数据已成功导出到 {rdf_path}")

# 调用示例
export_json_to_rdf("wikidata_entities0.json", "wikidata_data.nt")
