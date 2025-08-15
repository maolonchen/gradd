# -*- coding: utf-8 -*-
"""
相似度比较模块

本模块用于对处理后的数据进行语义相似度比较，
通过向量嵌入和Milvus数据库搜索，找出与输入数据最相似的内容。
"""

import re
import json
from rich import print
from pymilvus import MilvusClient
from sentence_transformers import SentenceTransformer
from cfg.settings import GRAD_ENTITY_PATH, EMBEDDING_MODEL_PATH, GRAD_COLLECTION_NAME, DB_PATH, MULTI_TREE_PATH, SIMILAR_COMPARE_PATH


# 初始化全局变量
milvus_client = MilvusClient(DB_PATH)
collection_name = GRAD_COLLECTION_NAME
embedding_model = SentenceTransformer(EMBEDDING_MODEL_PATH)

# 正则表达式：匹配 A1、A1-3 等格式的标识符
pattern = re.compile(r'^([A-Z]\d+(?:-\d+)*)(.*)')

# 字符数限制和批处理大小
char_count = 9
BATCH_SIZE = 8
OUTPUT_FILE = SIMILAR_COMPARE_PATH

def extract_and_combine_line(line, char_count):
    """
    提取该行所有匹配项，并拼接成一句话
    
    参数:
        line: JSON格式的行数据
        char_count: 每个部分保留的字符数
    
    返回:
        tuple: (原始列表, 拼接后的查询文本, 标识符列表) 或 (None, None, None)
    """
    try:
        data = json.loads(line.strip())
        items = data.get("data", [])
        if len(items) < 2:
            return None, None, None

        target_dict = items[1]
        parts = []
        identifiers = []
        for value in target_dict.values():
            match = pattern.match(value.strip())
            if match:
                code = match.group(1)
                text_part = match.group(2).strip()[:char_count]
                combined = f"{code} {text_part}".strip()
                if combined:
                    parts.append(combined)
                    identifiers.append(code)

        if not parts:
            return None, None, None

        query_text = "；".join(parts)
        return parts, query_text, identifiers

    except Exception as e:
        print(f"解析错误: {e}")
        return None, None, None

def process_search_results(search_results, batch_infos, output_file):
    """
    处理搜索结果并写入文件
    
    参数:
        search_results: Milvus搜索结果
        batch_infos: 批处理元数据
        output_file: 输出文件对象
    """
    for idx, (line_num, orig_line, orig_list, q_text, ori_ids) in enumerate(batch_infos):
        print(f"\n=== 第 {line_num} 行解析结果 ===")
        print(f"原始行内容: {orig_line}")
        print(f"提取数据: {orig_list}")
        print(f"查询文本: '{q_text}'")
        print(f"提取标识: {ori_ids}")
        
        if idx < len(search_results):
            print("相似内容:")
            similar_items = extract_similar_items(search_results[idx])
            
            # 准备输出数据
            result = {
                "ori_data": ori_ids,
                "similar_data": similar_items
            }
            
            # 写入输出文件
            json.dump(result, output_file, ensure_ascii=False)
            output_file.write("\n")
        else:
            print("  未返回搜索结果")

def extract_similar_items(search_result):
    """
    从搜索结果中提取相似项
    
    参数:
        search_result: 单个搜索结果
        
    返回:
        list: 提取的相似项列表
    """
    similar_items = []
    for i, hit in enumerate(search_result, 1):
        entity = hit["entity"]
        sim_text = entity.get("text", "")
        score = hit["distance"]
        print(f"  {i}. [{score:.4f}] {sim_text}")
        
        if isinstance(sim_text, dict):
            similar_items.append([str(v) for v in sim_text.values()])
        elif isinstance(sim_text, str):
            try:
                data_dict = json.loads(sim_text)
                similar_items.append([str(v) for v in data_dict.values()])
            except:
                key_values = re.findall(r"'(.*?)': '(.*?)'", sim_text)
                if key_values:
                    similar_items.append([v for _, v in key_values])
                else:
                    similar_items.append([sim_text])
        else:
            similar_items.append([str(sim_text)])
    
    return similar_items

def process_batch(batch_data, batch_infos, output_file):
    """
    处理一个批次的数据
    
    参数:
        batch_data: 批处理的查询文本
        batch_infos: 批处理的元数据
        output_file: 输出文件对象
    """
    try:
        embeddings = embedding_model.encode(batch_data)
        search_results = milvus_client.search(
            collection_name=collection_name,
            data=embeddings,
            limit=4,
            search_params={'metric_type': "IP", "params": {}},
            output_fields=["text"],
        )
        process_search_results(search_results, batch_infos, output_file)
    except Exception as e:
        print(f"批处理搜索出错: {e}")

def main():
    """主函数"""
    try:
        with open(MULTI_TREE_PATH, 'r', encoding='utf-8') as file, \
             open(OUTPUT_FILE, 'w', encoding='utf-8') as output_file:
            
            batch_data = []
            batch_infos = []
            valid_lines = 0
            
            for line_number, line in enumerate(file, 1):
                original_line = line.strip()
                if not original_line:
                    continue
                    
                original_list, query_text, identifiers = extract_and_combine_line(original_line, char_count)
                if not original_list or not query_text or not identifiers:
                    continue
                    
                batch_data.append(query_text)
                batch_infos.append((line_number, original_line, original_list, query_text, identifiers))
                valid_lines += 1
                
                # 达到批处理大小时处理批次
                if len(batch_data) >= BATCH_SIZE:
                    process_batch(batch_data, batch_infos, output_file)
                    # 重置批处理
                    batch_data = []
                    batch_infos = []
            
            # 处理剩余的最后一批数据
            if batch_data:
                process_batch(batch_data, batch_infos, output_file)
                
            print(f"\n处理完成！共处理 {valid_lines} 行有效数据")
            print(f"结果已保存到: {OUTPUT_FILE}")
                
    except FileNotFoundError:
        print(f"错误：找不到文件 '{MULTI_TREE_PATH}'")
    except Exception as e:
        print(f"处理出错: {e}")

if __name__ == "__main__":
    main()