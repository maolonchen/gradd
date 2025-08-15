# -*- coding: utf-8 -*-
"""
数据库构建模块

本模块用于将处理后的Sheet3数据转换为向量嵌入，
并存储到向量数据库中，为后续的相似度搜索做准备。
"""

import json
from cfg.settings import GRAD_ENTITY_PATH, EMBEDDING_MODEL_PATH, GRAD_COLLECTION_NAME, DB_PATH
from pathlib import Path
from rich import print

from pymilvus import MilvusClient
from sentence_transformers import SentenceTransformer

def find_sheet_in_tree(tree, sheet_name):
    """
    在树形结构中递归查找指定名称的工作表
    
    参数:
        tree: 树形结构数据
        sheet_name: 要查找的工作表名称
        
    返回:
        找到的工作表节点，或None
    """
    if isinstance(tree, dict):
        # 检查当前节点是否是目标工作表
        if tree.get('type') == 'sheet' and tree.get('name') == sheet_name:
            return tree
            
        # 递归检查子节点
        if 'children' in tree:
            for child in tree['children']:
                found = find_sheet_in_tree(child, sheet_name)
                if found:
                    return found
    return None

def print_sheet3_data(file_path, milvus_client, collection_name, embedding_model, embedding_dim):
    """
    处理Sheet3的所有数据行，生成向量嵌入并存储到Milvus数据库
    
    参数:
        file_path: 包含Sheet3数据的JSON文件路径
        milvus_client: Milvus客户端实例
        collection_name: 集合名称
        embedding_model: 嵌入模型实例
        embedding_dim: 向量维度
    """
    text_lines = []  # 用于收集所有需要编码的行

    try:
        with open(file_path, "r", encoding="utf-8") as f:
            tree = json.load(f)
        
        sheet3 = find_sheet_in_tree(tree, "Sheet3")
        if not sheet3:
            raise ValueError("未找到工作表 'Sheet3'")
        
        sheet_data = sheet3.get('info', {}).get('data', [])
        if not sheet_data:
            print("Sheet3 没有数据")
            return False
        
        # 打印表头
        header = sheet_data[0]
        print(f"表头: {header}")
        print("-" * 50)
        
        # 第一步：收集所有数据行（跳过表头）
        for i, row in enumerate(sheet_data[1:], 1):
            print(f"行 {i}: {row}")
            text_lines.append(row)  # 只是收集文本
        
        print("-" * 50)
        print(f"共找到 {len(text_lines)} 行数据")
        
        # 第二步：检查并创建集合（一次）
        if milvus_client.has_collection(collection_name):
            milvus_client.drop_collection(collection_name)
        
        milvus_client.create_collection(
            collection_name=collection_name,
            dimension=embedding_dim,
            metric_type="IP",
            consistency_level="Strong",
        )
        
        # 第三步：对所有收集到的文本行进行一次批量编码
        print("正在生成嵌入向量...")
        embeddings = embedding_model.encode(
            text_lines,  # 传入所有行
            batch_size=32,
            show_progress_bar=True
        )
        
        # 第四步：构建要插入的数据列表
        data = []
        for i, (line, embedding) in enumerate(zip(text_lines, embeddings)):
            data.append({
                "id": i,  # 或者使用其他唯一ID
                "vector": embedding,  # 直接使用编码结果
                "text": line
            })
        
        print(f"准备插入 {len(data)} 条数据...")
        print(data[:2])  # 打印前两条作为检查
        
        # 第五步：一次性插入所有数据
        milvus_client.insert(collection_name=collection_name, data=data)
        print("数据插入完成！")
        
        return True
        
    except FileNotFoundError:
        print(f"错误: 文件不存在: {file_path}")
        return False
    except json.JSONDecodeError as e:
        print(f"错误: JSON解析错误: {str(e)}")
        return False
    except Exception as e:
        print(f"错误: {str(e)}")
        return False

def main():
    """
    主函数：将处理后的Sheet3数据转换为向量嵌入，并存储到向量数据库中
    """
    try:
        # 设置输入文件路径
        input_file = GRAD_ENTITY_PATH
        
        # 检查文件是否存在
        if not Path(input_file).exists():
            print(f"错误: 输入文件不存在: {input_file}")
            return False
        
        # 初始化模型和数据库客户端
        embedding_model = SentenceTransformer(EMBEDDING_MODEL_PATH)
        milvus_client = MilvusClient(DB_PATH)
        collection_name = GRAD_COLLECTION_NAME

        test_embedding = embedding_model.encode("A")
        embedding_dim = len(test_embedding)  # 1024
        print(f"向量维度: {embedding_dim}")
        
        if milvus_client.has_collection(collection_name):  # 格式化指定表
            milvus_client.drop_collection(collection_name)
        
        print(f"正在处理文件: {input_file}")
        print("=" * 50)
        
        # 打印Sheet3数据
        success = print_sheet3_data(input_file, milvus_client, collection_name, embedding_model, embedding_dim)
        return success
        
    except Exception as e:
        print(f"处理过程中出现错误: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    if not success:
        exit(1)