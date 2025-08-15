import re
import json
import uuid
from rich import print
from datetime import datetime, timezone
from collections import defaultdict
import sys
import os
import requests
import time

# 添加项目根目录到Python路径
sys.path.append('/usr/local/app/cml/volume/project01')

# 导入必要的模块
from cfg.settings import LLM_API_URL, MODEL_NAME, API_PAYLOAD

def build_classification_tree(jsonl_lines):
    """
    从JSONL行数据构建分类树结构，并收集等级和特征信息
    
    参数:
        jsonl_lines: JSONL格式的行列表
    
    返回:
        tuple: (分类树字典, 等级列表, 特征映射列表)
    """
    # 创建嵌套字典结构
    tree = defaultdict(lambda: defaultdict(dict))
    grades_set = set()  # 收集所有等级
    feature_mappings = []  # 收集特征映射
    
    for line in jsonl_lines:
        try:
            # 解析JSON行
            entry = json.loads(line)
            header = entry['header']
            data = entry['data']
            
            # 确定分类层级路径
            path = []
            level_key = None
            feature_key = None
            classification_key = None
            
            # 查找"等级"字段在header中的键
            for key, value in header.items():
                if value == '等级':
                    level_key = key
                    break
                    
            # 查找"对应特征"字段在header中的键
            for key, value in header.items():
                if value == '对应特征':
                    feature_key = key
                    break
            
            # 获取所有数字键并排序，用于构建分类路径
            keys = [k for k in header.keys() if k.isdigit()]
            sorted_keys = sorted(keys, key=int)
            
            # 构建分类路径并识别关键字段
            for key in sorted_keys:
                header_val = header[key]
                if key in data:
                    data_val = data[key]
                    
                    if '级分类' in header_val:
                        path.append(data_val)
                        # 记录最后一级分类作为分类标识
                        classification_key = key
            
            # 如果路径为空，跳过该行
            if not path:
                continue
                
            # 获取等级和特征值
            level = data.get(level_key, None) if level_key else None
            feature = data.get(feature_key, None) if feature_key else None
            
            # 收集等级信息
            if level:
                # 处理等级信息
                if "/" in level:
                    # 处理多个等级的情况，如"第2级/第3级"
                    levels = level.split("/")
                    for l in levels:
                        l = l.strip()
                        # 提取核心数据、重要数据或第N级等标准等级格式
                        if "核心数据" in l:
                            grades_set.add("核心数据")
                        elif "重要数据" in l:
                            grades_set.add("重要数据")
                        elif l.startswith("第") and l.endswith("级"):
                            grades_set.add(l)
                else:
                    # 单个等级的情况
                    level = level.strip()
                    # 提取核心数据、重要数据或第N级等标准等级格式
                    if "核心数据" in level:
                        grades_set.add("核心数据")
                    elif "重要数据" in level:
                        grades_set.add("重要数据")
                    elif level.startswith("第") and level.endswith("级"):
                        grades_set.add(level)
            
            # 收集特征映射信息
            if feature and classification_key and classification_key in data:
                classification = data[classification_key]
                # 处理特征字符串（可能包含多个特征）
                features = [f.strip() for f in feature.split('、') if f.strip()]
                for feat in features:
                    feature_mappings.append({
                        "DataElement": feat,
                        "DataClassification": classification
                    })
            
            # 构建树结构
            current = tree
            for i, node_name in enumerate(path[:-1]):
                if node_name not in current:
                    current[node_name] = defaultdict(dict)
                current = current[node_name]
            
            # 处理最后一级节点
            last_node = path[-1]
            if last_node not in current:
                current[last_node] = {
                    "DataClassification": last_node,
                    "DataGrading": level,
                    "Annotate": feature,
                    "SubDataClassifications": []
                }
            else:
                # 更新现有节点
                current[last_node]["DataGrading"] = level
                current[last_node]["Annotate"] = feature
        except Exception as e:
            print(f"处理行时出错: {line}")
            print(f"错误信息: {str(e)}")
            continue
    
    # 将嵌套字典转换为模板格式
    def convert_to_template(node):
        """
        将嵌套字典转换为模板格式的递归函数
        
        参数:
            node: 当前处理的节点
            
        返回:
            转换后的节点结构
        """
        if isinstance(node, dict):
            result = []
            for key, value in node.items():
                if isinstance(value, dict) and "DataClassification" in value:
                    # 这是叶节点
                    result.append({
                        "DataClassification": value["DataClassification"],
                        "DataGrading": value["DataGrading"],
                        "Annotate": value["Annotate"],
                        "SubDataClassifications": convert_to_template(value.get("SubDataClassifications", []))
                    })
                else:
                    # 这是中间节点
                    result.append({
                        "DataClassification": key,
                        "DataGrading": None,
                        "Annotate": None,
                        "SubDataClassifications": convert_to_template(value)
                    })
            return result
        return None
    
    # 转换树结构
    tree_converted = convert_to_template(tree)
    
    # 获取所有有效的等级
    valid_grades = []
    for grade in grades_set:
        # 只处理我们关心的等级类型
        # if grade in ["核心数据", "重要数据", "第1级", "第2级", "第3级", "第4级"]:
            valid_grades.append(grade)
            print(f"处理等级: {grade}")
    
    # 使用LLM对等级进行排序
    valid_grades = ["第一级", "第二级", "第三级", "第四级", "特别"]
    print(f"处理等级: {valid_grades}")
    grades_list = sort_grades_with_llm(valid_grades)
    
    return tree_converted, grades_list, feature_mappings

def sort_grades_with_llm(grades):
    """
    使用LLM对等级进行排序
    
    参数:
        grades: 等级列表
    
    返回:
        排序后的等级列表，包含等级和对应的优先级
    """
    # 构建提示词
    prompt = f"""
你是一个数据安全专家，需要对以下数据等级按照重要性进行排序。

排序规则：
1. 名称中包含类似"核心"或"重要"等特殊关键词的等级具有最高优先级
2. 对于名称中包含"第"字和数字的等级（如"第1级"、"第2级"等），等级中数字越大，优先级越高，Priority越小
3. 如果两个等级都包含"第"字和数字，等级中数字越小的优先级更高，Priority越大
4. 如果两个等级都不包含"第"字和数字，则根据名称中的数字判断，等级中数字大的优先级更高，Priority越小
5. Priority的数字越小表示越重要，例如最重要等级优先级为1

请将以下等级按照上述规则进行排序，并为每个等级分配一个优先级数字：

等级列表：{', '.join(grades)}

请以JSON格式返回结果，格式如下：
[
  {{"DataGrading": "等级名称A", "Priority": 1}},
  {{"DataGrading": "等级名称B", "Priority": 2}},
  {{"DataGrading": "等级名称C", "Priority": 3}},
  ...
]

只返回JSON数组，不要包含其他文字。
"""
    
    # 持续重试直到成功
    while True:
        try:
            # 使用settings.py中的配置创建payload
            payload = API_PAYLOAD(MODEL_NAME, prompt)
            
            # 调用LLM API
            headers = {"Content-Type": "application/json"}
            response = requests.post(LLM_API_URL, headers=headers, json=payload, timeout=60)
            response.raise_for_status()  # 检查HTTP错误
            
            # 提取LLM的回复
            result = response.json()
            llm_output = result["choices"][0]["message"]["content"]
            llm_output = llm_output.split("</think>\n\n")[1] if "</think>\n\n" in llm_output else llm_output
            
            # 验证返回结果的结构
            if is_valid_json_format(llm_output):
                grades_list = json.loads(llm_output)
                # 验证内容是否符合要求
                if is_valid_grade_list(grades_list, grades):
                    return grades_list
                else:
                    print("LLM返回的内容不符合要求，重新请求...")
            else:
                print("LLM返回的不是有效的JSON格式，重新请求...")
                
        except requests.exceptions.Timeout:
            print("LLM请求超时，重新连接...")
            time.sleep(5)  # 等待5秒后重试
        except requests.exceptions.RequestException as e:
            print(f"LLM请求失败: {str(e)}，重新连接...")
            time.sleep(5)  # 等待5秒后重试
        except Exception as e:
            print(f"处理LLM响应时出错: {str(e)}，重新连接...")
            time.sleep(5)  # 等待5秒后重试

def is_valid_json_format(text):
    """
    验证文本是否为有效的JSON格式
    
    参数:
        text: 待验证的文本
    
    返回:
        bool: 是否为有效的JSON格式
    """
    try:
        json.loads(text)
        return True
    except json.JSONDecodeError:
        return False

def is_valid_grade_list(grades_list, expected_grades):
    """
    验证等级列表是否符合要求
    
    参数:
        grades_list: LLM返回的等级列表
        expected_grades: 期望的等级列表
    
    返回:
        bool: 是否符合要求
    """
    try:
        # 检查是否为列表
        if not isinstance(grades_list, list):
            return False
            
        # 检查每个元素
        expected_set = set(expected_grades)
        actual_set = set()
        
        for item in grades_list:
            # 检查每个元素是否为字典
            if not isinstance(item, dict):
                return False
                
            # 检查必需的键
            if "DataGrading" not in item or "Priority" not in item:
                return False
                
            # 检查DataGrading是否在期望的等级中
            if item["DataGrading"] not in expected_set:
                return False
                
            # 检查Priority是否为整数
            if not isinstance(item["Priority"], int):
                return False
                
            actual_set.add(item["DataGrading"])
            
        # 检查是否包含了所有期望的等级
        if actual_set != expected_set:
            return False
            
        # 检查优先级是否唯一
        priorities = [item["Priority"] for item in grades_list]
        if len(priorities) != len(set(priorities)):
            return False
            
        return True
    except Exception:
        return False

def main():
    """
    主函数：读取JSONL文件，构建分类树，收集等级和特征信息，并保存结果
    """
    try:
        # 输入文件路径
        input_file = '/usr/local/app/cml/volume/project01/data/processed/final.jsonl'
        
        # 输出文件路径
        kafka_data_path = "/usr/local/app/cml/volume/project01/data/processed/kafka_output_data.json"
        kafka_grad_path = "/usr/local/app/cml/volume/project01/data/processed/kafka_output_grad.json"
        kafka_feat_path = "/usr/local/app/cml/volume/project01/data/processed/kafka_output_feat.json"
        
        print(f"开始处理文件: {input_file}")
        
        # 读取JSONL文件
        with open(input_file, 'r') as f:
            jsonl_lines = f.readlines()
        
        print(f"成功读取 {len(jsonl_lines)} 行数据")
        
        # 构建分类树并收集信息
        print("开始构建分类树并收集信息...")
        classification_tree, grades_list, feature_mappings = build_classification_tree(jsonl_lines)
        print("分类树构建完成")
        
        # 保存分类树结果
        print(f"将分类树结果保存到: {kafka_data_path}")
        with open(kafka_data_path, 'w', encoding='utf-8') as f:
            json.dump(classification_tree, f, ensure_ascii=False, indent=2)
        
        # 保存等级信息
        print(f"将等级信息保存到: {kafka_grad_path}")
        with open(kafka_grad_path, 'w', encoding='utf-8') as f:
            json.dump(grades_list, f, ensure_ascii=False, indent=2)
        
        # 准备特征映射数据结构
        feat_data = feature_mappings
        
        # 保存特征映射信息
        print(f"将特征映射信息保存到: {kafka_feat_path}")
        with open(kafka_feat_path, 'w', encoding='utf-8') as f:
            json.dump(feat_data, f, ensure_ascii=False, indent=2)
        
        print("处理完成，所有结果已保存")
    
    except FileNotFoundError:
        print(f"错误: 文件未找到 - {input_file}")
    except json.JSONDecodeError as e:
        print(f"JSON解析错误: {str(e)}")
    except Exception as e:
        print(f"处理过程中发生错误: {str(e)}")

if __name__ == "__main__":
    main()