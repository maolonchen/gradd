import json
import re

def get_chinese_number(n):
    """将数字转换为中文数字表示"""
    chinese_numbers = ["零", "一", "二", "三", "四", "五", "六", "七", "八", "九", "十"]
    if n <= 10:
        return chinese_numbers[n]
    elif n <= 99:
        # 处理11-99的中文表示
        tens = n // 10
        units = n % 10
        if tens == 1:
            return f"十{chinese_numbers[units]}" if units > 0 else "十"
        else:
            return f"{chinese_numbers[tens]}十{chinese_numbers[units]}" if units > 0 else f"{chinese_numbers[tens]}十"
    else:
        return str(n)  # 超过99直接使用阿拉伯数字

def transform_json(original_json):
    # 解析原始JSON
    original = json.loads(original_json)
    
    # 找到"对应特征"的位置
    header = original["header"]
    feature_index = None
    for key, value in header.items():
        if value == "对应特征":
            feature_index = int(key)
            break
    
    if feature_index is None:
        raise ValueError("原始JSON中未找到'对应特征'字段")
    
    # 获取前一个分类字段的名称
    prev_key = str(feature_index - 1)
    prev_class_name = header.get(prev_key, "")
    
    # 提取前一个分类的层级数字
    match = re.search(r'([一二三四五六七八九十]+)级分类', prev_class_name)
    if match:
        # 从中文数字转换为阿拉伯数字
        chinese_num = match.group(1)
        chinese_to_num = {"一":1, "二":2, "三":3, "四":4, "五":5, "六":6, "七":7, "八":8, "九":9, "十":10}
        prev_level = chinese_to_num.get(chinese_num, feature_index)
        new_level = prev_level + 1
        new_level_name = f"{get_chinese_number(new_level)}级分类"
    else:
        # 如果没有匹配到，使用位置推断
        new_level = feature_index + 1
        new_level_name = f"{get_chinese_number(new_level)}级分类"
    
    # 创建新的header结构
    new_header = {}
    new_data = {}
    
    # 重建header和data
    new_key_index = 0
    for key in sorted(header.keys(), key=int):
        key_int = int(key)
        
        # 在"对应特征"前插入新分类
        if key_int == feature_index:
            new_header[str(new_key_index)] = new_level_name
            new_data[str(new_key_index)] = "{字符串}"
            new_key_index += 1
        
        # 复制原有字段
        new_header[str(new_key_index)] = header[key]
        new_data[str(new_key_index)] = original["data"][key]
        new_key_index += 1
    
    return json.dumps({"header": new_header, "data": new_data}, ensure_ascii=False, indent=2)

# 测试不同层级的JSON
test_json_1 = '''
{
    "header": {
        "0": "一级分类",
        "1": "二级分类",
        "2": "三级分类",
        "3": "对应特征",
        "4": "等级",
        "5": "条件",
        "6": "真实数据"
    },
    "data": {
        "0": "企业管理数据（G类）",
        "1": "G6 综合管理类数据",
        "2": "G6-5 采购",
        "3": "G6-5-2 物资数据：采购物资数量、类型等信息",
        "4": "第2级",
        "5": "",
        "6": ["采购数量", "物资类型", "采购数据", "库存信息"]
    }
}
'''

test_json_2 = '''
{
    "header": {
        "0": "一级分类",
        "1": "二级分类",
        "2": "三级分类",
        "3": "四级分类",
        "4": "五级分类",
        "5": "对应特征",
        "6": "等级",
        "7": "条件",
        "8": "真实数据"
    },
    "data": {
        "0": "企业管理数据（G类）",
        "1": "G6 综合管理类数据",
        "2": "G6-5 采购",
        "3": "G6-5-1 采购计划",
        "4": "G6-5-1-3 月度计划",
        "5": "月度采购计划数据",
        "6": "第4级",
        "7": "",
        "8": ["计划数量", "预算金额", "采购周期"]
    }
}
'''

print("测试1（三级分类结构）:")
print(transform_json(test_json_1))

print("\n测试2（五级分类结构）:")
print(transform_json(test_json_2))