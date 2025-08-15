# -*- coding: utf-8 -*-
"""
主流程控制脚本

控制整个数据处理流程的执行顺序和依赖关系
"""

import os
import sys
import importlib.util
from rich import print
from pathlib import Path

# 添加项目路径到系统路径
project_root = Path(__file__).parent  # /usr/local/app/cml/volume/project/src
sys.path.insert(0, str(project_root))

def load_module_from_file(file_path):
    """动态加载Python模块"""
    spec = importlib.util.spec_from_file_location(file_path.stem, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

def check_file_exists(file_path, description=""):
    """检查文件是否存在"""
    if not os.path.exists(file_path):
        print(f"错误: 所需文件不存在 {description}: {file_path}")
        return False
    return True

def run_step(step_number, module_name, required_files=None, specification_u_id=None):
    """运行指定步骤"""
    print(f"\n{'='*50}")
    print(f"[green]开始执行步骤 {step_number}: {module_name}[/green]")
    print(f"{'='*50}")
    
    module_path = project_root / f"{module_name}.py"
    
    if not module_path.exists():
        print(f"错误: 模块文件不存在: {module_path}")
        return False
    
    # 检查依赖文件
    if required_files:
        for file_path in required_files:
            if not check_file_exists(file_path):
                print(f"步骤 {step_number} 依赖文件缺失，跳过执行")
                return False
    
    try:
        # 动态加载并执行模块
        module = load_module_from_file(module_path)
        if hasattr(module, 'main') and callable(module.main):
            print(f"执行 {module_name}.main()...")
            # 检查main函数是否接受specification_u_id参数
            import inspect
            sig = inspect.signature(module.main)
            if specification_u_id is not None and 'specification_u_id' in sig.parameters:
                result = module.main(specification_u_id=specification_u_id)
            else:
                result = module.main()
            print(f"步骤 {step_number} 执行完成")
            return True
        else:
            print(f"警告: 模块 {module_name} 没有可执行的 main 函数")
            return False
    except Exception as e:
        print(f"步骤 {step_number} 执行出错: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """主函数"""
    print("数据处理流程控制器")
    print("项目路径:", project_root)
    
    # 定义处理流程和依赖关系
    steps = [
        {
            "number": 1,
            "module": "01xls2json",
            "description": "Excel转JSON",
            "dependencies": []  # 依赖原始Excel文件
        },
        {
            "number": 2,
            "module": "02multi_tree",
            "description": "多树结构处理",
            "dependencies": [project_root.parent / "data" / "processed" / "ori_json.json"]  # 依赖步骤1的输出
        },
        {
            "number": 3,
            "module": "03grad2entity",
            "description": "梯度到实体转换",
            "dependencies": [project_root.parent / "data" / "processed" / "ori_json.json"]
        },
        {
            "number": 4,
            "module": "04build_db",
            "description": "数据库构建",
            "dependencies": [project_root.parent / "data" / "processed" / "output_sheet3.json"]
        },
        {
            "number": 5,
            "module": "05similarCompare",
            "description": "相似度比较",
            "dependencies": [
                project_root.parent / "data" / "processed" / "output_sheet3.json"
            ]
        },
        {
            "number": 6,
            "module": "06llmPostsimilar",
            "description": "LLM后处理相似度分析",
            "dependencies": []
        },
        {
            "number": 7,
            "module": "07feature2entity",
            "description": "特征到实体转换",
            "dependencies": []
        },
        {
            "number": 8,
            "module": "08content2grad",
            "description": "内容到等级转换",
            "dependencies": [project_root.parent / "data" / "processed" / "processed_results.jsonl"]
        },
        {
            "number": 9,
            "module": "09tokafka",
            "description": "生成Kafka输出数据",
            "dependencies": [project_root.parent / "data" / "processed" / "final.jsonl"]
        }
    ]
    
    # 解析命令行参数
    import argparse
    parser = argparse.ArgumentParser(description='数据处理流程控制器')
    parser.add_argument('--step', type=int, help='执行特定步骤 (1-9)')
    parser.add_argument('--all', action='store_true', help='执行所有步骤')
    parser.add_argument('--from-step', type=int, help='从指定步骤开始执行')
    parser.add_argument('--specification-u-id', type=str, help='行业标识符')
    
    args = parser.parse_args()
    specification_u_id = args.specification_u_id
    
    if args.all:
        # 执行所有步骤
        for step in steps:
            success = run_step(
                step["number"], 
                step["module"], 
                step["dependencies"],
                specification_u_id=specification_u_id
            )
            if not success:
                print(f"步骤 {step['number']} 执行失败，停止执行后续步骤")
                return 1
    elif args.step:
        # 执行特定步骤
        if 1 <= args.step <= 9:
            step = steps[args.step - 1]
            run_step(step["number"], step["module"], step["dependencies"], specification_u_id=specification_u_id)
        else:
            print(f"无效步骤: {args.step}，请输入1-9之间的数字")
            return 1
    elif args.from_step:
        # 从指定步骤开始执行
        if 1 <= args.from_step <= 9:
            for i in range(args.from_step - 1, len(steps)):
                step = steps[i]
                success = run_step(
                    step["number"], 
                    step["module"], 
                    step["dependencies"],
                    specification_u_id=specification_u_id
                )
                if not success:
                    print(f"步骤 {step['number']} 执行失败，停止执行后续步骤")
                    return 1
        else:
            print(f"无效步骤: {args.from_step}，请输入1-9之间的数字")
            return 1
    else:
        # 显示帮助信息
        parser.print_help()
        print("\n步骤说明:")
        for step in steps:
            print(f"  {step['number']}. {step['description']}")
        return 0
    
    print("\n数据处理流程执行完成")
    return 0

if __name__ == "__main__":
    sys.exit(main())