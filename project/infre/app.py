import os
import datetime
from typing import List, Optional
from fastapi import FastAPI, UploadFile, File, Form, status
from fastapi.responses import JSONResponse
import urllib.parse
import sys
import subprocess
import threading
import time
import asyncio
from kafka import KafkaProducer
import json

# 初始化 Kafka Producer
producer = KafkaProducer(bootstrap_servers=['kafka1:9092'])

app = FastAPI(title="规范解读任务接口")

def send_kafka_task_status(task_uid: str, state: int, error_msg: Optional[str] = None, push_date: str = None):
    """
    向 Kafka 发送任务状态消息
    :param task_uid: 任务唯一标识
    :param state: 任务状态 (1: 待执行, 2: 执行中, 4: 任务完成, 6: 任务失败)
    :param error_msg: 错误信息 (仅在失败时使用)
    :param push_date: 时间戳
    """
    message = {
        "PushContentType": 0,
        "PushDate": push_date or datetime.datetime.now().isoformat(),
        "TaskUId": task_uid,
        "Value": {
            "State": state,
            "ErrorMsg": error_msg
        }
    }
    
    serialized_message = json.dumps(message).encode('utf-8')
    producer.send('scip_specification_analysis_task', serialized_message)
    producer.flush()  # 添加flush确保消息发送
    print(f"已发送 Kafka 消息: {message}")
    
def create_directory_structure(base_dir: str, structure: dict):
    """递归创建目录结构"""
    for name, children in structure.items():
        path = os.path.join(base_dir, name)
        os.makedirs(path, exist_ok=True)
        print(f"Created directory: {path}")
        if children:
            create_directory_structure(path, children)

def get_safe_filename(filename: str) -> str:
    """
    获取安全的文件名，处理中文乱码问题
    使用原始文件名但确保路径安全
    """
    # 获取文件名（不含路径）
    basename = os.path.basename(filename)
    
    # 在Linux系统上直接使用原始文件名（支持UTF-8）
    if sys.platform != "win32":
        return basename
    
    # 在Windows系统上处理中文编码问题
    try:
        # 尝试使用GBK编码（Windows默认编码）
        return basename.encode('gbk').decode('gbk')
    except:
        # 如果GBK编码失败，使用URL编码
        return urllib.parse.quote(basename)

def get_full_path(base_dir: str, specificationUId: str, version_dir: str, file_type: str, filename: str) -> str:
    """构建完整的文件路径"""
    return os.path.join(
        base_dir,
        f"standard_{specificationUId}",
        "01_raw_documents",
        version_dir,
        file_type,
        filename
    )

async def save_file(file: UploadFile, target_path: str):
    """安全保存文件到指定路径"""
    target_dir = os.path.dirname(target_path)
    os.makedirs(target_dir, exist_ok=True)
    print(f"确保目录存在: {target_dir}")
    
    # 在Windows上确保使用二进制模式写入
    with open(target_path, "wb") as buffer:
        content = await file.read()
        buffer.write(content)
    
    print(f"保存文件: {target_path}")
    print(f"文件大小: {len(content)} 字节")
    return target_path

def run_processing_pipeline(specification_u_id: str, task_u_id: str):
    """在后台运行处理流程并记录输出到日志文件"""
    try:
        # 获取项目根目录
        project_root = "/usr/local/app/cml/volume/project01"
        
        # 构建日志目录
        log_dir = os.path.join(project_root, "logs")
        os.makedirs(log_dir, exist_ok=True)
        
        # 构建日志文件路径，基于规范ID和当前时间
        current_time = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file_path = os.path.join(log_dir, f"processing_{specification_u_id}_{current_time}.log")
        
        # 构建命令
        cmd = [
            sys.executable, "-m", "src_01.main_control", 
            "--all", 
            "--specification-u-id", specification_u_id
        ]
        
        print(f"启动处理流程: {' '.join(cmd)}")
        print(f"日志文件: {log_file_path}")
        
        # 打开日志文件以写入标准输出和错误输出
        with open(log_file_path, "w") as log_file:
            # 在子进程中运行命令，并将 stdout 和 stderr 写入日志文件
            process = subprocess.Popen(
                cmd,
                cwd=project_root,
                stdout=log_file,
                stderr=subprocess.STDOUT,  # 将 stderr 合并到 stdout
                text=True
            )
            
            # 等待进程完成
            process.wait()
        
        if process.returncode == 0:
            print(f"处理流程执行成功: {specification_u_id}")
            # 发送任务完成状态
            send_kafka_task_status(task_u_id, state=4)
            
            # 读取生成的分级结果文件并发送到Kafka
            grad_file_path = "/usr/local/app/cml/volume/project01/data/processed/kafka_output_grad.json"
            try:
                if os.path.exists(grad_file_path):
                    with open(grad_file_path, 'r', encoding='utf-8') as f:
                        grad_data = json.load(f)
                    
                    # 构建分级结果消息
                    grad_message = {
                        "PushContentType": 1,
                        "PushDate": datetime.datetime.now().isoformat(),
                        "TaskUId": task_u_id,
                        "Value": grad_data
                    }
                    grad_message = json.dumps(grad_message).encode('utf-8')
                    # 发送分级结果到Kafka
                    producer.send('scip_specification_analysis_task', grad_message)
                    producer.flush()  # 添加flush确保消息发送
                    print(f"已发送分级结果到Kafka: {grad_message}")
                else:
                    print(f"分级结果文件不存在: {grad_file_path}")
            except Exception as e:
                print(f"读取或发送分级结果时出错: {str(e)}")
                import traceback
                traceback.print_exc()
                
            # 读取生成的特征结果文件并分批发送到Kafka
            feat_file_path = "/usr/local/app/cml/volume/project01/data/processed/kafka_output_feat.json"
            try:
                if os.path.exists(feat_file_path):
                    with open(feat_file_path, 'r', encoding='utf-8') as f:
                        feat_data = json.load(f)
                    
                    # 将数据分批，每批10个元素
                    batch_size = 10
                    for i in range(0, len(feat_data), batch_size):
                        batch = feat_data[i:i + batch_size]
                        
                        # 构建特征结果消息
                        feat_message = {
                            "PushContentType": 3,
                            "PushDate": datetime.datetime.now().isoformat(),
                            "TaskUId": task_u_id,
                            "Value": batch
                        }
                        feat_message = json.dumps(feat_message).encode('utf-8')
                        
                        # 发送特征结果批次到Kafka
                        producer.send('scip_specification_analysis_task', feat_message)
                        producer.flush()  # 添加flush确保消息发送
                        print(f"已发送特征结果批次 {i//batch_size + 1} 到Kafka，包含 {len(batch)} 个项目")
                else:
                    print(f"特征结果文件不存在: {feat_file_path}")
            except Exception as e:
                print(f"读取或发送特征结果时出错: {str(e)}")
                import traceback
                traceback.print_exc()
                
            # 读取生成的数据分类结果文件并逐个发送到Kafka
            data_file_path = "/usr/local/app/cml/volume/project01/data/processed/kafka_output_data.json"
            try:
                if os.path.exists(data_file_path):
                    with open(data_file_path, 'r', encoding='utf-8') as f:
                        data_data = json.load(f)
                    
                    # 为每个顶级元素创建单独的消息
                    for i, data_item in enumerate(data_data):
                        # 构建数据分类结果消息
                        data_message = {
                            "PushContentType": 2,
                            "PushDate": datetime.datetime.now().isoformat(),
                            "TaskUId": task_u_id,
                            "Value": [data_item]  # 包装成数组以匹配模板格式
                        }
                        
                        data_message = json.dumps(data_message).encode('utf-8')
                        
                        # 发送数据分类结果到Kafka
                        producer.send('scip_specification_analysis_task', data_message)
                        producer.flush()  # 添加flush确保消息发送
                        print(f"已发送数据分类结果项 {i+1} 到Kafka")
                else:
                    print(f"数据分类结果文件不存在: {data_file_path}")
            except Exception as e:
                print(f"读取或发送数据分类结果时出错: {str(e)}")
                import traceback
                traceback.print_exc()
        else:
            print(f"处理流程执行失败: {specification_u_id}")
            send_kafka_task_status(task_u_id, state=6, error_msg="处理流程执行失败")
            
    except Exception as e:
        print(f"执行处理流程时出错: {str(e)}")
        import traceback
        traceback.print_exc()
        send_kafka_task_status(task_u_id, state=6, error_msg=str(e))

# 处理任务创建接口
@app.post("/api/v1/specification/tasks",
          responses={
              400: {"description": "无效参数或文件错误"},
              500: {"description": "服务器内部错误"}
          })

async def create_specification_task(
    taskUId: str = Form(..., description="任务唯一标识"),
    specificationUId: str = Form(..., description="规范唯一标识"),
    specificationName: str = Form(..., description="规范名称"),
    excelFile: UploadFile = File(..., description="Excel规范文件"),
    supplementFiles: Optional[List[UploadFile]] = File(None, description="补充文档(PDF/Word)")
):
    """
    创建规范解读任务接口
    - 接收任务参数和文件
    - 返回任务接收状态
    - 实际处理通过后台消息队列进行
    """
    
    # 记录上传时间
    upload_time = datetime.datetime.now().isoformat()
    
    # 发送任务状态: 待执行
    send_kafka_task_status(taskUId, state=1, push_date=upload_time)

    # 验证任务唯一标识格式
    if not taskUId or len(taskUId) < 6:
        send_kafka_task_status(taskUId, state=6, error_msg="无效的任务唯一标识")
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"success": False, "code": 400, "msg": "无效的任务唯一标识"}
        )
    
    # 验证Excel文件格式
    excel_ext = os.path.splitext(excelFile.filename)[1].lower()
    if excel_ext not in [".xls", ".xlsx"]:
        send_kafka_task_status(taskUId, state=6, error_msg="无效的Excel文件格式")
        return JSONResponse(
            status_code=status.HTTP_400_BAD_REQUEST,
            content={"success": False, "code": 400, "msg": "无效的Excel文件格式"}
        )
    
    # 验证补充文件格式
    if supplementFiles:
        for file in supplementFiles:
            ext = os.path.splitext(file.filename)[1].lower()
            if ext not in [".pdf", ".doc", ".docx"]:
                send_kafka_task_status(taskUId, state=6, error_msg=f"无效的补充文件格式: {file.filename}")
                return JSONResponse(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    content={"success": False, "code": 400, "msg": f"无效的补充文件格式: {file.filename}"}
                )
    
    try:
        # 基础目录结构
        base_dir = os.path.abspath(r"/usr/local/app/cml/volume/project01/classification_and_grading")
        
        # 完整的目录结构定义
        directory_structure = {
            f"standard_{specificationUId}": {
                "01_raw_documents": {
                    "excel": {},  # Excel文件存放目录
                    "supplements": {}  # 补充文件存放目录
                }
            }
        }
        
        # 创建所有必要的目录
        create_directory_structure(base_dir, directory_structure)
        
        # 构建Excel文件路径 - 使用原始文件名
        excel_filename = get_safe_filename(excelFile.filename)
        excel_path = os.path.join(
            base_dir,
            f"standard_{specificationUId}",
            "01_raw_documents",
            "excel",
            excel_filename
        )
        
        # 保存Excel文件
        saved_excel_path = await save_file(excelFile, excel_path)
        
        # 保存补充文件
        saved_supp_paths = []
        if supplementFiles:
            for file in supplementFiles:
                supp_filename = get_safe_filename(file.filename)
                supp_path = os.path.join(
                    base_dir,
                    f"standard_{specificationUId}",
                    "01_raw_documents",
                    "supplements",
                    supp_filename
                )
                saved_path = await save_file(file, supp_path)
                saved_supp_paths.append(saved_path)
        
        # 打印所有保存的文件路径
        print("\n" + "=" * 50)
        print("文件保存位置验证".center(50))
        print("=" * 50)
        print(f"Excel文件: {saved_excel_path}")
        for i, path in enumerate(saved_supp_paths):
            print(f"补充文件 #{i+1}: {path}")
        print("=" * 50 + "\n")
        
        # 验证文件是否实际保存
        if not os.path.exists(saved_excel_path):
            send_kafka_task_status(taskUId, state=6, error_msg="Excel文件保存失败")
            return JSONResponse(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                content={"success": False, "code": 500, "msg": "Excel文件保存失败"}
            )
        
        # 启动后台处理流程
        print(f"启动后台处理流程，specificationUId: {specificationUId}")
        # 使用线程在后台运行处理流程，避免阻塞API响应

        # 发送任务状态: 执行中
        send_kafka_task_status(taskUId, state=2)

        thread = threading.Thread(
            target=run_processing_pipeline, 
            args=(specificationUId, taskUId),
            daemon=False
        )
        thread.start()
        
        print(f"Successfully created task structure for taskUId: {taskUId}")
        
        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content={
                "success": True, 
                "code": 200, 
                "msg": "任务创建成功，处理流程已在后台启动"
            }
        )
    
    except Exception as e:
        import traceback
        traceback.print_exc()
        send_kafka_task_status(taskUId, state=6, error_msg=str(e))
        return JSONResponse(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            content={"success": False, "code": 500, "msg": f"服务器内部错误: {str(e)}"}
        )

# 直接运行应用的核心代码
if __name__ == "__main__":
    import uvicorn
    
    print("\n" + "=" * 50)
    print("启动规范解读任务服务".center(50))
    print("=" * 50)
    print("API 地址: http://127.0.0.1:64001")
    print("文档地址: http://127.0.0.1:64001/docs")
    print("文件存储目录: /classification_and_grading/")
    print("=" * 50 + "\n")
    
    # 创建基础存储目录
    base_dir = os.path.abspath(r"/usr/local/app/cml/volume/project01/classification_and_grading")
    os.makedirs(base_dir, exist_ok=True)
    print(f"基础存储目录: {base_dir}")
    
    # 获取当前文件名
    module_name = os.path.splitext(os.path.basename(__file__))[0]
    
    # 设置系统编码为UTF-8
    if sys.version_info >= (3, 7):
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    
    # 以字符串形式传递应用
    uvicorn.run(
        f"{module_name}:app",  # 使用当前模块的路径
        host="0.0.0.0",      # 允许所有网络接口访问
        port=64001,
        reload=False,         # 生产模式，禁用自动重启
        log_level="info",    # 控制台日志级别
        access_log=True,     # 记录访问日志
        log_config=None       # 使用默认日志配置
    )