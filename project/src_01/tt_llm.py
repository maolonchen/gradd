import requests
import json


url = "http://localhost:6006/v1/chat/completions/v1/chat/completions"
# url = "http://192.168.101.108:11434/v1/chat/completions"

headers = {
    "Content-Type": "application/json"
}

payload = {
    "model": "/root/autodl-tmp/model/Qwen/Qwen3-8B",
    # "model": "qwen2.5-7b",
    "messages": [
        {"role": "user", "content": "输入为：客户姓名、证件类型及号码、驾照编号、银行账户等。实体识别输入中所有信息，输出独立数据单元名：- 输出模板：['...','...', ...]的JSON数组，...表示输出的字段。- 字段的数量要完整。"}
    ],
    "temperature": 0.2,
    "top_p": 0.8,
    "top_k": 20,
    "max_tokens": 1024,
    "presence_penalty": 1.5,
    "chat_template_kwargs": {"enable_thinking": True},
    "stream": True  # 启用流式输出
}

try:
    full_response = ""
    print("Bot: ", end="", flush=True)
    
    with requests.post(url, headers=headers, json=payload, stream=True) as response:
        response.raise_for_status()
        
        # 处理流式响应
        for line in response.iter_lines():
            if line:
                # 移除开头的"data: "前缀
                decoded_line = line.decode("utf-8").strip()
                if decoded_line.startswith("data: "):
                    json_str = decoded_line[6:]  # 移除"data: "
                    
                    # 检查流式结束标记
                    if json_str == "[DONE]":
                        break
                    
                    try:
                        data = json.loads(json_str)
                        # 提取内容增量
                        if "content" in data["choices"][0]["delta"]:
                            content = data["choices"][0]["delta"]["content"]
                            full_response += content
                            print(content, end="", flush=True)
                    except json.JSONDecodeError:
                        print(f" [解析错误] ", end="")
                        continue
        print()  # 换行
    
    print("\n完整回复:", full_response)

except requests.exceptions.RequestException as e:
    print(f"请求异常：{e}")
except KeyError:
    print("响应格式错误")