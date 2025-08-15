# data path
XLS_ORI_PATH = '/usr/local/app/cml/volume/project01/data/raw/origin_data.xlsx'
XLS_UNCOMBINE_PATH = '/usr/local/app/cml/volume/project01/data/processed/uncombine_data.xlsx'
JSON_ORI_PATH = '/usr/local/app/cml/volume/project01/data/processed/ori_json.json'
MULTI_TREE_PATH = '/usr/local/app/cml/volume/project01/data/processed/multi_tree.txt'
GRAD_ENTITY_PATH = '/usr/local/app/cml/volume/project01/data/processed/output_sheet3.json'
SIMILAR_COMPARE_PATH = "/usr/local/app/cml/volume/project01/data/processed/grad_results.txt"
LLM_POST_SIMILAR_PATH = "/usr/local/app/cml/volume/project01/data/processed/llm_similar_post.txt"
FINAL_PATH = "/usr/local/app/cml/volume/project01/data/processed/processed_results.jsonl"
POST_FINAL_PATH = "/usr/local/app/cml/volume/project01/data/processed/final.jsonl"
KAFKA_DATA_PATH = "/usr/local/app/cml/volume/project01/data/processed/kafka_output_data.json"
KAFKA_GRAD_PATH = "/usr/local/app/cml/volume/project01/data/processed/kafka_output_grad.json"
KAFKA_feat_PATH = "/usr/local/app/cml/volume/project01/data/processed/kafka_output_feat.json"

def get_dynamic_xls_path(specification_u_id=None):
    if specification_u_id:
        return f'/usr/local/app/cml/volume/project01/classification_and_grading/standard_{specification_u_id}/01_raw_documents/excel/oriAI_rules.xlsx'
    return XLS_ORI_PATH

# DB
EMBEDDING_MODEL_PATH = '/usr/local/app/cml/volume/project01/db/Qwen/Qwen3-Embedding-0___6B'
DB_PATH = '/usr/local/app/cml/volume/project01/db/milvus_excel_grading.db'
GRAD_COLLECTION_NAME = 'excel_grading_collection'

# llm
# LLM_API_URL = "http://192.168.101.108:11434/v1/chat/completions"
LLM_API_URL = "https://u343777-9ef7-ac2a0102.westc.gpuhub.com:8443/v1/chat/completions"
# MODEL_NAME = "qwen2.5-7b"
MODEL_NAME = "/root/autodl-tmp/model/Qwen/Qwen3-8B"
HEADERS = {
    "Content-Type": "application/json",
}

def API_PAYLOAD(MODEL_NAME, prompt):
    return {
        "model": MODEL_NAME,
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
        "top_p": 0.8,
        "top_k": 20,
        "max_tokens": 5096,
        "presence_penalty": 1.7,
        "chat_template_kwargs": {"enable_thinking": True},
        "stream": False
    }
