#模型下载
from modelscope import snapshot_download
model_dir = snapshot_download('Qwen/Qwen3-Embedding-0.6B', cache_dir='/usr/local/app/cml/volume/project/db')