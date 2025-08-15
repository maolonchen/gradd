# 创建一个kafka的生产者
from kafka import KafkaProducer
import json

producer = KafkaProducer(bootstrap_servers=['kafka1:9092'])

msg = [
    {
        "PushContentType": 0,
        "DataElement": "客户姓名",
        "DataClassification": "A1-1 自然人身份标识"
    },
    {
        "DataElement": "证件类型及号码",
        "DataClassification": "A1-1 自然人身份标识"
    }
]
msg = json.dumps(msg).encode('utf-8')

producer.send('scip_specification_analysis_task-0', msg)
producer.flush()
