# 创建kafka消费者
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'test-topic',
    bootstrap_servers=['kafka1:9092'],
    auto_offset_reset='smallest',
    # enable_auto_commit=True,
    group_id=None,
)

for message in consumer:
    recv = "%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                        message.offset, message.key,
                                        message.value)
    print(recv)
