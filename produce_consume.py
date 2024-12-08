from argparse import ArgumentParser
from confluent_kafka import Producer, Consumer, serialization
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.serialization import Serializer, Deserializer
from faker import Faker
from time import sleep
from multiprocessing import Pool
from random import randint
import json

TOPIC = 'yapr'
TOPIC_PARTITIONS = 3


class Message:
    def __init__(self, user_id, name):
        self.id = user_id
        self.name = name

class UserSerializer(Serializer):
   def __call__(self, obj: Message, ctx=None):
       name_bytes = obj.name.encode("utf-8")
       name_size = len(name_bytes)

       result = name_size.to_bytes(4, byteorder="big")
       result += name_bytes
       result += obj.id.to_bytes(4, byteorder="big")
       return result

class UserDeserializer(Deserializer):
   def __call__(self, value: bytes, ctx=None):
       if not value:
           return None
       name_size = int.from_bytes(value[:4], byteorder="big")
       name = value[4:4 + name_size].decode("utf-8")
       user_id = int.from_bytes(value[4 + name_size:], byteorder="big")
       return Message(user_id, name)

def create_topic():
    admin = AdminClient(
        {
            'bootstrap.servers': "localhost:9092"
        }
    )
    topic = list([NewTopic(TOPIC, TOPIC_PARTITIONS)])
    admin.create_topics(topic)


def produce():
    
    conf = {
        "bootstrap.servers": "localhost:9094",
        "acks": "all"
    }
    # Создание продюсера
    producer = Producer(conf)
    serializer = UserSerializer()
    try:
        while True:
            msg = Message(randint(1, TOPIC_PARTITIONS), Faker().name())
            value = serializer(msg)
            # Отправка сообщения
            producer.produce(
                topic=TOPIC,
                key='id'+str(randint(1, TOPIC_PARTITIONS)),
                value=value,
            )
            sleep(1)
            producer.flush()
    except KeyboardInterrupt:
        print('Caught ctlr+C')
    finally:
        producer.flush()

def consume(poll=1):
    conf = {
        "bootstrap.servers": "localhost:9094",
        "group.id": "my-group",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    }
    if poll:
        conf["enable.auto.commit"] = True
    deserializer = UserDeserializer()
    # Создание консьюмера
    consumer = Consumer(conf)

    # Подписка на топик
    consumer.subscribe([TOPIC])

    # Чтение сообщений в бесконечном цикле
    try:
        while True:
            # Получение сообщений
            msg = consumer.poll(poll)

            if msg is None:
                continue
            if msg.error():
                print(f"Ошибка: {msg.error()}")
                continue

            key = msg.key().decode("utf-8")
            value = deserializer(msg.value())
            if not poll:
                consumer.commit(asynchronous=False)
            print(f"Получено сообщение: key={key}, value={value.name}, offset={msg.offset()}, partition={msg.partition()}")
    except KeyboardInterrupt:
        print('caught ctrl+C')
    finally:
        # Закрытие консьюмера
        consumer.close()


if __name__ == '__main__':
    parser = ArgumentParser(usage='Simple Kafka producer/consumer')
    parser.add_argument('-p', action='store_true', help='Produce')
    parser.add_argument('-c', action='store_true', help='Consume')
    parser.add_argument('-t', action='store_true', help='Create topic')
    args = parser.parse_args()
    if args.p:
        produce()
    # Идея запускать одновременно два консьюмера с разными параметрами
    elif args.c:
        with Pool(2) as p:
            p.map(consume(), [0.1, None])
    elif args.t:
        create_topic()
    else:
        print('Unknown action')
