import logging

from confluent_kafka import SerializingProducer
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.serialization import StringDeserializer

class Topic :
  def __init__(self, topic_name) -> None:
    client = AdminClient({'bootstrap.servers': '10.0.10.44:9092'})
    print(client)
    topic = NewTopic(topic_name, num_partitions=1, replication_factor=1)
    try :
      future = client.create_topics([topic])
      for name, future in future.items():
        future.result()
        print("created topic {name}")
    except Exception as err : 
      print(err)


if __name__ == "__main__": 
  customer = Topic('ASYWDB.SEO.CUSTOMERS')
