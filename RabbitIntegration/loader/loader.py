import infofile
import uproot
import awkward as ak
import os, json
import pika

# RabbitMQ connection parameters
RABBITMQ_HOST = "localhost"
QUEUE_NAME = "data_chunks"

# Define the ROOT file and data processing parameters
DATA_PATH = "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/"
SAMPLES = {'data': ['data_A']}
CHUNK_SIZE = 10000

def send_to_queue(message):
    """
    Send a message to the RabbitMQ queue.
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBITMQ_HOST))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME)
    channel.basic_publish(exchange='', routing_key=QUEUE_NAME, body=json.dumps(message))
    connection.close()
    print(f"Sent to queue: {message}")
    
