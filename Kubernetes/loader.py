import infofile
import uproot
import awkward as ak
import os, json
import pika, time
from constants import samples, variables, weight_variables
import requests, aiohttp

# RabbitMQ connection parameters
RABBITMQ_HOST = os.getenv('RABBITMQ_HOST', 'rabbitmq')
credentials = pika.PlainCredentials('user', 'password')
parameters = pika.ConnectionParameters(RABBITMQ_HOST, 5672, '/', credentials)

QUEUE_NAME = "data_chunks"

DATA_PATH = "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/"
CHUNK_SIZE = 100000

def publish_message(channel, queue, message):
    """
    Publish a message to RabbitMQ.
    """
    channel.basic_publish(exchange='', routing_key=queue, body=json.dumps(message))


def load_and_split_data(channel, sample):
    """
    Load ROOT files, split into chunks, and publish chunk metadata to RabbitMQ.
    """
    print(f'Processing {sample} samples')

    for val in samples[sample]['list']:
        if sample == 'data':
            prefix = "Data/"  # Data prefix
        else:  # MC prefix
            prefix = f"MC/mc_{infofile.infos[val]['DSID']}."
        file_string = DATA_PATH + prefix + val + ".4lep.root"

        # Open file
        file = uproot.open(file_string)
        tree = file["mini"]
        
        sample_data = []

        for idx, data in enumerate(tree.iterate(variables + weight_variables, library="ak", step_size=CHUNK_SIZE)):
            
            chunk_data = {
                'sample': sample,
                'val': val,
                'idx': idx,
                'data': ak.to_list(data), #serialized data
            }
            
            print(f"Publishing chunk: {chunk_data['val']}-{idx}")
            publish_message(channel, QUEUE_NAME, chunk_data)


if __name__ == "__main__":
    max_retries = 20
    retry_delay = 5  # seconds

    for attempt in range(max_retries):
        try:
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            print("Connected to RabbitMQ")
            channel.queue_declare(queue=QUEUE_NAME)
            break
        except pika.exceptions.AMQPConnectionError as e:
            print(f"Attempt {attempt + 1}/{max_retries} failed: {e}")
            time.sleep(retry_delay)
    else:
        print("Failed to connect to RabbitMQ after several attempts")
        exit(1)

    # # Connect to RabbitMQ
    # connection = pika.BlockingConnection(parameters)
    # channel = connection.channel()

    # # Declare a task queue

    # Process each sample and publish chunks to RabbitMQ
    for sample_name in samples:
        load_and_split_data(channel, sample_name)

    # Signal completion
    publish_message(channel, QUEUE_NAME, {'done': True})

    print("Data loading and chunking complete.")

    # Close the connection
    connection.close()
    
    
    
