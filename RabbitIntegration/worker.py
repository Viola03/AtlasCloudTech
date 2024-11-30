import pika
import os
import shutil
import awkward as ak
import time, vector 
import json
from constants import variables, weight_variables, MeV, GeV, lumi
import infofile

# Functions
# Cut lepton type (electron type is 11,  muon type is 13)
def cut_lep_type(lep_type):
    sum_lep_type = lep_type[:, 0] + lep_type[:, 1] + lep_type[:, 2] + lep_type[:, 3]
    lep_type_cut_bool = (sum_lep_type != 44) & (sum_lep_type != 48) & (sum_lep_type != 52)
    return lep_type_cut_bool # True means we should remove this entry (lepton type does not match)

# Cut lepton charge
def cut_lep_charge(lep_charge):
    # first lepton in each event is [:, 0], 2nd lepton is [:, 1] etc
    sum_lep_charge = lep_charge[:, 0] + lep_charge[:, 1] + lep_charge[:, 2] + lep_charge[:, 3] != 0
    return sum_lep_charge # True means we should remove this entry (sum of lepton charges is not equal to 0)

# Calculate invariant mass of the 4-lepton state
# [:, i] selects the i-th lepton in each event
def calc_mass(lep_pt, lep_eta, lep_phi, lep_E):
    p4 = vector.zip({"pt": lep_pt, "eta": lep_eta, "phi": lep_phi, "E": lep_E})
    invariant_mass = (p4[:, 0] + p4[:, 1] + p4[:, 2] + p4[:, 3]).M * MeV # .M calculates the invariant mass
    return invariant_mass

def calc_weight(weight_variables, sample, events):
    info = infofile.infos[sample]
    xsec_weight = (lumi*1000*info["xsec"])/(info["sumw"]*info["red_eff"]) #*1000 to go from fb-1 to pb-1
    total_weight = xsec_weight 
    for variable in weight_variables:
        total_weight = total_weight * events[variable]
    return total_weight

# RabbitMQ setup
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
credentials = pika.PlainCredentials('user', 'password')
parameters = pika.ConnectionParameters(RABBITMQ_HOST, 5672, '/', credentials)

QUEUE_NAME = "data_chunks"
RESULTS_QUEUE = "processed_chunks"


def process_chunk(chunk_data):
    """
    Process a single chunk: Apply filters, calculate invariant mass, and save results.
    """
    
    print(f"Processing {chunk_data['val']} {chunk_data['idx']}...")
    
    list_data = chunk_data['data']
    data = ak.from_iter(list_data)

    val = chunk_data['val']
    
    nIn = len(data)
 
    # Apply filters and calculations
    data['leading_lep_pt'] = data['lep_pt'][:, 0]
    data['sub_leading_lep_pt'] = data['lep_pt'][:, 1]
    data['third_leading_lep_pt'] = data['lep_pt'][:, 2]
    data['last_lep_pt'] = data['lep_pt'][:, 3]
    
    # Cuts
    lep_type = data['lep_type']
    data = data[~cut_lep_type(lep_type)]
    lep_charge = data['lep_charge']
    data = data[~cut_lep_charge(lep_charge)]
    
    # Invariant Mass
    data['mass'] = calc_mass(data['lep_pt'], data['lep_eta'], data['lep_phi'], data['lep_E'])

    # Store Monte Carlo weights in the data
    if 'data' not in val:  # Only calculates weights if the data is MC
        data['totalWeight'] = calc_weight(weight_variables, val, data)
        nOut = sum(data['totalWeight'])  # sum of weights passing cuts in this batch
    else:
        nOut = len(data)
        
    print("\t\t nIn: "+str(nIn)+",\t nOut: \t"+str(nOut))  # events before and after

    processed_data = ak.to_list(data)
    
    print(f"Chunk {chunk_data['val']}-{chunk_data['idx']} processed.")
    
    return {
        'sample': chunk_data['sample'],
        'val': chunk_data['val'],
        'idx': chunk_data['idx'],
        'data': processed_data,  # Serialized processed data
    }


def callback(ch, method, properties, body):
    """
    Callback for consuming messages from RabbitMQ.
    """
    try:
        # Deserialize the message
        message = json.loads(body)

        if 'done' in message:
            print("All chunks processed. Exiting worker.")
            ch.basic_ack(delivery_tag=method.delivery_tag)
            ch.stop_consuming()
            return

        # Process the chunk
        result = process_chunk(message)

        # Publish the processed data to the results queue
        ch.basic_publish(exchange='', routing_key=RESULTS_QUEUE, body=json.dumps(result))
        print(f"Published processed chunk: {result['val']}-{result['idx']}")

        # Acknowledge the task
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"Error processing message: {e}")
        # Optionally requeue the message
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

def publish_message(channel, queue, message):
    """
    Publish a message to RabbitMQ.
    """
    channel.basic_publish(exchange='', routing_key=queue, body=json.dumps(message))

def main():
    max_retries = 20
    retry_delay = 5  # seconds

    for attempt in range(max_retries):
        try:
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            print("Connected to RabbitMQ")
            channel.queue_declare(queue=QUEUE_NAME)
            channel.queue_declare(queue=RESULTS_QUEUE)
            break
        except pika.exceptions.AMQPConnectionError as e:
            print(f"Attempt {attempt + 1}/{max_retries} failed: {e}")
            time.sleep(retry_delay)
    else:
        print("Failed to connect to RabbitMQ after several attempts")
        exit(1)

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)

    print('Waiting for messages')
    channel.start_consuming()
    
    publish_message(channel, RESULTS_QUEUE, {'done': True})
    

# def main():
#     # Connect to RabbitMQ
#     connection = pika.BlockingConnection(parameters)
#     channel = connection.channel()

#     # Declare the queues
#     channel.queue_declare(queue=QUEUE_NAME)
#     channel.queue_declare(queue=RESULTS_QUEUE)

#     # Start consuming messages from the data_chunks queue
#     print("Worker waiting for tasks...")
#     channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback)

#     channel.start_consuming()


if __name__ == "__main__":
    main()
    
    