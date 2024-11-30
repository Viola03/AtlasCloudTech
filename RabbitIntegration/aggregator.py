# import pika
# import json, os, time
# import awkward as ak
# import numpy as np
# import matplotlib.pyplot as plt
# from matplotlib.ticker import AutoMinorLocator
# from constants import variables, weight_variables, samples, lumi, fraction


# # RabbitMQ setup
# RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
# credentials = pika.PlainCredentials('user', 'password')
# parameters = pika.ConnectionParameters(RABBITMQ_HOST, 5672, '/', credentials)

# RESULTS_QUEUE = "processed_chunks"
# OUTPUT_PATH = "/output/4lep_invariant_mass.png"  # Path to save the plot in a non-shared volume

# step_size = 5  # Bin width
# xmin, xmax = 80, 250  # Plot x-axis range
# bin_edges = np.arange(xmin, xmax + step_size, step_size)  # Bin edges
# bin_centres = (bin_edges[:-1] + bin_edges[1:]) / 2  # Bin centers

# # Global data storage for aggregation
# aggregated_data = {key: [] for key in samples.keys()}

# def generate_plot():
#     """
#     Generate and save the final histogram plot.
#     """
#     print("Generating plot...")

#     # Combine all chunks into a single Awkward Array for each sample
#     combined_data = {key: ak.concatenate(aggregated_data[key], axis=0) if aggregated_data[key] else ak.Array([]) for key in samples.keys()}

#     # Histogram for data points
#     data_x, _ = np.histogram(ak.to_numpy(combined_data['data']['mass']), bins=bin_edges)
#     data_x_errors = np.sqrt(data_x)  # Statistical error on the data

#     # Plot setup
#     plt.figure(figsize=(10, 6))
#     plt.errorbar(bin_centres, data_x, yerr=data_x_errors, fmt='ko', label='Data')  # Data points

#     # Histogram for Monte Carlo background
#     mc_samples = []
#     mc_weights = []
#     mc_colors = []
#     mc_labels = []

#     for key, sample_info in samples.items():
#         if key not in ['data', r'Signal ($m_H$ = 125 GeV)']:
#             if len(combined_data[key]) > 0:
#                 mc_samples.append(ak.to_numpy(combined_data[key]['mass']))
#                 mc_weights.append(ak.to_numpy(combined_data[key].get('totalWeight', np.ones(len(combined_data[key])))))
#                 mc_colors.append(sample_info['color'])
#                 mc_labels.append(key)

#     if mc_samples:
#         mc_heights = plt.hist(mc_samples, bins=bin_edges, weights=mc_weights, stacked=True, color=mc_colors, label=mc_labels)

#     # Plot signal
#     signal_data = combined_data[r'Signal ($m_H$ = 125 GeV)']
#     if len(signal_data) > 0:
#         plt.hist(ak.to_numpy(signal_data['mass']), bins=bin_edges, weights=ak.to_numpy(signal_data['totalWeight']), color=samples[r'Signal ($m_H$ = 125 GeV)']['color'], label=r'Signal ($m_H$ = 125 GeV)', alpha=0.7)

#     # Formatting and labels
#     plt.xlabel(r'4-lepton invariant mass $\mathrm{m_{4l}}$ [GeV]', fontsize=13)
#     plt.ylabel(f'Events / {step_size} GeV', fontsize=13)
#     plt.title('H → ZZ* → 4l Analysis', fontsize=15)
#     plt.legend()
#     plt.grid(True, linestyle='--', alpha=0.6)
#     plt.savefig(OUTPUT_PATH)
#     print(f"Plot saved to {OUTPUT_PATH}")


# def callback(ch, method, properties, body):
#     """
#     Callback for consuming messages from RabbitMQ.
#     """
#     global aggregated_data

#     message = json.loads(body)

#     if 'done' in message:
#         print("Received 'done' signal. All chunks processed.")
#         ch.basic_ack(delivery_tag=method.delivery_tag)
#         ch.stop_consuming()
#         generate_plot()  # Generate the plot once all chunks are processed
#         return

#     # Process the chunk
#     sample_key = message['sample']
#     chunk_data = message['data']

#     if sample_key in aggregated_data:
#         aggregated_data[sample_key].append(ak.from_iter(chunk_data))
#         print(f"Added chunk {message['val']}-{message['idx']} to {sample_key}")

#     ch.basic_ack(delivery_tag=method.delivery_tag)

# def main():
    
#     max_retries = 20
#     retry_delay = 5  # seconds

#     for attempt in range(max_retries):
#         try:
#             connection = pika.BlockingConnection(parameters)
#             channel = connection.channel()
#             print("Connected to RabbitMQ")
#             channel.queue_declare(queue=RESULTS_QUEUE)
#             break
#         except pika.exceptions.AMQPConnectionError as e:
#             print(f"Attempt {attempt + 1}/{max_retries} failed: {e}")
#             time.sleep(retry_delay)
#     else:
#         print("Failed to connect to RabbitMQ after several attempts")
#         exit(1)
    
#     # Start consuming messages from the processed_chunks queue
#     print("Aggregator waiting for processed chunks...")
#     channel.basic_consume(queue=RESULTS_QUEUE, on_message_callback=callback)
#     channel.start_consuming()


# if __name__ == "__main__":
#     main()

import pika
import json, os, time
import awkward as ak
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator

# RabbitMQ setup
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
credentials = pika.PlainCredentials('user', 'password')
parameters = pika.ConnectionParameters(RABBITMQ_HOST, 5672, '/', credentials)

RESULTS_QUEUE = "processed_chunks"
OUTPUT_PATH = "/output/4lep_invariant_mass.png"  # Save plot in non-shared volume

# Luminosity and bin settings
lumi = 10  # Integrated luminosity in fb^-1
fraction = 1.0  # Fraction of luminosity used
step_size = 5  # Bin width
xmin, xmax = 80, 250  # Plot x-axis range
bin_edges = np.arange(xmin, xmax + step_size, step_size)  # Bin edges
bin_centres = (bin_edges[:-1] + bin_edges[1:]) / 2  # Bin centers

samples = {
    'data': {'list': ['data_A', 'data_B', 'data_C', 'data_D'], 'color': 'black'},
    r'Background $Z,t\bar{t}$': {'list': ['Zee', 'Zmumu', 'ttbar_lep'], 'color': "#6b59d3"},
    r'Background $ZZ^*$': {'list': ['llll'], 'color': "#ff0000"},
    r'Signal ($m_H$ = 125 GeV)': {'list': ['ggH125_ZZ4lep', 'VBFH125_ZZ4lep', 'WH125_ZZ4lep', 'ZH125_ZZ4lep'], 'color': "#00cdff"},
}

# Aggregated data storage
grouped_data = {key: [] for key in samples.keys()}


def generate_plot():
    """
    Generate and save the plot of invariant mass.
    """
    print("Generating plot...")

    # Combine all chunks into a single Awkward Array for each sample
    all_data = {key: ak.concatenate(grouped_data[key], axis=0) if grouped_data[key] else ak.Array([]) for key in samples.keys()}

    # Histogram for data points
    data_x, _ = np.histogram(ak.to_numpy(all_data['data']['mass']), bins=bin_edges)
    data_x_errors = np.sqrt(data_x)  # Statistical error on the data

    # Set up the plot
    plt.figure(figsize=(10, 6))
    plt.errorbar(bin_centres, data_x, yerr=data_x_errors, fmt='ko', label='Data')

    # Monte Carlo background
    mc_samples = []
    mc_weights = []
    mc_colors = []
    mc_labels = []

    for key, sample_info in samples.items():
        if key not in ['data', r'Signal ($m_H$ = 125 GeV)']:
            if len(all_data[key]) > 0:
                mc_samples.append(ak.to_numpy(all_data[key]['mass']))
                mc_weights.append(ak.to_numpy(all_data[key]['totalWeight']))
                mc_colors.append(sample_info['color'])
                mc_labels.append(key)

    if mc_samples:
        plt.hist(mc_samples, bins=bin_edges, weights=mc_weights, stacked=True, color=mc_colors, label=mc_labels)

    # Signal
    signal_data = all_data[r'Signal ($m_H$ = 125 GeV)']
    if len(signal_data) > 0:
        plt.hist(
            ak.to_numpy(signal_data['mass']),
            bins=bin_edges,
            weights=ak.to_numpy(signal_data['totalWeight']),
            color=samples[r'Signal ($m_H$ = 125 GeV)']['color'],
            label=r'Signal ($m_H$ = 125 GeV)',
            alpha=0.7
        )

    # Formatting and labels
    plt.xlabel(r'4-lepton invariant mass $\mathrm{m_{4l}}$ [GeV]', fontsize=13)
    plt.ylabel(f'Events / {step_size} GeV', fontsize=13)
    plt.title('H → ZZ* → 4l Analysis', fontsize=15)
    plt.legend()
    plt.grid(True, linestyle='--', alpha=0.6)
    plt.savefig(OUTPUT_PATH)
    print(f"Plot saved to {OUTPUT_PATH}")


def callback(ch, method, properties, body):
    """
    Callback for consuming messages from RabbitMQ.
    """
    global grouped_data

    message = json.loads(body)

    if 'done' in message:
        print("Received 'done' signal. All chunks processed.")
        ch.basic_ack(delivery_tag=method.delivery_tag)
        ch.stop_consuming()
        generate_plot()  # Generate the plot once all chunks are processed
        return

    # Add chunk data to the corresponding category
    sample_key = message['sample']
    chunk_data = message['data']

    if sample_key in grouped_data:
        grouped_data[sample_key].append(ak.from_iter(chunk_data))
        print(f"Aggregated chunk {message['val']}-{message['idx']} for {sample_key}")

    ch.basic_ack(delivery_tag=method.delivery_tag)


def main():
    # Connect to RabbitMQ
    max_retries = 20
    retry_delay = 5  # seconds

    for attempt in range(max_retries):
        try:
            connection = pika.BlockingConnection(parameters)
            channel = connection.channel()
            print("Connected to RabbitMQ")
            channel.queue_declare(queue=RESULTS_QUEUE)
            break
        except pika.exceptions.AMQPConnectionError as e:
            print(f"Attempt {attempt + 1}/{max_retries} failed: {e}")
            time.sleep(retry_delay)
    else:
        print("Failed to connect to RabbitMQ after several attempts")
        exit(1)

    # Start consuming messages from the processed_chunks queue
    print("Aggregator waiting for processed chunks...")
    channel.basic_consume(queue=RESULTS_QUEUE, on_message_callback=callback)
    channel.start_consuming()


if __name__ == "__main__":
    main()
