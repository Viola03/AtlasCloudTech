import os
import shutil
import awkward as ak
import pandas
from workerfunctions import *
import time

INPUT_DIR = "data/chunks"
PROCESSING_DIR = "data/processing"
OUTPUT_DIR = "data/processed"
DONE_FILE = "/data/loader_done"

variables = ['lep_pt','lep_eta','lep_phi','lep_E','lep_charge','lep_type']
weight_variables = ["mcWeight", "scaleFactor_PILEUP", "scaleFactor_ELE", "scaleFactor_MUON", "scaleFactor_LepTRIGGER"]

os.makedirs(PROCESSING_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

def process_chunk(chunk_path, output_path):
    """
    Process a single chunk: Apply filters, calculate invariant mass, and save results.
    """
    print(f"Processing {chunk_path}...")
    data = ak.from_parquet(chunk_path)
    
    # Number of events in this batch
    nIn = len(data)
    directory = chunk_path.split('-')[0]
    val = directory.split('/')[2]
    print(val)
 
    # Apply filters and calculations
    # Record transverse momenta 
    data['leading_lep_pt'] = data['lep_pt'][:,0]
    data['sub_leading_lep_pt'] = data['lep_pt'][:,1]
    data['third_leading_lep_pt'] = data['lep_pt'][:,2]
    data['last_lep_pt'] = data['lep_pt'][:,3]
    
    # Cuts
    lep_type = data['lep_type']
    data = data[~cut_lep_type(lep_type)]
    lep_charge = data['lep_charge']
    data = data[~cut_lep_charge(lep_charge)]
    
    # Invariant Mass
    data['mass'] = calc_mass(data['lep_pt'], data['lep_eta'], data['lep_phi'], data['lep_E'])

    # Store Monte Carlo weights in the data
    if 'data' not in val: # Only calculates weights if the data is MC
        data['totalWeight'] = calc_weight(weight_variables, val, data)
        nOut = sum(data['totalWeight']) # sum of weights passing cuts in this batch 
    else:
        nOut = len(data)
        
    print("\t\t nIn: "+str(nIn)+",\t nOut: \t"+str(nOut)) # events before and after

    ak.to_parquet(data, output_path)
    print(f"Processed data saved to {output_path}")

if __name__ == "__main__":
    while True:
        # Find the first available chunk to process
        chunk_files = [f for f in os.listdir(INPUT_DIR) if f.endswith(".awkd")]
        if chunk_files:

            # Lock the file by moving it to the processing directory
            chunk_file = chunk_files[0]
            chunk_path = os.path.join(INPUT_DIR, chunk_file)
            processing_path = os.path.join(PROCESSING_DIR, chunk_file)

            shutil.move(chunk_path, processing_path)

            try:
                # Process the chunk
                output_path = os.path.join(OUTPUT_DIR, f"processed-{chunk_file}")
                process_chunk(processing_path, output_path)

                # Mark as completed by deleting or archiving
                os.remove(processing_path)
            except Exception as e:
                print(f"Error processing {chunk_file}: {e}")
                # Move back to input directory for retry
                shutil.move(processing_path, chunk_path)
        else:
            # No chunks available, check if the loader has finished
            if os.path.exists(DONE_FILE):
                print("Loader has completed. No more chunks to process. Exiting.")
                break
            else:
                print("No chunks available. Waiting for new chunks...")
                time.sleep(2)  # Wait before checking again
    
    # After processing all chunks, create a "workers_done" file
   
    workers_done_path = "/data/workers_done"
    with open(workers_done_path, "w") as f:
        f.write("done")
    print(f"Worker signaling completion with {workers_done_path}.")
