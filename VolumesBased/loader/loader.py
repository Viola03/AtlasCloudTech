import infofile
import uproot
import awkward as ak
import os

# Define the path to the data and output directory
path = "https://atlas-opendata.web.cern.ch/atlas-opendata/samples/2020/4lep/"
output_path = "data/chunks"  # Mounted shared volume in Docker Compose

os.makedirs(output_path, exist_ok=True)

# Chunk size
CHUNK_SIZE = 100000

# Define the ROOT files to load
samples = {

    'data': {
        'list' : ['data_A','data_B','data_C','data_D'], # data is from 2016, first four periods of data taking (ABCD)
    },

    r'Background $Z,t\bar{t}$' : { # Z + ttbar
        'list' : ['Zee','Zmumu','ttbar_lep'],
        'color' : "#6b59d3" # purple
    },

    r'Background $ZZ^*$' : { # ZZ
        'list' : ['llll'],
        'color' : "#ff0000" # red
    },

    r'Signal ($m_H$ = 125 GeV)' : { # H -> ZZ -> llll
        'list' : ['ggH125_ZZ4lep','VBFH125_ZZ4lep','WH125_ZZ4lep','ZH125_ZZ4lep'],
        'color' : "#00cdff" # light blue
    },

}

variables = ['lep_pt','lep_eta','lep_phi','lep_E','lep_charge','lep_type']
weight_variables = ["mcWeight", "scaleFactor_PILEUP", "scaleFactor_ELE", "scaleFactor_MUON", "scaleFactor_LepTRIGGER"]


def load_and_split_data(sample):
    """
    Load ROOT files, split into chunks, and save each chunk.
    """
    
    # Print which sample is being processed
    print('Processing '+sample+' samples') 

    # Define empty list to hold data
    frames = [] 

    # Loop over each file
    for i,val in enumerate(samples[s]['list']): 
        if s == 'data': 
            prefix = "Data/" # Data prefix
        else: # MC prefix
            prefix = "MC/mc_"+str(infofile.infos[val]["DSID"])+"."
        fileString = path+prefix+val+".4lep.root" # file name to open


        # Open file
        file = uproot.open(fileString) 
        tree = file["mini"]
        
        sample_data = []
        
        for idx, data in enumerate(tree.iterate(variables + weight_variables, 
                                 library="ak", 
                                 step_size = CHUNK_SIZE)): 
            
            # Number of events in this batch
            nIn = len(data) 
            #print(nIn)
            
            chunk_file = os.path.join(output_path, f"{val}_{idx}.awkd")
            ak.to_parquet(data, chunk_file)  
            print(f"Saved chunk {idx} to {chunk_file}")

        # for idx, chunk in enumerate(tree.iterate(step_size=CHUNK_SIZE, library="ak")):
        #     chunk_file = os.path.join(output_path, f"chunk_{idx}.awkd")
        #     ak.to_parquet(chunk, chunk_file)  # Save chunk in a lightweight format
        #     print(f"Saved chunk {idx} to {chunk_file}")

if __name__ == "__main__":
    # Process each sample
    for s in samples:
        load_and_split_data(s)
    print("Data loading and chunking complete.")

