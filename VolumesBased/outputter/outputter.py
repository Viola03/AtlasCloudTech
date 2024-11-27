import os
import awkward as ak
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import AutoMinorLocator
import pandas
import time

# Paths
PROCESSED_DIR = "data/processed"  # Directory containing processed chunks
OUTPUT_PATH = "data/4lep_invariant_mass.png"  # Path to save the output plot

# Luminosity and bin settings (adjust as needed)
lumi = 10  # Integrated luminosity in fb^-1
fraction = 1.0  # Fraction of luminosity used
step_size = 5  # Bin width
xmin, xmax = 80, 250  # Plot x-axis range
bin_edges = np.arange(xmin, xmax + step_size, step_size)  # Bin edges
bin_centres = (bin_edges[:-1] + bin_edges[1:]) / 2  # Bin centers

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

# Helper functions
def load_chunks():
    """
    Load all processed chunks and group them by category.
    """
    grouped_data = {}

    for sample_key, sample_info in samples.items():
        for category in sample_info['list']:
            grouped_data[category] = [] 

    # Iterate over processed files
    for file in os.listdir(PROCESSED_DIR):
        if not file.startswith("processed-") or not file.endswith(".awkd"):
            continue

        # Extract category from file name
        file_path = os.path.join(PROCESSED_DIR, file)
        parts = file.split("-")  # E.g., "processed_data_chunk_0.awkd"
        category = parts[1]  # Second part is the category

        if category in grouped_data:
            grouped_data[category].append(file_path)

    return grouped_data

def combine_chunks(file_paths):
    """
    Combine multiple chunk files into a single awkward array.
    """
    arrays = [ak.from_parquet(file_path) for file_path in file_paths]
    return ak.concatenate(arrays, axis=0) if arrays else ak.Array([])

if __name__ == "__main__":
    
    # Wait for workers to finish
    workers_done_path = "/data/workers_done"
    while not os.path.exists(workers_done_path):
        #print(f"Waiting for workers to complete. Looking for {workers_done_path}...")
        time.sleep(5)  # Check every 5 seconds

    print("Workers have completed. Starting aggregation.")
    
    grouped_data = load_chunks()
    
    # Combine chunks into high-level categories (e.g., 'data', 'Signal', etc.)
    all_data = {}
    for sample_key, sample_info in samples.items():
            # Combine all chunks from the 'list' field
        category_chunks = [combine_chunks(grouped_data[cat]) for cat in sample_info['list']]
        all_data[sample_key] = ak.concatenate(category_chunks, axis=0) if category_chunks else ak.Array([])

        # Ensure categories with empty data are skipped
    for category, data in all_data.items():
        if len(data) == 0:
            print(f"Skipping {category}: No data available.")
            continue

        # Extract MC sample data
    mc_samples = []
    mc_colors = []
    mc_labels = []

    for key in samples.keys():
        if key not in ['data', r'Signal ($m_H$ = 125 GeV)']:
            if len(all_data[key]) > 0:  # Ensure the category is not empty
                mc_samples.append(all_data[key])
                mc_colors.append(samples[key]['color'])
                mc_labels.append(key)
            else:
                print(f"Skipping {key}: No data available.")
        
    data_x,_ = np.histogram(ak.to_numpy(all_data['data']['mass']), 
                        bins=bin_edges ) # histogram the data
    data_x_errors = np.sqrt( data_x ) # statistical error on the data

    signal_x = ak.to_numpy(all_data[r'Signal ($m_H$ = 125 GeV)']['mass']) # histogram the signal
    signal_weights = ak.to_numpy(all_data[r'Signal ($m_H$ = 125 GeV)'].totalWeight) # get the weights of the signal events
    signal_color = samples[r'Signal ($m_H$ = 125 GeV)']['color'] # get the colour for the signal bar

    mc_x = [] # define list to hold the Monte Carlo histogram entries
    mc_weights = [] # define list to hold the Monte Carlo weights
    mc_colors = [] # define list to hold the colors of the Monte Carlo bars
    mc_labels = [] # define list to hold the legend labels of the Monte Carlo bars

    for s in samples: # loop over samples
        if s not in ['data', r'Signal ($m_H$ = 125 GeV)']: # if not data nor signal
            mc_x.append( ak.to_numpy(all_data[s]['mass']) ) # append to the list of Monte Carlo histogram entries
            mc_weights.append( ak.to_numpy(all_data[s].totalWeight) ) # append to the list of Monte Carlo weights
            mc_colors.append( samples[s]['color'] ) # append to the list of Monte Carlo bar colors
            mc_labels.append( s ) # append to the list of Monte Carlo legend labels

    # *************
    # Main plot 
    # *************
    main_axes = plt.gca() # get current axes

    # plot the data points
    main_axes.errorbar(x=bin_centres, y=data_x, yerr=data_x_errors,
                        fmt='ko', # 'k' means black and 'o' is for circles 
                        label='Data') 

    # plot the Monte Carlo bars
    mc_heights = main_axes.hist(mc_x, bins=bin_edges, 
                                weights=mc_weights, stacked=True, 
                                color=mc_colors, label=mc_labels )

    mc_x_tot = mc_heights[0][-1] # stacked background MC y-axis value

    # calculate MC statistical uncertainty: sqrt(sum w^2)
    mc_x_err = np.sqrt(np.histogram(np.hstack(mc_x), bins=bin_edges, weights=np.hstack(mc_weights)**2)[0])

    # plot the signal bar
    signal_heights = main_axes.hist(signal_x, bins=bin_edges, bottom=mc_x_tot, 
                    weights=signal_weights, color=signal_color,
                    label=r'Signal ($m_H$ = 125 GeV)')

    # plot the statistical uncertainty
    main_axes.bar(bin_centres, # x
                    2*mc_x_err, # heights
                    alpha=0.5, # half transparency
                    bottom=mc_x_tot-mc_x_err, color='none', 
                    hatch="////", width=step_size, label='Stat. Unc.' )

    # set the x-limit of the main axes
    main_axes.set_xlim( left=xmin, right=xmax ) 

    # separation of x axis minor ticks
    main_axes.xaxis.set_minor_locator( AutoMinorLocator() ) 

    # set the axis tick parameters for the main axes
    main_axes.tick_params(which='both', # ticks on both x and y axes
                            direction='in', # Put ticks inside and outside the axes
                            top=True, # draw ticks on the top axis
                            right=True ) # draw ticks on right axis

    # x-axis label
    main_axes.set_xlabel(r'4-lepton invariant mass $\mathrm{m_{4l}}$ [GeV]',
                        fontsize=13, x=1, horizontalalignment='right' )

    # write y-axis label for main axes
    main_axes.set_ylabel('Events / '+str(step_size)+' GeV',
                            y=1, horizontalalignment='right') 

    # set y-axis limits for main axes
    main_axes.set_ylim( bottom=0, top=np.amax(data_x)*1.6 )

    # add minor ticks on y-axis for main axes
    main_axes.yaxis.set_minor_locator( AutoMinorLocator() ) 

    # Add text 'ATLAS Open Data' on plot
    plt.text(0.05, # x
                0.93, # y
                'ATLAS Open Data', # text
                transform=main_axes.transAxes, # coordinate system used is that of main_axes
                fontsize=13 ) 

    # Add text 'for education' on plot
    plt.text(0.05, # x
                0.88, # y
                'for education', # text
                transform=main_axes.transAxes, # coordinate system used is that of main_axes
                style='italic',
                fontsize=8 ) 

    # Add energy and luminosity
    lumi_used = str(lumi*fraction) # luminosity to write on the plot
    plt.text(0.05, # x
                0.82, # y
                r'$\sqrt{s}$=13 TeV,$\int$L dt = '+lumi_used+' fb$^{-1}$', # text
                transform=main_axes.transAxes ) # coordinate system used is that of main_axes

    # Add a label for the analysis carried out
    plt.text(0.05, # x
                0.76, # y
                r'$H \rightarrow ZZ^* \rightarrow 4\ell$', # text 
                transform=main_axes.transAxes ) # coordinate system used is that of main_axes

    # draw the legend
    main_axes.legend( frameon=False ) # no box around the legend

    plt.savefig(OUTPUT_PATH, format='png')