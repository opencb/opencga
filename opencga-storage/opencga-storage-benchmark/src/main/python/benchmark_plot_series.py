#!python
#
# Generate plots to visualise response times and codes from a set of JTL files. Separate plots are generated
# for fixed and random runs. Each plot shows:
# - boxplot of response times in ms per query per run
# - barchart of response code percentages per query per run
#
# Assumptions:
# - The first input file is the baseline run, which is used to determine if the other runs are faster or slower
# - the input file names include "FIXED" or "RANDOM"
# - the input files include the same set of fixed and random queries
# - the input file paths include number of repeats and concurrency e.g. "5x10"
#
# Usage:
#
# python3 plotSeries.py jtlfile1 jtlfile2 ...
#
# Example using the output of /scripts/runSeries.py
#
# python3 scripts/plotSeries.py results/results/benchmark-series-20250312202044-prod/run*/opencga_benchmark*/*.jtl
#
import sys
import csv
import os
import re
import glob
import matplotlib.pyplot as plot
from matplotlib.ticker import PercentFormatter

BOX_BASELINE = "skyblue"
BOX_DEGRADE = "lightcoral"
BOX_IMPROVE = "lightgreen"

MEDIAN_COLOR = 'darkred'


def read_response_times(file_path):
    values = {} # { query -> [ time1, time2, ] }
    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)
        # Read header
        row = next(csv_reader)
        # Identify the columns for elapsed time and query
        # timeStamp,elapsed,label,responseCode,responseMessage,threadName,dataType,success,failureMessage,bytes,sentBytes,grpThreads,allThreads,URL,Latency,IdleTime,Connect
        try:
            elapsed_idx = row.index('elapsed')
            query_idx = row.index('label')
        except ValueError:
            raise Exception(f"Error: Could not find 'elapsed' or 'label' columns in {file_path}")

        for row in csv_reader:

            if valid_row(row):
                elapsed = row[elapsed_idx]
                query = row[query_idx]
                try:
                    query_times = values.setdefault(query, [])
                    query_times.append(float(elapsed))
                except ValueError:
                    pass  # Ignore non-numeric values
    return values

def read_response_codes(file_path):
    code_query_counts = {} # { code -> { query -> count }}
    query_totals = {} # { query -> count}
    print(f"Reading response codes from {file_path}")
    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)
        # Read header
        row = next(csv_reader)
        # Identify the columns for response code and query
        # timeStamp,elapsed,label,responseCode,responseMessage,threadName,dataType,success,failureMessage,bytes,sentBytes,grpThreads,allThreads,URL,Latency,IdleTime,Connect
        try:
            query_idx = row.index('label')
            response_code_idx = row.index('responseCode')
            response_message_idx = row.index('responseMessage')
        except ValueError:
            raise Exception(f"Error: Could not find 'responseCode' or 'label' columns in {file_path}")


        for row in csv_reader:
            if valid_row(row):
                query = row[query_idx]
                response_code = row[response_code_idx] + ': ' + row[response_message_idx]
                query_counts = code_query_counts.setdefault(response_code, {})
                query_counts.setdefault(query, 0)
                query_counts[query] += 1
                query_totals.setdefault(query, 0)
                query_totals[query] += 1

    # calculate percentages
    for query_counts in code_query_counts.values():
        for query, count in query_counts.items():
            query_counts[query] = count * 100 / query_totals.get(query)
    return code_query_counts

def valid_row(row):
    # Discard rows starting with `#`
    if row[0].startswith('#'):
        return False
    elif len(row) < 5:
        return False
    else:
        return True

def get_run_size_from_folder_name(file_path):
    pattern = r'\_(\d+)x(\d+)'
    match = re.search(pattern, file_path)
    if match:
        return (int(match.group(1)), int(match.group(2)))
    else:
        return (0, 0)

def get_plot_title(file_paths):
    # Extract common dir
    common_dir_path_parts = os.path.commonpath(file_paths).split(os.sep)

    # Extract the series name from the common directory
    series_name = common_dir_path_parts[-1]
    if (series_name.endswith('.jtl')):
        series_name = common_dir_path_parts[-2]

    # Extract the run mode (RANDOM or FIXED) from the last part of the path
    # Assuming all files in the series have the same run mode
    run_name = file_paths[0].split(os.sep)[-1]  # Get the last part of the path
    mode_match = re.search(r'.(RANDOM|FIXED)', run_name)
    return series_name + " (" + mode_match.group(1) + ")"


def create_response_times_box_plot(sorted_files):
    all_data = []
    x_labels = []

    fig = plot.figure(figsize=(12, 8))
    margin = 0.06
    axes_separation = 0.01
    ax2_height = 0.05
    ax1 = fig.add_axes([margin, margin + ax2_height + axes_separation, 1 - margin * 2, 1 - margin * 2 - ax2_height])
    ax1.set_ylabel('Response time (ms)')
    ax1.grid(axis='y', linestyle='--', alpha=0.7)

    ax2 = fig.add_axes([margin, margin, 1 - margin * 2, ax2_height], sharex=ax1)

    sorted_queries = None
    num_runs=len(sorted_files)
    all_xs = []
    max_response_time = 2000
    common_dir_path = os.path.commonpath(sorted_files)
    first_bplot = None
    for i, file_path in enumerate(sorted_files):
        data = read_response_times(file_path) # { query -> [time1, time2, ...] }
        if data:
            # Get the series name from the directory name under the common path
            relative_path = os.path.relpath(file_path, common_dir_path)
            serie_name = os.path.dirname(relative_path).split(os.sep)[0]
            (concurrency, repeats) = get_run_size_from_folder_name(file_path)
            dataset_name = f"{concurrency} threads, {repeats} repeats\n{serie_name}"
            all_data.append(data)
            x_labels.append(dataset_name)

            num_queries = len(data)
            width = 0.9 / num_queries
            widths = [width] * num_queries
            xs = [ -0.45 + i + x * width for x in range(num_queries) ]

            if sorted_queries == None:
                sorted_queries = sorted(data, key=lambda k: sum(data[k]))

            max_response_time = max(max_response_time, max(max(data[query]) for query in sorted_queries))

            sorted_data = [data.get(query, []) for query in sorted_queries]

            bplot = ax1.boxplot(sorted_data, positions=xs, widths=widths, patch_artist=True, showfliers=True, showmeans=True,
                                boxprops={ "facecolor": BOX_BASELINE},
                                medianprops={ "color": MEDIAN_COLOR , "linewidth": 1 },
                                )

            if i == 0:
                first_bplot = bplot  # Store the first boxplot for later use
            else:
                for idx in range(len(data)):
                    # Get the average response time for the first run
                    first_run_median = first_bplot['medians'][idx].get_ydata()[0]
                    this_run_median = bplot['medians'][idx].get_ydata()[0]
                    # Compare the median of the first run with the current run. 3 scenarios:
                    # 1. Runs have less than 1% difference, set color to BOX_BASELINE
                    # 2. First run is faster than this run, set color to BOX_DEGRADE
                    # 3. First run is slower than this run, set color to BOX_IMPROVE

                    if abs(first_run_median - this_run_median) < 0.01 * first_run_median:
                        # Set color to BOX_BASELINE if the runs are similar
                        bplot['boxes'][idx].set_facecolor(BOX_BASELINE)
                    elif first_run_median < this_run_median:
                        # Set color to BOX_DEGRADE if this run is slower than the first run
                        bplot['boxes'][idx].set_facecolor(BOX_DEGRADE)
                    else:
                        # Set color to BOX_IMPROVE if this run is faster than the first run
                        bplot['boxes'][idx].set_facecolor(BOX_IMPROVE)

                    # if this_run_median > 5000:
                    #     # If the response time is greater than 5 seconds, add a checkerboard pattern
                    #     bplot['boxes'][idx].set_hatch('//')

            # Add label to each boxplot
            for j, query in enumerate(sorted_queries):
                ax1.text(xs[j], 0, j+1, rotation=0, ha='center', va='bottom', fontsize=8)

            all_xs += xs # concat all xs for response code bars

    ## Add grid step. Round to the closest 500ms
    gridStep = round(max(1, max_response_time/20/500))*500
    ax1.yaxis.set_major_locator(plot.MultipleLocator(gridStep))

    ax1.set_xticks([])
    ax1.set_title(get_plot_title(sorted_files))

    ax1.set_ylim(0, max_response_time + 100)

    # add custom legend entry for the median and mean
    ax1.plot([], [], marker="_", color=MEDIAN_COLOR, label='Median')
    ax1.plot([], [], marker='^', linestyle='None', color='green', label='Mean')
    ax1.plot([], [], marker='s', linestyle='None', color=BOX_BASELINE, label='Baseline')
    ax1.plot([], [], marker='s', linestyle='None', color=BOX_DEGRADE, label='Degraded')
    ax1.plot([], [], marker='s', linestyle='None', color=BOX_IMPROVE, label='Improved')
    ax1.legend()

    all_counts = {} # { code -> [ count1, count2 ]}
    all_run_counts = []
    for file_path in sorted_files:
        counts = read_response_codes(file_path) # { code -> { query -> count } }
        all_counts.update(dict.fromkeys(counts.keys(), []))
        all_run_counts.append(counts)

    # concatenate code counts for all queries for all runs
    for run_counts in all_run_counts:
        for code in all_counts:
            query_code_counts = run_counts.get(code, {})
            all_counts[code] = all_counts[code] + [query_code_counts.get(query, 0) for query in sorted_queries]


    # plot response code percentages for each query for each run
    colors = { '200': 'darkgreen', '500': 'red' }
    bottom = [0] * num_runs * num_queries
    width = 0.9 / num_queries
    for code, counts in all_counts.items():
        ax2.bar(all_xs, counts, width, label=code, bottom=bottom, edgecolor='white', color=colors.get(code[:3]))
        bottom = [ a + b for a, b in zip(bottom, counts)]

    ax2.set_xticks(range(num_runs), labels=x_labels)
    ax2.yaxis.set_major_formatter(PercentFormatter(100))
    ax2.legend()

    # Add query list
    queriesText = "Queries: "
    for i, query in enumerate(sorted_queries):
        queriesText += f"\n{i + 1}: {query}"

    ## Add background rectangle for the query text
    ax1.text(0.01, 0.98, queriesText, transform=ax1.transAxes, bbox=dict(facecolor='white', alpha=0.8, edgecolor='black'), horizontalalignment='left', verticalalignment='top')
    # ax1.text(0.02 , 0.98, queriesText, transform=ax1.transAxes, horizontalalignment='left', verticalalignment='top', fontsize=8)
    # ax1.text(0.02 , 0.98, "Queries: ", transform=ax1.transAxes)


def main(arg):
    all_files = []
    for file in arg:
        if not os.path.isdir(file):
            if file.endswith('.jtl'):
                all_files.append(file)
            else:
                print(f"Skipping non-JTL file: {file}")
        else:
            all_files += glob.glob(f"{file}/**/*.jtl", recursive=True)

    # Sort run folders by increasing concurrency and length
    sorted_files = sorted(all_files, key=lambda x: get_run_size_from_folder_name(x))

    print("Files included:")
    for file_path in sorted_files:
        print("  " + file_path)

    create_response_times_box_plot(sorted_files)

    dir = os.path.commonpath(all_files);
    if os.path.isfile(dir):
        dir = os.path.dirname(dir)
    plot.savefig(f'{dir}/boxplot.png')
    plot.show()


if __name__ == "__main__":
    if (len(sys.argv) <= 1):
        fileName = sys.argv[0].split(os.sep)[-1]
        print(f"Usage: python3 {fileName} <baseline> <test1> <test2> ...")

        sys.exit(1)

    main(sys.argv[1:])