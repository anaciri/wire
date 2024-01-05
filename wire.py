from typing import Dict
import statistics
#from scipy.stats import skew
import csv
import time
import numpy as np
import os

# this one is for debug only
TIMESTAMP_CUTOFF = 1704148887160

# Read environment variables from .env file
TICK_DATA_POINTS_SZ = int(os.environ['TICK_DATA_POINTS_SZ'])
LOOP_TIME_INTERVAL_SEC = int(os.environ['LOOP_TIME_INTERVAL_SEC'])
LOG_FILE_PATH = os.environ['LOG_FILE_PATH']


def _read_last_segment(filename: str, linesToRead: int = 80) -> Dict[str, Dict[int, int]]:
    tick_data = {}

    with open(filename, 'r') as file:
        # Move the file cursor to the end
        file.seek(0, 2)
        file_size = file.tell()

        # Read the file line by line from the end
        lines = []
        line_count = 0

        for i in range(file_size - 1, 0, -1):
            file.seek(i)
            char = file.read(1)
            if char == '\n':
                lines.append(file.readline().strip())
                line_count += 1
                if line_count == linesToRead:
                    break

        # Process the lines. OJO: tick log timestamps are in msecs
        for row in reversed(lines):
            components = row.split(',')
            if len(components) == 3:
                ticker, timestamp, tick = components
                timestamp, tick = int(timestamp), int(tick)

                if ticker not in tick_data:
                    tick_data[ticker] = {}

                tick_data[ticker][timestamp] = tick

    return tick_data

# create tick data dict from tick log file. dict key: tkr, valu (timestamp, ticks)
def createTickDict(filename: str) -> Dict[str, Dict[int, int]]:
    # we only care about the last 
    tick_data = _read_last_segment(filename)

    # Truncate each entry to the most recent TICK_DATA_POINTS_SZ
    for ticker, timestamp_dict in tick_data.items():
        sorted_timestamps = sorted(timestamp_dict.keys(), reverse=True)
        truncated_timestamps = sorted_timestamps[:TICK_DATA_POINTS_SZ]
        tick_data[ticker] = {timestamp: timestamp_dict[timestamp] for timestamp in truncated_timestamps}

    return tick_data

def getTickDiff(tick_data: Dict[str, Dict[int, int]]) -> Dict[str, Dict[int, int]]:
    tick_diff = {}

    for ticker, timestamp_dict in tick_data.items():
        tick_diff[ticker] = {}
        previous_tick = None

        for timestamp, tick in sorted(timestamp_dict.items()):
            if previous_tick is not None:
                tick_diff[ticker][timestamp] = tick - previous_tick
            previous_tick = tick
    return tick_diff



def tickStats(tick_data):
    stats = {}

    for ticker, tick_values in tick_data.items():
        values = list(tick_values.values())

        if values:
            mean_val = np.mean(values)
            median_val = np.median(values)
            ptile5_val = np.percentile(values, 5)
            ptile95_val = np.percentile(values, 95)

            stats[ticker] = {
                'mean': mean_val,
                'median': median_val,
                '5ptile': ptile5_val,
                '95ptile': ptile95_val
            }

    return stats



# not sure how good this heuristic, mean change from last cycle. taking advantage to mean
# sensibility to rare but large deviations. Input is stats from statsImpact
# https://en.wikipedia.org/wiki/Skewness#:~:text=In%20probability%20theory%20and%20statistics,zero%2C%20negative%2C%20or%20undefined.
# also: https://www.tandfonline.com/doi/abs/10.1080/10691898.2010.11889493 

def statsImpact(stats):
    impact_dict = {}
    # logs are in milisecs so need to normalize to ms
    now = int(time.time()*1000)
    lastcycle = now - LOOP_TIME_INTERVAL_SEC*1000

    for ticker, stat_values in stats.items():
        values = list(stat_values.values())

        if values:
            mean_all = np.mean(values)
            # exclude newest values to measure impact
            filtered_values = [value for timestamp, value in stat_values.items() if timestamp <= lastcycle]
            mean_filtered = np.mean(filtered_values)
            delta = 1 + (mean_all - mean_filtered) / abs(mean_filtered)

            impact_dict[ticker] = {
                'mean': mean_all,
                'oldmean': mean_filtered,
                'mean_delta': delta
            }

    return impact_dict

def printStats(tick_diff, stats_filename='stats.csv'):
    with open(stats_filename, 'a', newline='') as csvfile:
        fieldnames = ['ticker', 'timestamp', 'mean', 'median', 'max', 'min', 'mean_delta']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

        # Write header if the file is empty
        if csvfile.tell() == 0:
            writer.writeheader()

        # get the stats impact needed for my skew heuristics
        stats = statsImpact(tick_diff)
        for ticker, timestamp_diff_dict in tick_diff.items():
            timestamps = list(timestamp_diff_dict.keys())
            if timestamps:
                latest_timestamp = max(timestamps)
                differences = list(timestamp_diff_dict.values())

                mean_diff = statistics.mean(differences)
                median_diff = statistics.median(differences)
                max_diff = max(differences)
                min_diff = min(differences)
                
                # Calculate heuristic skew
                myskew = stats.get(ticker, {}).get('mean_delta', None)

                row = {
                    'ticker': ticker,
                    'timestamp': latest_timestamp,
                    'mean': "{:.4f}".format(mean_diff),
                    'median': "{:.4f}".format(median_diff),
                    'max': max_diff,
                    'min': min_diff,
                    'mean_delta': "{:.4f}".format(myskew) if myskew is not None else 'N/A'
                }
                writer.writerow(row)


def main( stats_filename: str = 'stats.csv'):
    while True:
        try:
            # deserialize tick data from file
            mkt_tick_data = createTickDict(LOG_FILE_PATH)
            # transform into tick changes
            tick_diff = getTickDiff(mkt_tick_data)
            # get stats impact e.g meanDelta from last set of ticks
            stats = statsImpact(tick_diff)

            # append statistics
            printStats(tick_diff, stats_filename)
            # Wait for the next iteration
            time.sleep(LOOP_TIME_INTERVAL_SEC)

        except KeyboardInterrupt:
            # Allow the user to interrupt the loop with Ctrl+C
            print("Loop interrupted by user.")
            break

# Example usage:

main()
