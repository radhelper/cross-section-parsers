#!/usr/bin/env python3
import csv
import sys
import pandas as pd
import numpy as np
from datetime import datetime


def read_count_file(in_file_name):
    """
    Read neutron log file
    :param in_file_name: neutron log filename
    :return: list with all neutron lines
    """
    file_lines = list()
    with open(in_file_name, 'r') as in_file:
        for line in in_file:
            # Sanity check, we require a date at the beginning of the line
            line_split = line.rstrip().split()
            if len(line_split) < 7:
                print(f"Ignoring line (malformed):{line}")
                continue
            if "N/A" in line:
                break

            year_date, day_time, sec_frac = line_split[0], line_split[1], line_split[2]
            fission_counter = float(line_split[6])

            # Generate datetime for line
            cur_dt = datetime.strptime(year_date + " " + day_time + sec_frac, "%d/%m/%Y %H:%M:%S.%f")

            file_lines.append((cur_dt, fission_counter))
    file_lines = np.array(file_lines)
    return file_lines


def get_fluency_flux(start_dt, end_dt, file_lines, factor, distance_factor):
    last_fission_counter = None
    last_dt = None
    beam_off_time = 0
    first_curr_integral = None
    first_fission_counter = None

    # It is better to modify line on source than here
    for (cur_dt, fission_counter) in file_lines:
        # Generate datetime for line
        if start_dt <= cur_dt and first_fission_counter is None:
            first_fission_counter = fission_counter
            last_dt = cur_dt
            continue

        if first_fission_counter is not None:
            if fission_counter == last_fission_counter:
                beam_off_time += (cur_dt - last_dt).total_seconds()

            last_fission_counter = fission_counter
            last_dt = cur_dt

        if cur_dt > end_dt:
            interval_total_seconds = float((end_dt - start_dt).total_seconds())
            flux1h = ((last_fission_counter - first_fission_counter) * factor) / interval_total_seconds
            if flux1h < 0:
                print(f"SOMETHING HAPPENED HERE {start_dt} {end_dt}, {flux1h} last fission {last_fission_counter}, "
                      f"{interval_total_seconds} {beam_off_time}")
                return 0, 0

            flux1h *= distance_factor
            return flux1h, beam_off_time
        elif first_curr_integral is not None:
            last_fission_counter = fission_counter
    return 0, 0


def generate_cross_section(row, distance_data, neutron_count):
    start_dt = row["start_dt"]
    machine = row["machine"]
    acc_time_s = row["acc_time"]
    sdc_s = row["#SDC"]
    abort_zero_s = row["#abort"]
    # Slicing the neutron count to not run thought all of it
    neutron_count_cut = neutron_count[neutron_count[:, 0] >= start_dt]

    distance_line = distance_data[(distance_data["board"].str.contains(machine)) &
                                  (distance_data["start"] <= start_dt) &
                                  (start_dt <= distance_data["end"])]
    factor = float(distance_line["factor"])
    distance_factor = float(distance_line["Distance attenuation"])
    end_dt = start_dt + pd.Timedelta(hours=1)

    print(f"Generating cross section for {row['benchmark']}, start {start_dt} end {end_dt}")
    flux, time_beam_off = get_fluency_flux(start_dt=start_dt, end_dt=end_dt, file_lines=neutron_count_cut,
                                           factor=factor, distance_factor=distance_factor)
    fluency = flux * acc_time_s
    cross_section = 0
    cross_section_crash = 0
    if fluency > 0:
        cross_section = sdc_s / fluency
        cross_section_crash = abort_zero_s / fluency

    row["#AccTime"] = acc_time_s
    row["#(Abort==0)"] = abort_zero_s
    row["Flux 1h"] = flux
    row["Fluency(Flux * $AccTime)"] = fluency
    row["Cross Section SDC"] = cross_section
    row["Cross Section Crash"] = cross_section_crash
    row["Time Beam Off (sec)"] = time_beam_off
    return row


def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <neutron counts input file> <csv file> <factor> <distance factor file>")
        exit(1)

    neutron_count_file = sys.argv[1]
    csv_file_name = sys.argv[2]
    distance_factor_file = sys.argv[3]
    print(f"Generating cross section for {csv_file_name}")
    print(f"- {distance_factor_file} for distance")
    print(f"- {neutron_count_file} for neutrons")

    # Load all distances before continue
    distance_data = pd.read_csv(distance_factor_file)
    # Replace the hours and the minutes to the last
    distance_data["start"] = distance_data["start"].apply(
        lambda row: datetime.strptime(row, "%m/%d/%Y").replace(hour=0, minute=0)
    )
    distance_data["end"] = distance_data["end"].apply(
        lambda row: datetime.strptime(row, "%m/%d/%Y").replace(hour=23, minute=59)
    )
    # -----------------------------------------------------------------------------------------------------------------
    # We need to read the neutron count files before calling get_fluency_flux
    neutron_count = read_count_file(neutron_count_file)

    csv_out_file_summary = csv_file_name.replace(".csv", "_cross_section.csv")
    print(f"in: {csv_file_name}")
    print(f"out: {csv_out_file_summary}")
    # -----------------------------------------------------------------------------------------------------------------
    # Read the input csv file
    input_df = pd.read_csv(csv_file_name, delimiter=';').drop("file_path", axis="columns")
    input_df["time"] = pd.to_datetime(input_df["time"])
    input_df = input_df.sort_values("time")
    input_df = input_df.groupby(
        [pd.Grouper(key="time", freq="1h", sort=True, origin="start"), "machine", "benchmark", "header"]).sum()
    input_df = input_df.reset_index().rename(columns={"time": "start_dt"})

    # Apply generate_cross section function
    final_df = input_df.apply(generate_cross_section, axis="columns", args=(distance_data, neutron_count))
    print(final_df)
    final_df.to_csv(csv_out_file_summary)


#########################################################
#                    Main Thread                        #
#########################################################
if __name__ == '__main__':
    main()
