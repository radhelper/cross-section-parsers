#!/usr/bin/env python3
import csv
import sys
import pandas as pd
import numpy as np
from datetime import timedelta, datetime

# smallest run in hour
ONE_HOUR = 60
SMALLEST_RUN = timedelta(minutes=20)


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

            file_lines.append(line_split)
    file_lines = np.array(file_lines)
    return file_lines


def get_fluency_flux(start_dt, end_dt, file_lines, factor, distance_factor):
    last_fission_counter = None
    last_dt = None
    beam_off_time = 0
    first_curr_integral = None
    first_fission_counter = None

    for line in file_lines:
        year_date = line[0]
        day_time = line[1]
        sec_frac = line[2]
        fission_counter = float(line[6])

        # Generate datetime for line
        cur_dt = datetime.strptime(year_date + " " + day_time + sec_frac, "%d/%m/%Y %H:%M:%S.%f")
        if start_dt <= cur_dt and first_fission_counter is None:
            first_fission_counter = fission_counter
            last_dt = cur_dt
            continue

        if first_fission_counter is not None:
            if fission_counter == last_fission_counter:
                beam_off_time += (cur_dt - last_dt).total_seconds()  # add the diff of beam off i and i - 1

            last_fission_counter = fission_counter
            last_dt = cur_dt

        if cur_dt > end_dt:
            interval_total_seconds = float((end_dt - start_dt).total_seconds())
            flux1h = ((last_fission_counter - first_fission_counter) * factor) / interval_total_seconds
            if flux1h < 0:
                raise ValueError(f"SOMETHING HAPPENED HERE {start_dt} {end_dt}")
            flux1h *= distance_factor
            return flux1h, beam_off_time
        elif first_curr_integral is not None:
            last_fission_counter = fission_counter
    return 0, 0


def pre_process_data(full_file_lines):
    """
    I group all the possible configurations in a dictionary
    the key is
    MACHINE + BENCHMARK + LOG HEADER
    :param full_file_lines:
    :return: dictionary that contains the grouped benchmarks
    """
    grouped_benchmarks = {}
    for i in full_file_lines:
        # MACHINE + BENCHMARK + LOG HEADER
        key = i["machine"] + i["benchmark"] + i["header"]
        if key not in grouped_benchmarks:
            grouped_benchmarks[key] = []

        grouped_benchmarks[key].append(i)

    return grouped_benchmarks


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

    # # Load all distances before continue
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
    with open(csv_file_name, "r") as csv_input:
        reader = csv.DictReader(csv_input, delimiter=';')
        full_lines = list(reader)

    # List of dicts that contains all the final data
    final_processed_data = list()

    # grouping the benchmarks
    grouped_benchmarks = pre_process_data(full_file_lines=full_lines)

    for bench in grouped_benchmarks:
        print("Parsing for {}".format(bench))
        lines = grouped_benchmarks[bench]
        i = 0
        while i < len(lines) - 1:
            try:
                start_dt = datetime.strptime(lines[i]["time"].strip(), "%c")
                end_dt = datetime.strptime(lines[i + 1]["time"].strip(), "%c")
            except ValueError as err:
                print(err)
                continue

            machine = lines[i]["machine"]
            bench = lines[i]["benchmark"]
            header_info = lines[i]["header"]
            # acc_time
            acc_time_s = float(lines[i]["acc_time"])
            # #SDC
            sdc_s = int(lines[i]["#SDC"])
            # abort
            abort_zero_s = 0
            if int(lines[i]["#abort"]) == 0 and int(lines[i]["#end"]) == 0:
                abort_zero_s += 1

            first_i = i
            while (end_dt - start_dt) < timedelta(minutes=ONE_HOUR):
                i += 1
                if i >= (len(lines) - 1):  # end of lines
                    break
                if lines[first_i]["benchmark"] != lines[i]["benchmark"] or lines[first_i]["header"] != lines[i][
                    "header"]:
                    print("ISSO ACONTECEU NATURALMENTE")
                    continue

                acc_time_s += float(lines[i]["acc_time"])
                sdc_s += int(lines[i]["#SDC"])
                if int(lines[i]["#abort"]) == 0 and int(lines[i]["#end"]) == 0:
                    abort_zero_s += 1

                end_dt = datetime.strptime(lines[i + 1]["time"].strip(), "%c")

            # why consider such a small run???
            if (end_dt - start_dt) < SMALLEST_RUN:
                i += 1
                continue

            print("Generating cross section for {}, start {} end {}".format(bench.strip(), start_dt, end_dt))

            # compute 1h flux; sum SDC, ACC_TIME, Abort with 0; compute fluency (flux*(sum ACC_TIME))
            distance_line = distance_data[(distance_data["board"].str.contains(machine)) &
                                          (distance_data["start"] <= start_dt) &
                                          (start_dt <= distance_data["end"])]
            factor, distance_factor = distance_line[["factor", "Distance attenuation"]]

            flux, time_beam_off = get_fluency_flux(start_dt=start_dt, end_dt=(start_dt + timedelta(minutes=60)),
                                                   file_lines=neutron_count, factor=factor,
                                                   distance_factor=distance_factor)
            flux_acc_time, time_beam_off_acc_time = get_fluency_flux(start_dt=start_dt,
                                                                     end_dt=(start_dt + timedelta(seconds=acc_time_s)),
                                                                     file_lines=neutron_count, factor=factor,
                                                                     distance_factor=distance_factor)

            fluency = flux * acc_time_s
            fluency_acc_time = flux_acc_time * acc_time_s
            if fluency > 0:
                cross_section = sdc_s / fluency
                cross_section_crash = abort_zero_s / fluency
            else:
                cross_section = 0
                cross_section_crash = 0
            if fluency_acc_time > 0:
                cross_section_acc_time = sdc_s / fluency_acc_time
                cross_section_crash_acc_time = abort_zero_s / fluency_acc_time
            else:
                cross_section_acc_time = 0
                cross_section_crash_acc_time = 0

            # final row for full file, and almost final for summary file
            final_processed_data.append({
                "start timestamp": start_dt.ctime(), "end timestamp": end_dt.ctime(), "Machine": machine,
                "benchmark": bench,
                "header info": header_info, "#lines computed": (i - first_i + 1), "#SDC": sdc_s, "#AccTime": acc_time_s,
                "#(Abort==0)": abort_zero_s, "Flux 1h": flux, "Flux AccTime": flux_acc_time,
                "Fluence(Flux * $AccTime)": fluency, "Fluence AccTime(FluxAccTime * $AccTime)": fluency_acc_time,
                "Cross Section SDC": cross_section, "Cross Section Crash": cross_section_crash,
                "Time Beam Off (sec)": time_beam_off, "Cross Section SDC AccTime": cross_section_acc_time,
                "Cross Section Crash AccTime": cross_section_crash_acc_time,
                "Time Beam Off AccTime (sec)": time_beam_off_acc_time
            })
            i += 1


#########################################################
#                    Main Thread                        #
#########################################################
if __name__ == '__main__':
    main()
