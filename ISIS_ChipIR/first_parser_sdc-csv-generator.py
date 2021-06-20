#!/usr/bin/env python3

import csv
import re
from datetime import datetime
import os
import glob


def main():
    tmp_dir = "/tmp/parserSDC"
    if not os.path.isdir(tmp_dir):
        os.mkdir(tmp_dir)
    else:
        os.system(f"rm -r -f {tmp_dir}/*")

    all_tar = [y for x in os.walk(".") for y in glob.glob(os.path.join(x[0], '*.tar.gz'))]
    for tar in all_tar:
        try:
            if os.system(f"tar -xzf {tar} -C {tmp_dir}/") != 0:
                raise ValueError
        except ValueError:
            os.system(f"gzip -d {tar} -C {tmp_dir}/temp_file.tar")
            os.system(f"tar -xf {tmp_dir} /temp_file.tar -C {tmp_dir}")

    # Copy some other logs that are not compacted
    all_logs_list = [y for x in os.walk(".") for y in glob.glob(os.path.join(x[0], '*.log'))]
    for log in all_logs_list:
        os.system(f"cp {log} {tmp_dir}/")

    all_logs_tmp = [y for x in os.walk(tmp_dir) for y in glob.glob(os.path.join(x[0], '*.log'))]
    for log in all_logs_tmp:
        os.system(f"mv {log} {tmp_dir}/")

    machine_dict = dict()

    header_csv = ["time", "machine", "benchmark", "header", "#SDC", "#abort",   "#end","acc_err", "acc_time",
                  "file_path", "is_it_problematic"]

    total_sdc = 0

    all_logs = [y for x in os.walk(tmp_dir) for y in glob.glob(os.path.join(x[0], '*.log'))]

    all_logs.sort()

    folder_p = "logs_parsed"

    if not os.path.isdir(folder_p):
        os.mkdir(folder_p)

    for fi in all_logs:

        m = re.match(r'.*/(\d+)_(\d+)_(\d+)_(\d+)_(\d+)_(\d+)_(.*)_(.*).log', fi)
        if m:
            year = int(m.group(1))
            month = int(m.group(2))
            day = int(m.group(3))
            hour = int(m.group(4))
            minute = int(m.group(5))
            sec = int(m.group(6))
            benchmark = m.group(7)
            machine_name = m.group(8)

            start_dt = datetime(year, month, day, hour, minute, sec)
            sdc, end, abort, acc_time, acc_err = [0] * 5
            header = "unknown"
            with open(fi, "r") as lines:
                # Only the cannon markers of LogHelper must be added here
                for line in lines:
                    m = re.match(r".*HEADER(.*)", line)
                    if m:
                        header = m.group(1)
                        header.replace(";", "-")

                    m = re.match(".*SDC.*", line)
                    if m:
                        sdc += 1
                        total_sdc += 1

                    m = re.match(r".*AccTime:(\d+.\d+)", line)
                    if m:
                        acc_time = float(m.group(1))

                    m = re.match(r".*AccErr:(\d+)", line)
                    if m:
                        acc_err = int(m.group(1))

                    # TODO: Add on the log helper a way to write framework errors
                    m = re.match(".*ABORT.*", line)
                    if m:
                        abort = 1

                    m = re.match(".*END.*", line)
                    if m:
                        end = 1

            with open(f'./{folder_p}/logs_parsed_{machine_name}.csv', 'a') as fp:
                good_fp = csv.writer(fp, delimiter=';')

                if machine_name not in machine_dict:
                    machine_dict[machine_name] = 1
                    good_fp.writerow(header_csv)
                    print(f"Machine first time: {machine_name}")
                is_problematic = header == "unknown"
                good_fp.writerow(
                    [start_dt.ctime(), machine_name, benchmark, header, sdc, acc_err, acc_time, abort, end, fi,
                     is_problematic]
                )

    print(f"\n\t\tTOTAL_SDC: {total_sdc}")


if __name__ == '__main__':
    main()
