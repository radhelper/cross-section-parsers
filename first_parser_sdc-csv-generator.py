#!/usr/bin/env python3
import csv
import glob
import os
import re
from datetime import datetime


def main():
    tmp_dir = "/tmp"
    folder_p = "logs_parsed"

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
        os.system(f"mv {log} {tmp_dir}/ 2>/dev/null")

    machine_dict = dict()

    total_sdc = 0

    all_logs = [y for x in os.walk(tmp_dir) for y in glob.glob(os.path.join(x[0], '*.log'))]

    all_logs.sort()

    if not os.path.isdir(folder_p):
        os.mkdir(folder_p)

    for fi in all_logs:
        log_m = re.match(r'.*/(\d+)_(\d+)_(\d+)_(\d+)_(\d+)_(\d+)_(.*)_(.*).log', fi)
        if log_m:
            year = int(log_m.group(1))
            month = int(log_m.group(2))
            day = int(log_m.group(3))
            hour = int(log_m.group(4))
            minute = int(log_m.group(5))
            sec = int(log_m.group(6))
            benchmark = log_m.group(7)
            machine_name = log_m.group(8)

            start_dt = datetime(year, month, day, hour, minute, sec).ctime()
            sdc, end, abort, app_crash, sys_crash, acc_time, acc_err = [0] * 7
            header = "unknown"

            with open(fi, "r") as lines:
                # Only the cannon markers of LogHelper must be added here
                for line in lines:
                    header_m = re.match(r".*HEADER(.*)", line)
                    if header_m:
                        header = header_m.group(1).replace(";", "-")

                    if re.match(".*SDC.*", line):
                        sdc += 1
                        total_sdc += 1

                    acc_time_m = re.match(r".*AccTime:(\d+.\d+)", line)
                    if acc_time_m:
                        acc_time = float(acc_time_m.group(1))

                    acc_m = re.match(r".*AccErr:(\d+)", line)
                    if acc_m:
                        acc_err = int(acc_m.group(1))

                    # TODO: Add on the log helper a way to write framework errors
                    if re.match(".*ABORT.*", line):
                        abort += 1
                    if re.match(".*soft APP reboot.", line):
                        app_crash += 1
                    if re.match(".*power cycle", line):
                        sys_crash += 1
                    if re.match(".*END.*", line):
                        end = 1
            new_line_dict = {
                "time": start_dt, "machine": machine_name, "benchmark": benchmark, "header": header, "#SDC": sdc,
                "#appcrash": app_crash, "#abort": abort, "#syscrash": sys_crash, "#end": end, "acc_err": acc_err,
                "acc_time": acc_time, "file_path": fi
            }
            header_csv = list(new_line_dict.keys())
            with open(f'./{folder_p}/logs_parsed_{machine_name}.csv', 'a') as fp:
                csv_writer = csv.DictWriter(fp, fieldnames=header_csv, delimiter=';')
                if machine_name not in machine_dict:
                    machine_dict[machine_name] = 1
                    csv_writer.writeheader()
                    print(f"Machine first time: {machine_name}")
                csv_writer.writerow(new_line_dict)

    print(f"\n\t\tTOTAL_SDC: {total_sdc}")


if __name__ == '__main__':
    main()
