#!/usr/bin/env python3

import csv
import re
from datetime import timedelta, datetime
import os
import glob


def main():
    tmp_dir = "/tmp/parserSDC/"
    if not os.path.isdir(tmp_dir):
        os.mkdir(tmp_dir)
    else:
        os.system("rm -r -f " + tmp_dir + "*")

    all_tar = [y for x in os.walk(".") for y in glob.glob(os.path.join(x[0], '*.tar.gz'))]
    for tar in all_tar:
        try:
            if os.system("tar -xzf " + tar + " -C " + tmp_dir) != 0:
                raise ValueError
        except ValueError:
            os.system("gzip -d " + tar + " -C " + tmp_dir + "/temp_file.tar")
            os.system("tar -xf " + tmp_dir + "/temp_file.tar" + " -C " + tmp_dir)

    all_logs_list = [y for x in os.walk(".") for y in glob.glob(os.path.join(x[0], '*.log'))]
    for logs in all_logs_list:
        os.system("cp " + logs + " " + tmp_dir)

    all_logs_tmp = [y for x in os.walk(tmp_dir) for y in glob.glob(os.path.join(x[0], '*.log'))]
    for logs in all_logs_tmp:
        os.system("mv " + logs + " " + tmp_dir)

    machine_dict = dict()

    header_csv = "Time;Machine;Benchmark;Header Info;#SDC;acc_err;acc_time;abort;end;framework_error;filename and dir"

    total_sdc = 0

    all_logs = [y for x in os.walk(tmp_dir) for y in glob.glob(os.path.join(x[0], '*.log'))]

    all_logs.sort()

    folder_p = "logs_parsed"

    good_csv_files = list()

    if not os.path.isdir(folder_p):
        os.mkdir(folder_p)

    for fi in all_logs:

        m = re.match(".*/(\d+)_(\d+)_(\d+)_(\d+)_(\d+)_(\d+)_(.*)_(.*).log", fi)
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

            lines = open(fi, "r")
            sdc = 0
            end = 0
            abort = 0
            acc_time = 0
            acc_err = 0
            cuda_framework_abort = 0
            header = "unknown"
            for line in lines:
                m = re.match(r".*HEADER(.*)", line)
                if m:
                    header = m.group(1)
                    header.replace(";", "-")

                m = re.match(".*SDC.*", line)
                if m:
                    sdc += 1
                    total_sdc += 1

                m = re.match(".*AccTime:(\d+.\d+)", line)
                if m:
                    acc_time = float(m.group(1))

                m = re.match(".*AccErr:(\d+)", line)
                if m:
                    acc_err = int(m.group(1))

                m = re.match(".*ABORT.*", line)
                if m:
                    abort = 1

                m = re.match(".*CUDA Framework error.*", line)
                if m:
                    cuda_framework_abort = 1

                m = re.match(".*END.*", line)
                if m:
                    end = 1

            good_file = './' + folder_p + '/logs_parsed_' + machine_name + '.csv'
            if good_file not in good_csv_files:
                good_csv_files.append(good_file)

            with open('./' + folder_p + '/logs_parsed_' + machine_name + '.csv', 'a') as fp, open(
                    './' + folder_p + '/logs_parsed_problematics_' + machine_name + '.csv', 'a') as fp_problem:
                good_fp = csv.writer(fp, delimiter=';')  # , quotechar='|', quoting=csv.QUOTE_MINIMAL)
                problem_fp = csv.writer(fp_problem, delimiter=';')  # , quotechar='|', quoting=csv.QUOTE_MINIMAL)

                if machine_name not in machine_dict:
                    machine_dict[machine_name] = 1
                    good_fp.writerow(header_csv.split(";"))
                    problem_fp.writerow(header_csv.split(";"))

                    print("Machine first time: " + machine_name)

                if not re.match(".*sm.*", benchmark):
                    row = [start_dt.ctime(), machine_name, benchmark, header, sdc,
                           acc_err, acc_time, abort, end, cuda_framework_abort, fi]
                    if header == "unknown" or acc_time == 0:
                        problem_fp.writerow(row)
                    else:
                        good_fp.writerow(row)

    print("\n\t\tTOTAL_SDC: ", total_sdc, "\n")

    summaries_file = "./" + folder_p + "/summaries.csv"
    os.system("rm -f " + summaries_file)

    for csvFileName in good_csv_files:

        csv_out_file_name = csvFileName.replace(".csv", "_summary.csv")

        print("in: " + csvFileName)
        print("out: " + csv_out_file_name)

        # csvFP = open(csvFileName, "r")
        # csvWFP = open(csvOutFileName, "w")
        # csvWFP2 = open(summariesFile, "a") # summary
        with open(csvFileName, "r") as csvFP, open(csv_out_file_name, "w") as csvWFP, open(summaries_file, "a") as csvWFP2:

            reader = csv.reader(csvFP, delimiter=';')
            writer = csv.writer(csvWFP, delimiter=';')
            writer2 = csv.writer(csvWFP2, delimiter=';')

            writer2.writerow([])
            writer2.writerow([csvFileName])
            header_w2 = ["start timestamp", "end timestamp", "benchmark", "header detail",
                        "#lines computed", "#SDC", "#AccTime",
                        "#(Abort==0 and END==0)", "framework_error"]
            writer2.writerow(header_w2)
            ##################

            csv_header = next(reader, None)

            writer.writerow(csv_header)

            lines = list(reader)

            i = 0
            size = len(lines)

            while i < size:
                if re.match("Time", lines[i][0]):
                    i += 1

                start_dt = datetime.strptime(lines[i][0].strip(), "%c")
                ##################summary
                benchmark = lines[i][2]
                input_detail = lines[i][3]
                ##################
                # print "date in line "+str(i)+": ",startDT
                j = i
                acc_time_s = float(lines[i][6])
                sdc_s = int(lines[i][4])
                abort_zero_s = 0
                framework_errors = 0

                if int(lines[i][7]) == 0:
                    abort_zero_s += 1
                writer.writerow(lines[i])
                if i + 1 < size:
                    try:
                        if re.match("Time", lines[i + 1][0]):
                            i += 1
                        if i + 1 < size:

                            while start_dt - datetime.strptime(lines[i + 1][0], "%c") < timedelta(minutes=60):
                                if i + 1 == size:
                                    break
                                if lines[i + 1][2] != lines[i][2]:  # not the same benchmark
                                    break
                                if lines[i + 1][3] != lines[i][3]:  # not the same input
                                    break
                                i += 1
                                ##################summary
                                end_dt1h = datetime.strptime(lines[i][0], "%c")
                                ##################
                                acc_time_s += float(lines[i][6])
                                sdc_s += int(lines[i][4])
                                if int(lines[i][7]) == 0 and int(lines[i][8]) == 0:
                                    abort_zero_s += 1

                                # CUDA framework errors
                                if int(lines[i][9]) == 1:
                                    framework_errors += 1

                                writer.writerow(lines[i])
                                if i == (len(lines) - 1):  # end of lines
                                    break
                                if re.match("Time", lines[i + 1][0]):
                                    i += 1
                                if i == (len(lines) - 1):  # end of lines
                                    break
                    except ValueError as e:
                        print("date conversion error, detail: " + str(e))
                        print("date: " + lines[i + 1][0] + "\n")
                header_c = ["start timestamp", "#lines computed", "#SDC", "#AccTime",
                           "#(Abort==0 and END==0), CUDA Framework "
                           "errors"]
                writer.writerow(header_c)
                row = [start_dt.ctime(), (i - j + 1), sdc_s, acc_time_s, abort_zero_s, framework_errors]
                writer.writerow(row)
                writer.writerow([])
                writer.writerow([])
                ##################summary
                try:
                    row2 = [start_dt.ctime(), end_dt1h.ctime(), benchmark, input_detail, (i - j + 1), sdc_s, acc_time_s,
                            abort_zero_s, framework_errors]
                    writer2.writerow(row2)
                except (ValueError, TypeError):
                    pass
                ##################
                i += 1


if __name__ == '__main__':
    main()
