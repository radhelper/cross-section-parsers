#!/usr/bin/python -u
import sys
import re
import csv

from datetime import timedelta
from datetime import datetime

# smallest run in minutes
# SMALLEST_RUN = timedelta(minutes=20)
ONE_HOUR = 60


def getDt(yearDate, dayTime, secFrac):
    yv = yearDate.split('-')
    year = int(yv[0])
    month = int(yv[1])
    day = int(yv[2])

    dv = dayTime.split(':')
    hour = int(dv[0])
    minute = int(dv[1])
    second = int(dv[2])

    # we get secFrac in seconds, so we must convert to microseconds
    # e.g: 0.100 seconds = 100 milliseconds = 100000 microseconds
    microsecond = int(float("0." + secFrac) * 1e6)

    return datetime(year, month, day, hour, minute, second, microsecond)


def getWenderFactor(startDT):
    # Oct 2 9:03  52624.6
    if startDT < datetime(2018, 10, 2, 9, 3, 0, 0):
        return 52624.6
    # Oct 2	16:33	53089.1
    elif startDT < datetime(2018, 10, 2, 16, 33, 0, 0):
        return 53089.1
    # Oct 3	16:49	50663
    elif startDT < datetime(2018, 10, 3, 16, 49, 0, 0):
        return 50663
    # Oct 4	9:20	50845.2
    elif startDT < datetime(2018, 10, 4, 9, 20, 0, 0):
        return 50845.2
    # Oct 4	17:11	50399
    elif startDT < datetime(2018, 10, 4, 17, 11, 0, 0):
        return 50399
    # Oct 5	15:37	50793.2
    elif startDT < datetime(2018, 10, 5, 15, 37, 0, 0):
        return 50793.2
    # Oct 6	10:27	50686.5
    elif startDT < datetime(2018, 10, 6, 10, 27, 0, 0):
        return 50686.5
    # Oct 7	10:27	51832.2
    elif startDT < datetime(2018, 10, 7, 10, 27, 0, 0):
        return 51832.2
    # Oct 8	9:28	52178.6
    # el startDT < datetime(2018, 10, 8, 9, 28, 0, 0):
    else:
        return 52178.6


def readCountFile(inFileName):
    inFile = open(inFileName, 'r')
    fileLines = []
    for l in inFile:
        line = l.rstrip()
        m = re.match("(\d{2,4}-\d{1,2}-\d{1,2}) (\d\d:\d\d:\d\d),(\d{1,3}) (.*)", line)
        if m:
            fileLines.append(line)

    return fileLines


def getFluenceFlux(start_dt, end_dt, file_lines, factor):
    # inFile = open(inFileName, 'r')
    # endDT = startDT + timedelta(minutes=60)

    pulse_count = 0
    last_count = None
    last_dt = start_dt
    time_no_pulse = 0  # in seconds

    # for l in inFile:
    for l in file_lines:

        line = l.rstrip()
        m = re.match("(\d{2,4}-\d{1,2}-\d{1,2}) (\d\d:\d\d:\d\d),(\d{1,3}) (.*)", line)
        if m:
            cur_dt = getDt(m.group(1), m.group(2), m.group(3))
            if start_dt <= cur_dt <= end_dt:
                try:
                    cur_count = int(m.group(4))
                except ValueError:
                    continue  # Ignore line in case it contain "Start of test"

                if last_count is not None:
                    diff_count = cur_count - last_count
                    if diff_count <= 0 or last_count <= 0:
                        time_no_pulse += (cur_dt - last_dt).total_seconds()
                    else:
                        pulse_count += diff_count
                last_count = cur_count
                last_dt = cur_dt
            elif cur_dt > end_dt:
                return [(pulse_count * getWenderFactor(start_dt) / ((end_dt - start_dt).total_seconds())) * factor,
                        time_no_pulse]
    # It should not get out of the loop, but in case there is no pulse data, timeNoPulse is updated
    time_no_pulse += (end_dt - cur_dt).total_seconds()
    return (pulse_count * getWenderFactor(start_dt) / ((end_dt - start_dt).total_seconds())) * factor, time_no_pulse


def getFlux(start_dt, file_lines, factor):
    # inFile = open(inFileName, 'r')
    end_dt = start_dt + timedelta(minutes=ONE_HOUR)

    pulse_count = 0
    last_count = None
    last_dt = start_dt
    time_no_pulse = 0  # in seconds

    for l in file_lines:

        line = l.rstrip()
        m = re.match("(\d{2,4}-\d{1,2}-\d{1,2}) (\d\d:\d\d:\d\d),(\d{1,3}) (.*)", line)
        if m:
            cur_dt = getDt(m.group(1), m.group(2), m.group(3))
            if start_dt <= cur_dt <= end_dt:
                try:
                    cur_count = int(m.group(4))
                except ValueError:
                    continue  # Ignore line in case it contain "Start of test"

                if last_count is not None:
                    diff_count = cur_count - last_count
                    if diff_count <= 0 or last_count <= 0:
                        time_no_pulse += (cur_dt - last_dt).total_seconds()
                    else:
                        pulse_count += diff_count
                last_count = cur_count
                last_dt = cur_dt
            elif cur_dt > end_dt:
                return (pulse_count * getWenderFactor(start_dt) / (60 * 60)) * factor, time_no_pulse
    # It should not get out of the loop, but in case there is no pulse data, timeNoPulse is updated

    # timeNoPulse += (endDT - curDt).total_seconds()
    return (pulse_count * getWenderFactor(start_dt) / (60 * 60)) * factor, time_no_pulse


def read_count_file(in_file_name):
    """
    Read neutron log file
    :param in_file_name: neutron log filename
    :return: list with all neutron lines
    """
    file_lines = []
    with open(in_file_name, 'r') as in_file:
        for l in in_file:
            # Sanity check, we require a date at the beginning of the line
            line = l.rstrip()
            if not re.match("(\d{2,4}-\d{1,2}-\d{1,2}) (\d\d:\d\d:\d\d),(\d{1,3}) (.*)", line):
                sys.stderr.write("Ignoring line (malformed):\n%s\n" % (line))
                continue

            if "N/A" in line:
                break

            file_lines.append(line)
    return file_lines


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
        key = i[1] + i[2] + i[3]
        if key not in grouped_benchmarks:
            grouped_benchmarks[key] = []

        grouped_benchmarks[key].append(i)

    return grouped_benchmarks


def get_distance_data(distance_factor_file):
    """
    Get the distance data based on distance factor file
    :param distance_factor_file: path to distance factor file
    :return: a dict containing the distance date
    """
    distance_data = {}
    with open(distance_factor_file, "r") as csv_distance:
        dict_data = csv.DictReader(csv_distance, delimiter=';')
        for i in dict_data:
            distance_data[i['board']] = 400.0 / (
                (float(i['distances']) / 100 + 20) * (float(i['distances']) / 100 + 20))
    return distance_data


def main():
    if len(sys.argv) < 3:
        print("Usage: %s <neutron counts input file> <csv file> <distance factor file>" % (sys.argv[0]))
        sys.exit(1)

    in_file_name = sys.argv[1]
    csv_file_name = sys.argv[2]
    distance_factor_file = sys.argv[3]
    # Load all distances before continue
    distance_data = get_distance_data(distance_factor_file=distance_factor_file)

    csv_out_file_summary = csv_file_name.replace(".csv", "_cross_section_summary.csv")
    print("in: " + csv_file_name)
    print("out: " + csv_out_file_summary)

    with open(csv_file_name, "r") as csv_input, open(csv_out_file_summary, "w") as csv_summary:
        # reading and starting the csvs files
        reader = csv.reader(csv_input, delimiter=';')
        writer_csv_summary = csv.writer(csv_summary, delimiter=';')
        header_summary = ["start timestamp", "end timestamp", "Machine", "benchmark",
                          "header info", "#lines computed", "#SDC", "#AccTime", "#(Abort==0)",
                          "Flux 1h", "Flux AccTime", "Fluence(Flux * $AccTime)",
                          "Fluence AccTime(FluxAccTime * $AccTime)", "Cross Section SDC",
                          "Cross Section Crash", "Time Beam Off (sec)", "Cross Section SDC AccTime",
                          "Cross Section Crash AccTime", "Time Beam Off AccTime (sec)", "distance_factor"]

        writer_csv_summary.writerow(header_summary)
        full_lines = list(reader)

        # grouping the benchmarks
        grouped_benchmarks = pre_process_data(full_file_lines=full_lines)

        # We need to read the neutron count files before calling get_fluency_flux
        file_lines = read_count_file(in_file_name)

        for bench in grouped_benchmarks:
            print("Parsing for {}".format(bench))
            lines = grouped_benchmarks[bench]
            i = 0
            while i < len(lines) - 1:
                try:
                    start_dt = datetime.strptime(lines[i][0].strip(), "%c")
                    end_dt = datetime.strptime(lines[i + 1][0].strip(), "%c")
                except ValueError as err:
                    print(err)
                    continue

                machine = lines[i][1]
                bench = lines[i][2]
                header_info = lines[i][3]
                # acc_time
                acc_time_s = float(lines[i][6])
                # #SDC
                sdc_s = int(lines[i][4])
                # abort
                abort_zero_s = 0
                if int(lines[i][7]) == 0 and int(lines[i][8]) == 0:
                    abort_zero_s += 1

                first_i = i
                end_dt_next = end_dt
                while (end_dt_next - start_dt) < timedelta(minutes=ONE_HOUR):
                    i += 1
                    if i >= (len(lines) - 1):  # end of lines
                        break
                    if lines[first_i][2] != lines[i][2] or lines[first_i][3] != lines[i][3]:
                        print("ISSO ACONTECEU NATURALMENTE")
                        continue

                    acc_time_s += float(lines[i][6])
                    sdc_s += int(lines[i][4])
                    if int(lines[i][7]) == 0 and int(lines[i][8]) == 0:
                        abort_zero_s += 1

                    end_dt_next = datetime.strptime(lines[i + 1][0].strip(), "%c")
                    end_dt = datetime.strptime(lines[i][0].strip(), "%c")

                # why consider such a small run???
                # if (end_dt - start_dt) < SMALLEST_RUN:
                #     i += 1
                #     continue

                print("Generating cross section for {}, start {} end {}".format(bench.strip(), start_dt, end_dt))

                # compute 1h flux; sum SDC, ACC_TIME, Abort with 0; compute fluency (flux*(sum ACC_TIME))
                distance_factor = distance_data[lines[i][1].strip()]
                flux, time_beam_off = getFlux(start_dt=start_dt, file_lines=file_lines, factor=distance_factor)
                flux_acc_time, time_beam_off_acc_time = getFluenceFlux(start_dt=start_dt,
                                                                       end_dt=(start_dt + timedelta(seconds=acc_time_s)),
                                                                       file_lines=file_lines, factor=distance_factor)

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
                row = [start_dt.ctime(), end_dt.ctime(), machine, bench, header_info,
                       (i - first_i + 1), sdc_s, acc_time_s, abort_zero_s, flux,
                       flux_acc_time, fluency, fluency_acc_time,
                       cross_section, cross_section_crash, time_beam_off, cross_section_acc_time,
                       cross_section_crash_acc_time, time_beam_off_acc_time, distance_factor]

                writer_csv_summary.writerow(row)
                i += 1


#########################################################
#                    Main Thread                        #
#########################################################
if __name__ == '__main__':
    main()



# #########################################################
# #                    Main Thread                        #
# #########################################################
# if len(sys.argv) < 4:
#     print "Usage: %s <lansce pulse log file> <csv file> <factor>" % (sys.argv[0])
#     sys.exit(1)
#
# inFileName = sys.argv[1]
# csvFileName = sys.argv[2]
# factor = float(sys.argv[3])
#
# csvOutFileName = csvFileName.replace(".csv", "_cross_section.csv")
#
# print "in: " + csvFileName
# print "out: " + csvOutFileName
#
# csvFP = open(csvFileName, "r")
# reader = csv.reader(csvFP, delimiter=';')
# csvWFP = open(csvOutFileName, "w")
# writer = csv.writer(csvWFP, delimiter=';')
#
# ##################summary
# csvWFP2 = open("summaries.csv", "a")
# writer2 = csv.writer(csvWFP2, delimiter=';')
# writer2.writerow([])
# writer2.writerow([csvFileName])
# headerW2 = ["start timestamp", "end timestamp", "benchmark", "header detail", "#lines computed", "#SDC", "#AccTime",
#             "#(Abort==0)"]
# headerW2.append("Flux 1h (factor " + str(factor) + ")")
# headerW2.append("Flux AccTime (factor " + str(factor) + ")")
# headerW2.append("Fluence(Flux * $AccTime)")
# headerW2.append("Fluence AccTime(FluxAccTime * $AccTime)")
# headerW2.append("Cross Section SDC")
# headerW2.append("Cross Section Crash")
# headerW2.append("Time No Neutron Count (sec)")
# headerW2.append("Cross Section SDC AccTime")
# headerW2.append("Cross Section Crash AccTime")
# headerW2.append("Time No Neutron Count AccTime (sec)")
# writer2.writerow(headerW2)
# ##################
#
# csvHeader = next(reader, None)
#
# writer.writerow(csvHeader)
#
# lines = list(reader)
#
# # We need to read the neutron count files before calling getFluenceFlux
# readCountFile()
#
# i = 0
# size = len(lines)
# while i < size:
#     if re.match("Time", lines[i][0]):
#         i += 1
#
#     progress = "{0:.2f}".format(((float(i) / float(size)) * 100.0))
#     sys.stdout.write("\rProcessing Line " + str(i) + " of " + str(size) + " - " + progress + "%")
#     sys.stdout.flush()
#
#     startDT = datetime.strptime(lines[i][0][0:-1], "%c")
#     endDT1h = startDT
#     ##################summary
#     benchmark = lines[i][2]
#     inputDetail = lines[i][3]
#     ##################
#     # print "date in line "+str(i)+": ",startDT
#     j = i
#     accTimeS = float(lines[i][6])
#     sdcS = int(lines[i][4])
#     abortZeroS = 0
#     if (int(lines[i][7]) == 0):
#         abortZeroS += 1
#     writer.writerow(lines[i])
#     if i + 1 < size:
#         try:
#             if re.match("Time", lines[i + 1][0]):
#                 i += 1
#             if i + 1 < size:
#                 while (datetime.strptime(lines[i + 1][0][0:-1], "%c") - startDT) < timedelta(minutes=60):
#                     progress = "{0:.2f}".format(((float(i) / float(size)) * 100.0))
#                     sys.stdout.write("\rProcessing Line " + str(i) + " of " + str(size) + " - " + progress + "%")
#                     sys.stdout.flush()
#
#                     if lines[i + 1][2] != lines[i][2]:  # not the same benchmark
#                         break
#                     if lines[i + 1][3] != lines[i][3]:  # not the same input
#                         break
#                     # print "line "+str(i)+" inside 1h interval"
#                     i += 1
#                     # summary
#                     endDT1h = datetime.strptime(lines[i][0][0:-1], "%c")
#                     ##################
#                     accTimeS += float(lines[i][6])
#                     sdcS += int(lines[i][4])
#                     if int(lines[i][7]) == 0:
#                         abortZeroS += 1
#                     writer.writerow(lines[i])
#                     if i == (len(lines) - 1):  # end of lines
#                         break
#                     if re.match("Time", lines[i + 1][0]):
#                         i += 1
#                     if i == (len(lines) - 1):  # end of lines
#                         break
#         except ValueError as e:
#             print "date conversion error, detail: " + str(e)
#             print "date: " + lines[i + 1][0][0:-1] + "\n"
#     # compute 1h flux; sum SDC, ACC_TIME, Abort with 0; compute fluence (flux*(sum ACC_TIME))
#     flux, timeBeamOff = getFlux(startDT)
#     fluence = flux * accTimeS
#     fluxAccTime, timeBeamOffAccTime = getFluenceFlux(startDT, (startDT + timedelta(seconds=accTimeS)))
#     fluenceAccTime = fluxAccTime * accTimeS
#     if fluence > 0:
#         crossSection = sdcS / fluence
#         crossSectionCrash = abortZeroS / fluence
#     else:
#         crossSection = 0
#         crossSectionCrash = 0
#     if fluenceAccTime > 0:
#         crossSectionAccTime = sdcS / fluenceAccTime
#         crossSectionCrashAccTime = abortZeroS / fluenceAccTime
#     else:
#         crossSectionAccTime = 0
#         crossSectionCrashAccTime = 0
#     headerC = ["start timestamp", "end timestamp", "#lines computed", "#SDC", "#AccTime", "#(Abort==0)"]
#     headerC.append("Flux 1h (factor " + str(factor) + ")")
#     headerC.append("Flux AccTime (factor " + str(factor) + ")")
#     headerC.append("Fluence(Flux * $AccTime)")
#     headerC.append("Fluence AccTime(FluxAccTime * $AccTime)")
#     headerC.append("Cross Section SDC")
#     headerC.append("Cross Section Crash")
#     headerC.append("Time No Neutron Count (sec)")
#     headerC.append("Cross Section SDC AccTime")
#     headerC.append("Cross Section Crash AccTime")
#     headerC.append("Time No Neutron Count AccTime (sec)")
#     writer.writerow(headerC)
#     row = [startDT.ctime(), endDT1h.ctime(), (i - j + 1), sdcS, accTimeS, abortZeroS, flux, fluxAccTime, fluence,
#            fluenceAccTime, crossSection, crossSectionCrash, timeBeamOff, crossSectionAccTime, crossSectionCrashAccTime,
#            timeBeamOffAccTime]
#     # row = [startDT.ctime(), (i-j+1), sdcS, accTimeS, abortZeroS]
#     writer.writerow(row)
#     writer.writerow([])
#     writer.writerow([])
#     ##################summary
#     row2 = [startDT.ctime(), endDT1h.ctime(), benchmark, inputDetail, (i - j + 1), sdcS, accTimeS, abortZeroS, flux,
#             fluxAccTime, fluence, fluenceAccTime, crossSection, crossSectionCrash, timeBeamOff, crossSectionAccTime,
#             crossSectionCrashAccTime, timeBeamOffAccTime]
#     # row2 = [startDT.ctime(),endDT1h.ctime(),benchmark,inputDetail, (i-j+1), sdcS, accTimeS, abortZeroS]
#     writer2.writerow(row2)
#     ##################
#     i += 1
#
# progress = "{0:.2f}".format(((float(i) / float(size)) * 100.0))
# sys.stdout.write("\rProcessing Line " + str(i) + " of " + str(size) + " - " + progress + "%")
# sys.stdout.flush()
# sys.stdout.write("\nDone\n")
#
# csvFP.close()
# csvWFP.close()
#
# sys.exit(0)
