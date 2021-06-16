#!/usr/bin/python -u

import os
import sys
import re
import csv
from datetime import timedelta
from datetime import datetime
import time


def get_dt(year_date, day_time, sec_frac):
    yv = year_date.split('/')
    day = int(yv[0])
    month = int(yv[1])
    year = int(yv[2])

    dv = day_time.split(':')
    hour = int(dv[0])
    minute = int(dv[1])
    second = int(dv[2])

    # we get secFrac in seconds, so we must convert to microseconds
    # e.g: 0.100 seconds = 100 milliseconds = 100000 microseconds
    microsecond = int(float(sec_frac) * 1e6)

    return datetime(year, month, day, hour, minute, second, microsecond)


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
            if not re.match("\d{1,2}/\d{1,2}/\d{2,4}", line):
                sys.stderr.write("Ignoring line (malformed):\n%s\n" % (line))
                continue

            if "N/A" in line:
                break

            file_lines.append(line)
    return file_lines


def get_fluence_flux(start_dt, end_dt, file_lines, factor, distance_factor=1.0): # file_lines eh a corrtete

    last_curr_integral = None
    last_dt = None
    new_curr = 0
    samples = 0
    beam_off_time = 0
    first_curr_integral = None
    # file_lines teve que ser colocado como csv, substituit " " por ; e "tab" por ;
    for l in file_lines:    # file_lines is the neutron_log/current_log given by CHIPIR
        #print(l)
        # Parse the line
        line = l.split(';')
        # Format
        # Date ; Hour ; Current ; Integral Current
        # Date format: dd/mm/yy
        year_date = line[0]
        buff = line[1].split('.')
        day_time = buff[0]
        sec_frac = "."+buff[1]  # Sec Frac nao esta separado no log
        actual_curr = float(line[2])
        curr_integral = float(line[3])
        #print(str(year_date) + str(day_time) + str(sec_frac) + str(curr_integral))
        # Generate datetime for line
        cur_dt = get_dt(year_date, day_time, sec_frac)  #Horario lido do arquivo

        if start_dt <= cur_dt and first_curr_integral is None:
            first_curr_integral = curr_integral
            new_curr = float(line[2])   # Obtemos a corrente que devemos somar.
            samples = 1                 # Necesario para calcular a media.
            last_dt = cur_dt
            continue

        if first_curr_integral is not None:
            if actual_curr == 0: #beam off se nao tem corrente
                beam_off_time += (cur_dt - last_dt).total_seconds() # Adiciona a diferenca do tempo de i e i-1 -> Beam Parado 3S

            #print(curr_integral)
            last_curr_integral = curr_integral
            last_dt = cur_dt
            new_curr += float(line[2])
            samples += 1

        if cur_dt > end_dt:
            #print(cur_dt)
            #interval_total_seconds = float((end_dt - start_dt).total_seconds())
            #flux1h = ((last_curr_integral - first_curr_integral) * factor) / interval_total_seconds
            #flux1h *= distance_factor

            thermal_flux = new_curr / samples   # Fluxo eh a corrente media
            thermal_flux *= factor              # Multiplicada x 16000 (Jun 2018)
            thermal_flux *= distance_factor     # Distancia eh a mesma para todos, assim assume-se 1

            return thermal_flux, beam_off_time

        elif first_curr_integral is not None:
            last_curr_integral = curr_integral

    print("Neutron file have less logged time than Execution log aka parsed+board.csv")
    # Here is the case that logparsed_boarf_,csv end after logcurrent
    flux1h = -1
    beam_off_time = -1
    return flux1h, beam_off_time

def main():
    if len(sys.argv) < 4:
        print("Usage: %s <neutron counts input file> <csv file> <factor>" % (sys.argv[0]))
        sys.exit(1)
    in_file_name = sys.argv[1]
    csv_file_name = sys.argv[2]
    factor = float(sys.argv[3]) #This is the magic factor
    distance_factor = float(sys.argv[4]) #

    csv_out_file_full = csv_file_name.replace(".csv", "_cross_section.csv")
    csv_out_file_summary = csv_file_name.replace(".csv", "_cross_section_summary.csv")
    print("in: " + csv_file_name)
    print("out: " + csv_out_file_full)
    with open(csv_file_name, "r") as csv_input, open(csv_out_file_full, "w") as csv_full, open(csv_out_file_summary,
                                                                                               "w") as csv_summary:
        reader = csv.reader(csv_input, delimiter=';')
        writer_csv_full = csv.writer(csv_full, delimiter=';')
        writer_csv_summary = csv.writer(csv_summary, delimiter=';')

        csv_header = next(reader, None)

        #writer_csv_full.writerow(csv_header)
        writer_csv_summary.writerow(csv_header)

        lines = list(reader)

        header_c = ["Machine","benchmark","header info","start timestamp", "end timestamp", "#lines computed", "#SDC", "#AccTime", "#(Abort==0)",
                    "Flux 1h (factor " + str(distance_factor) + ")",
                    "Flux AccTime (factor " + str(distance_factor) + ")",
                    "Fluence(Flux * $AccTime)", "Fluence AccTime(FluxAccTime * $AccTime)", "Cross Section SDC",
                    "Cross Section Crash", "Time Beam Off (sec)", "Cross Section SDC AccTime",
                    "Cross Section Crash AccTime", "Time Beam Off AccTime (sec)"]
        writer_csv_full.writerow(header_c)
        # We need to read the neutron count files before calling get_fluence_flux
        file_lines = read_count_file(in_file_name)

        i = -1
        while(i< len(lines)-1):
            i = i +1 # Soluciona sobreposicao
            print("Nova Linha",i)
            try:
                start_d_t = datetime.strptime(lines[i][0][0:-1], "%c")
                end_d_t = datetime.strptime(lines[i + 1][0][0:-1], "%c")
            except:
                print("data lixo")

                continue
            start_dt = datetime.strptime(lines[i][0][0:-1], "%c")
            j = i
            machine = lines[i][1]
            bench = lines[i][2]
            header_info = lines[i][3]
            # acc_time
            acc_time_s = float(lines[i][6])
            # #SDC
            sdc_s = int(lines[i][4])
            # abort
            abort_zero_s = 0
            if( int(lines[i][7]) == 0 and int(lines[i][8]) == 0 ):
                abort_zero_s += 1

            writer_csv_summary.writerow(lines[i])
            end_dt = datetime.strptime(lines[i + 1][0][0:-1], "%c")
            print("parsing file {} date in line {}:{}".format(csv_file_name.replace(".csv", ""), str(i), start_dt,
                                                              end_dt))
            last_line = ""
            #flag = 0
            while (end_dt - start_dt) < timedelta(minutes=60):
                print("Procuro no run")
                #flag = 1
                if lines[i + 1][2] != lines[i][2]:  # not the same benchmark
                    break
                if lines[i + 1][3] != lines[i][3]:  # not the same input
                    break
                # print "line "+str(i)+" inside 1h interval"
                i += 1 # !!!!----------------------------------------------------------------------------
                acc_time_s += float(lines[i][6])
                sdc_s += int(lines[i][4])
                if( int(lines[i][7]) == 0 and int(lines[i][8]) == 0 ):
                    abort_zero_s += 1
                #writer_csv_full.writerow(lines[i])
                last_line = lines[i]
                if i == (len(lines) - 1):  # end of lines
                    break
                end_dt = datetime.strptime(lines[i + 1][0][0:-1], "%c")
            # compute 1h flux; sum SDC, ACC_TIME, Abort with 0; compute fluence (flux*(sum ACC_TIME))
            print("Fora While",i)

            flux, time_beam_off = get_fluence_flux(start_dt=start_dt, end_dt=(start_dt + timedelta(minutes=60)),file_lines=file_lines, factor=factor,distance_factor=distance_factor)
            flux_acc_time, time_beam_off_acc_time = get_fluence_flux(start_dt=start_dt, end_dt=(start_dt + timedelta(seconds=acc_time_s)),file_lines=file_lines, factor=factor, distance_factor=distance_factor)

            fluence = flux * acc_time_s
            fluence_acc_time = flux_acc_time * acc_time_s
            if fluence > 0:
                cross_section = sdc_s / fluence
                cross_section_crash = abort_zero_s / fluence
            else:
                cross_section = 0
                cross_section_crash = 0
            if fluence_acc_time > 0:
                cross_section_acc_time = sdc_s / fluence_acc_time
                cross_section_crash_acc_time = abort_zero_s / fluence_acc_time
            else:
                cross_section_acc_time = 0
                cross_section_crash_acc_time = 0

            #writer_csv_full.writerow(header_c)
            writer_csv_summary.writerow(last_line)
            writer_csv_summary.writerow(header_c)
            row = [machine,bench,header_info,start_dt.ctime(), end_dt.ctime(), (i - j + 1), sdc_s, acc_time_s, abort_zero_s, flux, flux_acc_time,
                   fluence,
                   fluence_acc_time, cross_section, cross_section_crash, time_beam_off, cross_section_acc_time,
                   cross_section_crash_acc_time,
                   time_beam_off_acc_time]
            writer_csv_full.writerow(row)
            writer_csv_summary.writerow(row)
            #writer_csv_full.writerow([])
            # writer_csv_full.writerow([])
            writer_csv_summary.writerow([])
            # writer_csv_summary.writerow([])


#########################################################
#                    Main Thread                        #
#########################################################
if __name__ == '__main__':
    main()
