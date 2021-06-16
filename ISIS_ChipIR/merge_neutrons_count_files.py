#!/usr/bin/env python3
import sys

# Default line in the logs
DEFAULT_CHIPIR_LINE_SIZE = 80

def merge_files():
    output_file = sys.argv[1]
    paths = sys.argv[2:]
    paths.sort()
    with open(output_file, "w") as output_file:
        for path in paths:
            with open(path, 'r') as input_file:
                for line in input_file:
                    if len(line) >= DEFAULT_CHIPIR_LINE_SIZE:
                        output_file.write(line)
                    else:
                        print(f"Line not parsed {line} at file {path}")


if __name__ == '__main__':
    merge_files()
