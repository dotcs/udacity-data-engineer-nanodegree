#!/usr/bin/env python3

import sys
import re

def run():
    regex = re.compile(r"^(\d+) (.*?) ([\d]+) ([\d]+) (\-?[\d]+) (\-?[\d]+)$")
    for line in sys.stdin:
        m = regex.match(line)
        print('|'.join(m.groups()))

if __name__ == "__main__":
    run()