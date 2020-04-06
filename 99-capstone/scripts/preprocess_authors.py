#!/usr/bin/env python3

import sys
import re

# Values taken from historical reddit source code:
# https://github.com/reddit-archive/reddit/blob/master/r2/r2/lib/validator/validator.py#L1567-L1570
MIN_USERNAME_LENGTH = 3
MAX_USERNAME_LENGTH = 20
USER_REGEX = re.compile(r"\A[\w-]+\Z", re.UNICODE)

def _replace_none(val):
    if val == 'None':
        return ''
    return val

def run():
    line_regex = re.compile(r"^(\d+) (.*?) ([\d]+|None) ([\d]+|None) (\-?[\d]+|None) (\-?[\d]+|None)$", re.UNICODE)

    for line in sys.stdin:
        try:
            m = line_regex.match(line)
            userid = m.group(1)
            username = m.group(2)
            date1 = _replace_none(m.group(3))
            date2 = _replace_none(m.group(4))
            int1 = _replace_none(m.group(5))
            int2 = _replace_none(m.group(6))

            # Determine if username is valid based.
            # 0 (invalid) or 1 (valid) will be added as an additional column.
            valid = USER_REGEX.match(username) is not None \
                and MIN_USERNAME_LENGTH <= len(username) <= MAX_USERNAME_LENGTH

            sys.stdout.write('|'.join([userid, username, date1, date2, int1, int2, str(int(valid))]) + '\n')
        except:
            # The above implementatino should cover all cases.
            # In any none forseen cases output the line to stderr
            sys.stderr.write(line)

if __name__ == "__main__":
    run()