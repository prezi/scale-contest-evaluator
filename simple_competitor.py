#!/usr/bin/env python
#
# Copyright (c) 2013 prezi.com
#
# Permission is hereby granted, free of charge, to any person obtaining a copy of
# this software and associated documentation files (the "Software"), to deal in
# the Software without restriction, including without limitation the rights to use,
# copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the
# Software, and to permit persons to whom the Software is furnished to do so,
# subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
#
import sys


def procLine(line):
    if line:
        return dict(zip(('date', 'time', 'id', 'queue', 'length'), line.split()))


def servers(date_time, command):
    if date_time:
        for i in range(100):
            for kind in ['general', 'url', 'export']:
                sys.stdout.write(' '.join((date_time['date'], date_time['time'], command, kind, '\n')))


def main():
    line = sys.stdin.readline()
    first_event = last_event = procLine(line)
    servers(first_event, 'launch')
    while line:
        last_event = procLine(line)
        sys.stdout.write(line)
        line = sys.stdin.readline()
    servers(last_event, 'terminate')

if __name__ == '__main__':
    main()
