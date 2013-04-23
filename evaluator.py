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
import argparse
import calendar
import heapq
import logging
import math
import random
import re
import sys
import uuid


class Event(object):
    def __init__(self, data_dict):
        fields = ['year', 'month', 'day', 'hour', 'minute', 'second']
        self.timestamp = calendar.timegm(map(lambda k: int(data_dict[k]), fields))

    def __cmp__(self, other):
        return cmp(self.timestamp, other.timestamp)


class Job(Event):
    def __init__(self, data_dict):
        super(Job, self).__init__(data_dict)
        self.category = data_dict['category']
        self.elapsed = float(data_dict['elapsed'])
        self.guid = data_dict['guid']


class Command(Event):
    def __init__(self, data_dict):
        super(Command, self).__init__(data_dict)
        self.category = data_dict['category']
        self.cmd = data_dict['cmd']


class Machine(object):
    # VMs are billed by the hour (3600 seconds).
    BILLING_UNIT = 3600
    # Boot time is 2 minutes (120 seconds).
    MACHINE_INACTIVE = 120

    def __init__(self, booted, world):
        self.active_from = booted + self.MACHINE_INACTIVE
        # busy_till specifies when the job currently processed by the
        # node will end (if any).
        self.busy_till = 0
        self.world = world
        self.terminated = False
        self.guid = str(uuid.uuid1())

    def __cmp__(self, other):
        now = self.world.now
        return cmp(self.till_billing(now), other.till_billing(now))

    @property
    def running_since(self):
        return self.active_from - self.MACHINE_INACTIVE

    def till_billing(self, now):
        return abs((now - self.running_since) % -self.BILLING_UNIT)

    def is_active(self, now):
        return now >= self.active_from and self.busy_till <= now


class WithLog(object):
    log = logging.getLogger('prezi.com')

    def info(self, *args):
        self.log.info(*args)


class State(WithLog):

    # TRIAL_ENDS refers to the grace period of the first 24 hours
    # during which competitors will not be disqualified or penalized.
    TRIAL_ENDS = 24 * 60 * 60
    # After the grace period, 5 seconds is the maximum time a job can
    # spend in the queue.
    MAX_QUEUE_TIME = 5

    def __init__(self):
        self.time = 0
        self.billed = 0
        self.trial = None
        self.overwait = False
        self.jobs = {'url': [], 'general': [], 'export': []}
        self.machines = {'url': [], 'general': [], 'export': []}

    @property
    def now(self):
        return self.time

    @now.setter
    def now(self, value):
        self.time = value
        if self.trial is None:
            self.trial = self.time + self.TRIAL_ENDS
            self.info('trial_ends %d' % self.trial)

    def receive(self, event):
        self.now = event.timestamp
        if isinstance(event, Job):
            heapq.heappush(self.jobs[event.category], event)
            self.process_events(event.category)
        elif isinstance(event, Command):
            self.process_events(event.category)
            if event.cmd == 'launch':
                self.launch(Machine(event.timestamp, self), event.category)
            elif event.cmd == 'terminate':
                closest = heapq.nsmallest(1, self.machines[event.category])
                if closest:
                    self.terminate(closest[0], event.category)

    def launch(self, machine, category):
        heapq.heappush(self.machines[category], machine)
        self.info('launch %d %d %s' % (machine.running_since, machine.busy_till, machine.guid))

    def terminate(self, machine, category):
        machine.terminated = True
        bill = self.bill(category=category)
        self.info('terminate %d %d %d %s' % (machine.running_since, machine.active_from, bill, machine.guid))

    def rnd_machine(self, category):
        n = len(self.machines[category])
        rnd_start = random.randrange(n)
        for i in range(n):
            yield self.machines[category][(rnd_start + i) % n]

    def process_events(self, category):
        while self.jobs[category]:
            job = heapq.heappop(self.jobs[category])
            self.info('job_retrieved %d %s' % (job.timestamp, job.guid))
            for machine in self.rnd_machine(category):
                if machine.is_active(job.timestamp + self.MAX_QUEUE_TIME):
                    machine.busy_till = max(machine.busy_till, job.timestamp) + job.elapsed
                    self.info('job_executed_till %d %s %s' % (machine.busy_till, job.guid, machine.guid))
                    break
            else:
                self.info('no_machine_for %d %s' % (job.timestamp, job.guid))
                self.overwait = job.timestamp > self.trial

    def bill(self, category=None):
        bill_previous = self.billed
        if category:
            all_terminated = filter(lambda m: m.terminated, self.machines[category])
            for machine in all_terminated:
                self.bill_it(machine)
            for machine in all_terminated:
                self.machines[category].remove(machine)
        else:
            for _, machines in self.machines.items():
                for machine in machines:
                    self.bill_it(machine)
        return self.billed - bill_previous

    def bill_it(self, machine):
        """ Computes the cost of a single virtual machine. """
        if self.now > self.trial:
            when_stops = max(self.now, machine.busy_till)
            billing_start = max(self.trial, machine.running_since)
            billing_end = max(self.trial, when_stops + machine.till_billing(when_stops))
            self.billed += int(math.ceil(float(billing_end - billing_start) / machine.BILLING_UNIT))

    def evaluate(self):
        for category in ['general', 'export', 'url']:
            self.process_events(category)
            for machine in self.machines[category]:
                self.terminate(machine, category)
            if self.overwait:
                return -1
        return self.billed


def read_events(fd):
    common = r'^(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2}) (?P<hour>\d\d):(?P<minute>\d\d):(?P<second>\d\d) '
    cmd_re = re.compile(common + r'(?P<cmd>[^ ]+) (?P<category>\w+)')
    job_re = re.compile(common + r'(?P<guid>[^ ]+) (?P<category>\w+) (?P<elapsed>\d+\.\d+)')

    while True:
        line = fd.readline()
        if not line:
            break

        m = job_re.match(line)
        if m:
            yield Job(m.groupdict())
            continue
        m = cmd_re.match(line)
        if m:
            yield Command(m.groupdict())


def parse_arguments():
    parser = argparse.ArgumentParser(description='Prezi scale contest evaluator')
    parser.add_argument('-d', '--debug', dest='debug', action='store_true', default=False, help='turn on debugging')
    return parser.parse_known_args()


def set_logger():
    logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)


def main():
    args, rest = parse_arguments()
    if args.debug:
        set_logger()
    state = State()
    with open(rest[0]) if rest else sys.stdin as fd:
        for event in read_events(fd):
            state.receive(event)
            if state.overwait:
                break
    print state.evaluate()
    sys.exit(1 if state.overwait else 0)

if __name__ == '__main__':
    main()
