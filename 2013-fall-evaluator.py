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
# Example usage:
# python 2013-fall-evaluator.py 2013-fall-week1-abbreviated.log output00000.out
#
# Where the first argument is the input log file and the second argument is the
# output of the contest submission.
#

import argparse
import heapq
import logging
import math
import re
import sys
import uuid
import json

# ---
# The baseline score is that of running 100 nodes for each queue for 3 days + 1 hour (because
# instances should be started at least 2 minutes before the first conversion job).
#                       HOURS * MACHINES * QUEUES
BASELINE_SCORE = float((24 * 2 + 1) * 100 * 3)

def calculate_score(vm_hours_used, test_case_id):
    """ Turn the number of VM hours used by the contestant into a score.
    The score also depends on test_case_id."""
    if 1 < test_case_id < 4: # The secret data sets
        score_coefficient = 100000.0
    elif test_case_id < 2: # the public data sets
        score_coefficient = 100.0
    else: # the sample data set
        score_coefficient = 0.0
    # vm hours used should be > 0 and < BASELINE_SCORE
    capped_vm_hours = max(0.0, min(BASELINE_SCORE, float(vm_hours_used)))
    score = score_coefficient * ((BASELINE_SCORE - capped_vm_hours ) / BASELINE_SCORE)
    return score


def main():
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    testcase_status = []
    score = 0
    vm_hours_used, is_valid = evaluate_submission_output(output_file)
    if is_valid:
        # We score this as if it were one of the solutions for a secret data set
        score += calculate_score(vm_hours_used, 3)
        testcase_status.append(1)
    else:
        testcase_status.append(0)

    print score
    print " ".join(str(i) for i in testcase_status)

# --
# Prezi Scale contest evaluator logic
# From: https://raw.github.com/prezi/scale-contest-evaluator/master/evaluator.py
# --

class Event(object):
    def __init__(self, data_dict):
        # Note that timestamp is in UTC
        self.timestamp = int(data_dict['timestamp'])

    def __cmp__(self, other):
        return cmp(self.timestamp, other.timestamp)


class Job(Event):
    def __init__(self, data_dict):
        super(Job, self).__init__(data_dict)
        self.category = data_dict['category']
        self.duration = float(data_dict['duration'])
        self.guid = data_dict['guid']


class Command(Event):
    def __init__(self, data_dict):
        super(Command, self).__init__(data_dict)
        self.category = data_dict['category']
        self.cmd = data_dict['cmd']


class Machine(object):
    # VMs are billed by the hour (3600 seconds).
    BILLING_UNIT = 3600
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
        """The number of seconds until the billing period ends."""
        return abs((now - self.running_since) % -self.BILLING_UNIT)

    def is_active(self, now):
        """The machine has booted and it is currently unoccupied."""
        return now >= self.active_from and self.busy_till <= now

    def take_job(self, timestamp, length):
        self.busy_till = self.job_runtime(timestamp, length)

    def job_runtime(self, timestamp, length):
        """The unix timestamp at which this machine can finish with a job of the given length."""
        return max(self.active_from, self.busy_till, timestamp) + length

class WithLog(object):
    log = logging.getLogger('prezi.com')

    def info(self, *args):
        self.log.info(*args)


class State(WithLog):

    FREE_QUEUE_TIME = 5
    MAX_QUEUE_TIME = 120
    # TRIAL_ENDS refers to the grace period of the first 24 hours
    # during which competitors will not be disqualified or penalized.
    TRIAL_ENDS = 0 #24 * 60 * 60

    def __init__(self):
        self.time = 0
        self.billed = 0
        self.trial = None
        self.overwait = False
        self.jobs = {'url': [], 'default': [], 'export': []}
        self.machines = {'url': [], 'default': [], 'export': []}

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
            if event.cmd == 'launch':
                self.launch(Machine(event.timestamp, self), event.category)
            self.process_events(event.category)
            if event.cmd == 'terminate':
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

    def process_events(self, category):
        """ Processes one of the three queues in its entirety.
        For each job, it takes the machine which is freed earliest
        and assigns it the job.
        """
        while self.jobs[category]:
            job = heapq.heappop(self.jobs[category])
            self.info('job_retrieved %d %s' % (job.timestamp, job.guid))
            best = None     # (rate, overrun, machine)
            # Find a machine which can run the job within the "free time interval"
            for machine in self.machines[category]:
                if machine.is_active(job.timestamp + self.FREE_QUEUE_TIME):
                    # Rate is the amount of time left after the job is done in the billing period.
                    rate = machine.till_billing(machine.job_runtime(job.timestamp, job.duration))
                    if best is None or rate < best[0]:
                        best = (rate, 0, machine)
            # Find a machine which can run the job before the contestant is disqualified.
            if best is None:
                for machine in self.machines[category]:
                    if machine.is_active(job.timestamp + self.MAX_QUEUE_TIME):
                        start_time = machine.job_runtime(job.timestamp, 0)
                        if best is None or start_time < best[0]:
                            best = (start_time, start_time - job.timestamp, machine)
            if best is not None:
                machine = best[2]
                machine.take_job(job.timestamp, job.duration)
                penalty = self.calculate_penalty(best[1])
                if penalty > 0:
                    self.info('job_penalty %d %s' % (penalty, job.guid))
                    if self.now > self.trial:
                        self.billed += penalty
                self.info('job_executed_till %d %s %s' % (machine.busy_till, job.guid, machine.guid))
            else:
                self.info('no_machine_for %d %s' % (job.timestamp, job.guid))
                self.overwait = job.timestamp >= self.trial
                if self.overwait:
                    sys.exit(1)

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

    def calculate_penalty(self, overrun):
        return max(int(math.ceil(3.0 * (overrun - self.FREE_QUEUE_TIME) / self.MAX_QUEUE_TIME)), 0)

    def evaluate(self):
        for category in ['default', 'export', 'url']:
            self.process_events(category)
            for machine in self.machines[category]:
                self.terminate(machine, category)
            if self.overwait:
                return -1
        return self.billed


def read_events(fd):
    common = r'^(?P<timestamp>\d+) '
    cmd_re = re.compile(common + r'(?P<cmd>[^ ]+) (?P<category>\w+)\s*$')
    job_re = re.compile(common + r'(?P<duration>\d+\.\d+) (?P<guid>[^ ]+) (?P<category>\w+)\s*$')
    line_no = 0
    while True:
        line = fd.readline()
        line_no += 1
        #if line_no % 1000 == 0:
        #    print line_no
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
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

def evaluate_submission_output(output_file):
    lines_read = 0
    with open(output_file, 'r') as output_fd:
        state = State()
        for event in read_events(output_fd):
            lines_read += 1
            state.receive(event)
            if state.overwait:
                break
    return state.evaluate(), not state.overwait


if __name__ ==  '__main__':
    main()
