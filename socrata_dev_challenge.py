#!/usr/bin/env python

'''
This script is an example project as part of an application
to work at socrata.  

The goal is to accept two dates of the format [H]H:MM AM|PM 
on the command line and calculate the degrees that the minute
hand of the first time would have to travel to reach the
second time.  Its a neat little problem but probably not 
useful in any general sense.

Written by Pat Christopher, October 2012
'''

import sys, re

class InputTime:
	def __init__(self, t_input):
		self.raw_time = t_input
		csplit_raw = t_input.split(':')
		if len(csplit_raw) > 0:
			self.hours = csplit_raw[0]
		if len(csplit_raw) > 1:
			space_split = csplit_raw[1].split(' ')
			self.minutes = space_split[0]
			if len(space_split) > 1:
				self.midday = space_split[1]

	#  check if the time passed in was valid
	#  valid is hours from 1-12, minutes from 0-59 and AM or PM
	#    if any of the above are not there, date is not valid
	def is_valid(self):
		valid = True
        #  check for existance
		if hasattr(self, 'hours') and hasattr(self, 'minutes') and hasattr(self, 'midday'): 
            #  hours valid?
			if not re.match('^\d{1,2}$', self.hours) or int(self.hours) > 12 or int(self.hours) < 1:
				valid = False

            #  minutes valid?
			if not re.match('^\d{2}$', self.minutes) or int(self.minutes) > 59:
				valid = False

            #  midday valid?
			if not re.match('^AM|PM$', self.midday.upper()):
				valid = False
		else:
			valid = False

		return valid

	#  convert the passed-in time to absolute degrees
	#  if not valid, return -1
	def degrees(self):
		degrees = -1
		if self.is_valid:
			degrees = int(self.hours) * 360 + int(self.minutes) * 6
			#  modulo operation removes oddity of 12 being less then one
			degrees = degrees % (12 * 360)

			if self.midday == 'PM':
				degrees += 12 * 360

		return degrees

#  its not clear if spaces are allowed on the command
#  so deal with or without them
def print_help():
    print ("this script expects two arguments like so:\n"
           "  ./socrata_dev_challenge.py [H]H:MM AM|PM [H]H:MM AM|PM\n"
           "you may enclose each time in quote but you don't need to")

def parse_inputs(raw_args):
	if len(raw_args) == 4:
		time_one = InputTime(raw_args[0] + ' ' + raw_args[1])
		time_two = InputTime(raw_args[2] + ' ' + raw_args[3])
	elif len(raw_args) == 2:
		time_one = InputTime(raw_args[0])
		time_two = InputTime(raw_args[1])
	else:
		print_help()
		sys.exit(1)

	#  check both times are valid
	if not time_one.is_valid():
		print "The first time %s is not a valid time.  Must be of format: [H]H:MM [AM|PM], where [H] is from 1 to 12 and MM is from 00 to 59" % time_one.raw_time
	if not time_two.is_valid():
		print "The second time %s is not a valid time.  Must be of format: [H]H:MM [AM|PM], where [H] is from 1 to 12 and MM is from 00 to 59" % time_two.raw_time

	return time_one, time_two

def subtract_input_times(time_one, time_two):
	degrees_diff = -1
	if time_one.is_valid() and time_two.is_valid():
		degrees_diff = time_two.degrees() - time_one.degrees()

		#  check for wrap-around
		if degrees_diff < 0:
			degrees_diff = (24 * 360) + degrees_diff

	return degrees_diff
	

time_one, time_two = parse_inputs(sys.argv[1:])
degrees_diff = subtract_input_times(time_one, time_two)

if not degrees_diff == -1:
	print "Travel degrees are: %d" % degrees_diff
