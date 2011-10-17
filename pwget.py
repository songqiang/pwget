#!/usr/bin/env python

# Parallel wget
# Song Qiang <qiang.song@usc.edu> 2011
#
# This program provides an simple wrapper to allow multi-threaded
# downloading with the UNIX utility wget

import threading
import subprocess
import argparse
import tempfile
import os.path
import sys
import time

class WgetThread(threading.Thread):
	def __init__(self, (url, wget_options)):
		threading.Thread.__init__(self)
		self.url = url
		self.wget_options = wget_options

	def run(self):
		print >>sys.stderr, time.strftime("%X %x") + "\tStart\t" + self.url

		retcode = subprocess.call('wget ' + self.wget_options + ' ' + self.url,
								  shell=True)
		if retcode == 0:
			print >>sys.stderr, time.strftime("%X %x") + "\tDONE\t" + self.url
		else:
			print >>sys.stderr, time.strftime("%X %x") + "\tERROR\t" + self.url
			
def get_url_from_input_file(infile):
	urls = []
	if not infile == "":
		for line in open(infile):
			line  = line.split("#")[0].strip()
			if not line == "":
				urls.append(line)
	return urls

def main():
	parser = argparse.ArgumentParser(description='Parallel wget',
									 add_help = True)
	parser.add_argument('-i', action="store",
						default = "", dest="infile", help = "File of URLS")
	parser.add_argument('--max-num-threads', type = int, action="store",
						default = 3, dest="max_num_threads",
						help = "Maximum number of threads")
	parser.add_argument('--sleep', type = int, action="store",
						default = 5, dest="sleep",
						help = "The time interval to create new process")
	parse_result = parser.parse_known_args()
	(options, args) = (parse_result[0], parse_result[1])

	# add 1 to account for the main thread
	options.max_num_threads += 1

	# obtain resources to be dowloaded
	urls = get_url_from_input_file(options.infile)
	
	wget_options = ""
	for arg in args:
		if arg.find("ftp://") == 0 or arg.find("http://") == 0:
			urls.append(arg)
		else:
			wget_options += " " + arg
	
	threads = []
	while len(urls) > 0 or threading.active_count() > 1:
		# add new threads if not fully occupied
		while threading.active_count() < options.max_num_threads and len(urls) > 0:
			threads.append(WgetThread((urls.pop(), wget_options)))
			threads[-1].start()

		# remove finished threads	
		while len(threads) > 0 and not threads[0].is_alive():
			threads.pop(0)
		time.sleep(options.sleep)
			
if __name__ == '__main__':
	main()


	
			
