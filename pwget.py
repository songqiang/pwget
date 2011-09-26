#!/usr/bin/env python

# Parallel wget
# Song Qiang <qiang.song@usc.edu> 2011
#
# This program provides an simple wrapper to allow multi-threaded
# downloading with the UNIX utility wget

import threading
import subprocess
import optparse
import tempfile
import os.path
import sys

class WgetThread(threading.Thread):
	def __init__(self, (url, LOG_REQUIRED)):
		threading.Thread.__init__(self)
		self.url = url
		self.LOG_REQUIRED = LOG_REQUIRED

	def run(self):
		print >>sys.stderr, "Start\t", self.url

		if self.LOG_REQUIRED:
			logfile = tempfile.NamedTemporaryFile(prefix = 'wget-log.', 
												  dir='.').name
		else:
			logfile = '/dev/null'

		retcode = subprocess.call('wget -r -N -o ' + logfile + ' ' + self.url,
								  shell=True)
		if retcode == 0:
			print >>sys.stderr, "DONE\t", self.url
		else:
			print >>sys.stderr, "ERROR\t", self.url
			
def main():
	parser = optparse.OptionParser("usage: %prog")
	parser.add_option("-n", "--max-num-threads", dest="max_num_threads",
					  default = 3, type = "int",
					  help = "Maximum number of threads")
	parser.add_option("-l", "--logfile",
					  action="store_true", dest="LOG_REQUIRED", default=False,
					  help="Output log file for each url")
	(options, args) = parser.parse_args()

	# add 1 to account for the main thread
	options.max_num_threads += 1
	
	urls = args
	threads = []
	while len(urls) > 0 or threading.active_count() > 1:
		if threading.active_count() <= options.max_num_threads and len(urls) > 0:
			threads.append(WgetThread((urls.pop(), options.LOG_REQUIRED)))
			threads[-1].start()

		while len(threads) > 0 and not threads[0].is_alive():
			threads.pop(0)
			
if __name__ == '__main__':
	main()


	
			
