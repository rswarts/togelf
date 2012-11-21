#!/usr/bin/env python

# Note that this script requires the python-json package to be installed

import ConfigParser
try: import simplejson as json
except ImportError: import json
import os
import Queue
import re
import sys
import syslog
import threading
import time
import traceback
import zlib
from os import stat
from stat import ST_SIZE
from socket import *

configFile = '/etc/gelfDaemon.conf'

user_host_re = re.compile('^# User@Host: (?P<user>\w+)\[[^\]]+\] @ (?P<host>[a-zA-Z0-9.-]+)? \[(?P<ip>[0-9.]+)]');
stats_re = re.compile('^# Query_time: (?P<query_time>\d+) \s*Lock_time: (?P<lock_time>\d+) \s*Rows_sent: (?P<sent>\d+) \s*Rows_examined: (?P<scanned>\d+)')
content_re = re.compile('^[^#].*')

# This is the class that sends log messages to the GELF server
class Client:
    def __init__(self):
        self.graylog2_server = config.get('default', 'gelfServer')
        self.graylog2_port = config.getint('default', 'gelfPort')
        self.maxChunkSize = config.getint('default', 'gelfMaxChunkSize')

    def log(self, message):
        UDPSock = socket(AF_INET,SOCK_DGRAM)
        zmessage = zlib.compress(message)
        UDPSock.sendto(zmessage,(self.graylog2_server,self.graylog2_port))
        UDPSock.close()

# This class is used to collapse multi-line log files into a single line. You 
# will want to populate regEx with something useful!
class Concatenate:
    def __init__(self):
        self.results = ''
        # This is a default regex. Please override this.
        self.regEx = '^\ \~$'
        
    def Concatenate(self, line):
        # Get rid of those pesky newlines that can straggle on
        line = line.rstrip()
        m = re.search(self.regEx, line)
        if m:
            final = self.results
            self.results = ''
            return final
        else:
            self.results = self.results + '<br/>' + line
            self.results = self.results.lstrip('<br/>')
            return False

# This is the main logging thread. One of these will start up for each log file
# that is to be monitored. Note that while this approach is safe, it does not
# handle missing log files well and will simply exit the thread with an error
# if the log file is missing or inaccessible for any reason.
class LogThread(threading.Thread):
    def GetConfig(self):
        self.logPath = os.path.abspath(config.get(self.section, 'path'))
        self.logLevel = config.get(self.section, 'level')
        self.regEx = config.get(self.section, 'regex')
        self.facility = config.get(self.section, 'facility')
        self.shortMessageRegEx = config.get(self.section, 'short_message_regex')
        try:
            self.concatenateOn = config.get(self.section, 'concatenate_on')
        except ConfigParser.NoOptionError:
            self.concatenateOn = False

    def OpenLog(self):
        try:
            self.handle = open(self.logPath, 'r')
            self.fileLength = os.stat(self.logPath)[ST_SIZE]
            self.handle.seek(self.fileLength)
            self.position = self.handle.tell()
        except:
            raise

    def ResetLog(self):
        """This is called to reset the position in the log file upon a 
        truncation"""
        self.handle.close()
        self.handle = open(self.logPath, 'r')
        self. position = self.handle.tell()
       
    def run(self):
        # The the paths and whatnot from the config file
        self.GetConfig()
        
        # Open our log file for reading
        try:
            self.OpenLog()
        except IOError:
            sys.exit()
        except: 
            sys.stderr.write('%s: unknown error occurred, thread exiting\n' %
                            sys.argv[0])
            sys.exit()
        
        # Instantiate the Concatenation class
        cat = Concatenate()
        message = {}
        # Now for the thread's main loop
        while True:
            # We break if the queue is a non-zero size. This is pretty
            # simplistic.
            if WorkerQueue.qsize() != 0:
                break
            self.position = self.handle.tell()
            line = self.handle.readline()
            if not line:
                # this means we've hit the EOF or have been truncated
                if stat(self.logPath)[ST_SIZE] < self.position:
                    self.ResetLog()
                else:
                    time.sleep(0.1)
                    self.handle.seek(self.position)
            else:
                # Now ditch any blank lines, because they're dumbass
                if line != '':
                    uh = st = ct = False
                    # Check whether the log line matches our configured regex
                    uh = user_host_re.match(line)
                    st = stats_re.match(line)
                    ct = content_re.match(line)

                    if uh:
                        if message:
                            message['short_message'] = message['full_message'][:60]
                            client.log(json.dumps(message))
                            message = {}
                        
                        message['version'] = '1.0'
                        message['facility'] = self.facility
                        message['file'] = self.logPath
                        message['level'] = self.logLevel
                        message['host'] = os.getenv('HOSTNAME')
                        message['_user'] = uh.group('user')
                        message['_client_host'] = uh.group('host')
                        message['_client_ip'] = uh.group('ip')
                    elif st:
                        message['_query_time'] = st.group('query_time')
                        message['_lock_time'] = st.group('lock_time')
                        message['_rows_sent'] = st.group('sent')
                        message['_rows_examined'] = st.group('scanned')
                    elif ct:
                        if message.has_key('full_message'):
                            message['full_message'] += line
                        else:
                            message['full_message'] = line


if __name__ == '__main__':
    # We use a typical RFC 822 (called an .ini file) format
    config = ConfigParser.ConfigParser()
    try:
        config.read(configFile)
    except:
        # obviously no config means we're bailing immediately
        traceback.print_exc()
        sys.stderr.write('%s: unable to open config file\n' % sys.argv[0])
        sys.exit(1)

    # Instantiate our actual GELF agent here. Note that as we are using UDP
    # there is no need to ever close any connections here. We can just exit
    # the script later without any grace or civility.
    client = Client()

    # Create our queuing object, which is only used to signal threads
    # to shut down. This object may be expanded for more things later on though,
    # such as perhaps reloading the config.
    WorkerQueue = Queue.Queue()

    # Now go through the list of configured sections in the config file, each
    # of which should be for a single log file to be monitored
    sections = config.sections()

    # Ignore the default section, obviously
    sections.remove('default')

    # Create names for logging threads that will be started up
    threads = []
    for section in sections:
        print ("%s: found config section called %s" % (sys.argv[0], section))
        threads.append(section + 'Thread')

    # Start our threads up
    for thread in threads:
    	sectionName = thread.replace('Thread', '')
        thread = LogThread()
        thread.section = sectionName
        thread.start()
    
    # Now for our main loop, which really does dick all
    while True:
        try:
            time.sleep(0.1)
        except KeyboardInterrupt:
            print "%s: interrupt called, shutting down" % sys.argv[0]
            WorkerQueue.put('0')
            time.sleep(0.2)
            sys.exit(0)
        
