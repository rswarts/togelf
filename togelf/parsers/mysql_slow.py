import re
import os


class Parser:
    """Parses mysql slow logs"""
    def __init__(self, client, log_path, log_level):
        self.user_host_re = re.compile('^# User@Host: (?P<user>\w+)\[[^\]]+\] @ (?P<host>[a-zA-Z0-9.-]+)? \[(?P<ip>[0-9.]+)]');
        self.stats_re = re.compile('^# Query_time: (?P<query_time>\d+) \s*Lock_time: (?P<lock_time>\d+) \s*Rows_sent: (?P<sent>\d+) \s*Rows_examined: (?P<scanned>\d+)')
        self.content_re = re.compile('^[^#].*')
        self.client = client
        self.message = {}
        self.log_path = log_path
        self.log_level = log_level
        
    def parse_line(self, line):
        # Check whether the log line matches our configured regex
        uh = user_host_re.match(line)
        if uh:
            if self.message:
                self.message['short_message'] = message['full_message'][:60]
                client.log(json.dumps(self.message))
                self.message = {}

            self.message['version'] = '1.0'
            self.message['facility'] = self.facility
            self.message['file'] = self.log_path
            self.message['level'] = self.log_level
            self.message['host'] = os.getenv('HOSTNAME')
            self.message['_user'] = uh.group('user')
            self.message['_client_host'] = uh.group('host')
            self.message['_client_ip'] = uh.group('ip')
            return

        st = stats_re.match(line)
        if st:
            self.message['_query_time'] = st.group('query_time')
            self.message['_lock_time'] = st.group('lock_time')
            self.message['_rows_sent'] = st.group('sent')
            self.message['_rows_examined'] = st.group('scanned')
            return
        ct = content_re.match(line)
        if ct:
            if message.has_key('full_message'):
                self.message['full_message'] += line
            else:
                self.message['full_message'] = line