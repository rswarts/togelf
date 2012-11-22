
import zlib
from socket import *


# This is the class that sends log messages to the GELF server
class Client:
    def __init__(self, config):
        self.graylog2_server = config.get('default', 'gelfServer')
        self.graylog2_port = config.getint('default', 'gelfPort')
        self.maxChunkSize = config.getint('default', 'gelfMaxChunkSize')

    def log(self, message):
        UDPSock = socket(AF_INET,SOCK_DGRAM)
        zmessage = zlib.compress(message)
        UDPSock.sendto(zmessage,(self.graylog2_server,self.graylog2_port))
        UDPSock.close()