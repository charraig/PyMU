import socket

import requests


class Endpoint:
    def __init__(self):
        pass

    def sendData(self):
        pass

    def stop(self):
        pass

    def readSample(self):
        pass


class Proxy(Endpoint):
    def __init__(self, url, auth=None):
        super().__init__()
        self.url = url
        self.payload = None
        self.session = requests.Session()
        if auth is not None:
            self.session.auth = auth

    def readSample(self):
        raise Exception("Proxy clients have no read ability.")


class HologramProxy(Proxy):
    def __init__(self, auth_type, auth_params):
        if auth_type == "basic":
            auth = ("apikey", auth_params["apikey"])
            url = "https://dashboard.hologram.io/api/1/devices/messages"
        elif auth_type == "webhook":
            auth = None
            did = auth_params["deviceid"]
            whguid = auth_params["webhookguid"]
            url = f"https://dashboard.hologram.io/api/1/devices/messages/{did}/{whguid}"
        else:
            raise ValueError("auth_type not recognized.")
        super().__init__(url, auth)

    def attach_core_payload(self, device_ids, protocol, port, wait_for_response):
        self.payload = {
            "deviceids": device_ids,
            "protocol": protocol,
            "port": port,
            "waitForResponse": wait_for_response,
        }

    @staticmethod
    def parse_response(response):
        response_json = response.json()
        status = "success" if response_json["success"] else "failed"
        return {"status": status, "response": response}

    def sendData(self, frame):
        self.payload["data"] = frame.fullFrameBase64
        response = self.session.post(self.url, json=self.payload)
        return self.parse_response(response)


class Client(Endpoint):
    """
    Client class that creates a client and provides simple functions for connecting to
    PMUs or PDCs without needing to directly use Python's socket library.
    Supports INET and UNIX sockets

    :param theDestIp: IP address to connect to.  If using unix socket this is
      the file name to connect to
    :type theDestIp: str
    :param theDestPort: Port to connect to
    :type theDestPort: int
    :param proto: Protocol to use.  Accepts TCP or UDP
    :type proto: str
    :param sockType: Type of socket to create.  INET or UNIX
    :type sockType: str
    """

    def __init__(
        self,
        theDestIp,
        theDestPort,
        protocol="TCP",
        timeout=60,
        sockType="INET",
    ):
        super().__init__()
        self.destAddr = None
        self.theSocket = None
        self.theConnection = None
        self.useUdp = False
        self.unixSock = False

        self.destIp = theDestIp
        self.destPort = theDestPort
        self.destAddr = (theDestIp, theDestPort)

        if protocol == "UDP":
            self.useUdp = True
        if sockType.upper() == "UNIX":
            self.unixSock = True

        self.createSocket()
        self.setTimeout(timeout)
        self.connectToDest()

    def createSocket(self):
        """Create socket based on constructor arguments"""
        if self.useUdp:
            if self.unixSock:
                self.theSocket = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
            else:
                self.theSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        else:
            if self.unixSock:
                self.theSocket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            else:
                self.theSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def connectToDest(self):
        """Connect socket to destination IP:Port.  If UNIX socket then use destIP"""
        if not self.useUdp:
            if self.unixSock:
                self.theSocket.connect(self.destIp)
            else:
                self.theSocket.connect(self.destAddr)

    def readSample(self, bytesToRead):
        """
        Read a sample from the socket

        :param bytesToRead: Number of bytes to read from socket
        :type bytesToRead: int

        :return: Byte array of data read from socket
        """
        byte_str = b""

        while len(byte_str) < bytesToRead:
            need_to_read = bytesToRead - len(byte_str)
            if self.useUdp:
                byte_str += self.theSocket.recvfrom(need_to_read)
            else:
                byte_str += self.theSocket.recv(need_to_read)
        return byte_str

    def sendData(self, frame):
        """Send bytes to destination

        :param bytesToSend: Number of bytes to send
        :type bytesToSend: int
        """
        bytesToSend = frame.fullFrameBytes
        if self.useUdp:
            if self.unixSock:
                self.theSocket.sendto(bytesToSend, self.destIp)
            else:
                self.theSocket.sendto(bytesToSend, self.destAddr)
        else:
            self.theSocket.send(bytesToSend)
        return {"status": "unknown", "response": None}

    def stop(self):
        """Close the socket connection"""
        self.theSocket.close()

    def setTimeout(self, numOfSecs):
        """Set socket timeout

        :param numOfSecs: Time to wait for socket action to complete before
          throwing timeout exception
        :type numOfSecs: int
        """
        self.theSocket.settimeout(numOfSecs)

    def __class__(self):
        return "client"
