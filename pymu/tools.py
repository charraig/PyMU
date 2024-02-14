"""
Tools for common functions relayed to commanding, reading, and parsing PMU data
"""

from .client import Client
from .pmuCommandFrame import CommandFrame
from .pmuConfigFrame import ConfigFrame
from .pmuDataFrame import DataFrame
from .pmuLib import bytesToHexStr

MAXFRAMESIZE = 65535


def turnDataOff(cli, idcode):
    """
    Send command to turn off real-time data

    :param cli: Client being used to connect to data source
    :type cli: Client
    :param idcode: Frame ID of data source
    :type idcode: int
    """
    cmdOff = CommandFrame("DATAOFF", idcode)
    response = cli.sendData(cmdOff)
    return response


def dataoff_and_close(cli, idcode):
    response = turnDataOff(cli, idcode)
    cli.stop()
    return response


def turnDataOn(cli, idcode):
    """
    Send command to turn on real-time data

    :param cli: Client connection to data source
    :type cli: Client
    :param idcode: Frame ID of data source
    :type idcode: int
    """
    cmdOn = CommandFrame("DATAON", idcode)
    response = cli.sendData(cmdOn)
    return response


def requestConfigFrame2(cli, idcode):
    """
    Send command to request config frame 2

    :param cli: Client connection to data source
    :type cli: Client
    :param idcode: Frame ID of data source
    :type idcode: int
    """
    cmdConfig2 = CommandFrame("CONFIG2", idcode)
    response = cli.sendData(cmdConfig2)
    return response


def readConfigFrame2(cli, debug=False):
    """
    Retrieve and return config frame 2 from PMU or PDC

    :param cli: Client connection to data source
    :type cli: Client
    :param debug: Print debug statements
    :type debug: bool
    :return: Populated ConfigFrame
    """
    configFrame = None

    s = cli.readSample(4)
    configFrame = ConfigFrame(bytesToHexStr(s), debug)
    expSize = configFrame.framesize
    s = cli.readSample(expSize - 4)
    configFrame.frame = configFrame.frame + bytesToHexStr(s).upper()
    configFrame.finishParsing()

    return configFrame


def getDataSampleBytes(rcvr, total_bytes=-1, debug=False):
    """
    Get a data sample regardless of TCP or UDP connection

    :param rcvr: Object used for receiving data frames
    :type rcvr: :class:`Client`/:class:`Server`
    :type total_bytes: int
    :param total_bytes: indicate how many bytes to read, or indicate to read from header
    :param debug: Print debug statements
    :type debug: bool
    :return: Data frame in hex string format
    """

    if isinstance(rcvr, Client):
        if total_bytes == -1:
            introBytesStrSize = 4
            intro_bytes = rcvr.readSample(introBytesStrSize)
            introHexStr = bytesToHexStr(intro_bytes)
            total_bytes = int(introHexStr[5:], 16)
        else:
            introBytesStrSize = 0
            intro_bytes = b""

        lenToRead = total_bytes - introBytesStrSize
        payload_bytes = rcvr.readSample(lenToRead)
        full_bytes_str = intro_bytes + payload_bytes
    else:
        full_bytes_str = rcvr.readSample(8)

    return full_bytes_str


def getDataSampleHex(rcvr, total_bytes=-1, debug=False):
    full_bytes_str = getDataSampleBytes(rcvr, total_bytes, debug)
    return bytesToHexStr(full_bytes_str)


def getConfigFrame2(idcode, outgoing_client, incoming_client, compute_bytes=True):
    response = requestConfigFrame2(outgoing_client, idcode)
    if response["status"] == "success":
        configFrame = readConfigFrame2(incoming_client, debug=False)
    else:
        raise Exception("Unable to request Config Frame")

    if compute_bytes:
        pass
        # TO DO: Implement compute_bytes computation based on config frame
    return configFrame


def startDataStream(idcode, outgoing_client, incoming_client, debug=False):
    """
    Connect to data source, request config frame, send data start command

    :param idcode: ID of PMU
    :type idcode: int
    :param outgoing_client: client object (Client or Proxy type) for messages to device
    :type outgoing_cient: BaseClient
    :param incoming_client: object (Client or Server type) to read messages from device
    :type outgoing_cient: Client
    :param debug: Print debug statements
    :type debug: bool

    :return: Populated :py:class:`pymu.pmuConfigFrame.ConfigFrame` object
    """
    configFrame = None

    while configFrame is None:
        configFrame = getConfigFrame2(
            idcode, outgoing_client, incoming_client, compute_bytes=False
        )

    _ = turnDataOn(outgoing_client, idcode)
    data = getDataSampleHex(incoming_client)
    dFrame = DataFrame(data, configFrame)
    dataframe_bytes = dFrame.framesize
    configFrame.update_dataframe_size(dataframe_bytes)
    incoming_client.stop()

    return configFrame


def getStations(configFrame):
    """
    Returns all station names from the config frame

    :param configFrame: ConfigFrame containing stations
    :type configFrame: ConfigFrame

    :return: List containing all the station names
    """
    stations = []
    for s in configFrame.stations:
        print("Station:", s.stn)
        stations.append(s)

    return stations


def parseSamples(data, configFrame, pmus):
    """
    Takes an array of dataFrames and inserts the data into an array of aggregate phasors

    :param data: List containing all the data samples
    :type data: List
    :param configFrame: ConfigFrame containing stations
    :type configFrame: ConfigFrame
    :param pmus: List of phasor values
    :type pmus: List

    :return: List containing all the phasor values
    """
    numOfSamples = len(data)
    for s in range(0, numOfSamples):
        for p in range(0, len(data[s].pmus)):
            for ph in range(0, len(data[s].pmus[p].phasors)):
                utcTimestamp = data[s].soc.utcSec + (
                    data[s].fracsec / configFrame.time_base.baseDecStr
                )
                pmus[p][ph].addSample(
                    utcTimestamp,
                    data[s].pmus[p].phasors[ph].mag,
                    data[s].pmus[p].phasors[ph].rad,
                )

    return pmus
