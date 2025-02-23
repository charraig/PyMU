from struct import pack

from PyCRC.CRCCCITT import CRCCCITT

from . import pmuLib as pl


class TransferFrame:
    """
    Custom class meant to create a message that can be passed to a socket connection.
    Only contains timestamp, phasor values, and ID for each phasor

    :param inputDataFrame: Populated data frame containing measurement values
    :type inputDataFrame: pmuDataFrame
    """

    def __init__(self, inputDataFrame):
        self.header = "AAFF"
        self.length = 0
        self.timestamp = None
        self.numOfPhasors = 0
        self.phasors = []
        self.crc = None
        self.fullFrameBytes = ""
        self.fullFrameHexStr = ""

        self.dataFrame = inputDataFrame
        self.parseDataSample()
        self.createFullFrame()

    def parseDataSample(self):
        """Parse the input data sample"""
        self.length += len(self.header) / 2  # Separator
        self.timestamp = self.dataFrame.soc.utcSec + (
            self.dataFrame.fracsec / self.dataFrame.configFrame.time_base.baseDecStr
        )
        self.parsePhasors()

        self.length += len(pack("d", self.timestamp))  # Double
        self.length += len(pack("H", self.numOfPhasors))  # Unsigned Short
        self.length += len(pack("I", int(self.length)))  # Unsigned Int

    def parsePhasors(self):
        """Parse the phasors in the data sample to extract measurements"""
        ident = 0
        for p in range(0, self.dataFrame.configFrame.num_pmu):
            for ph in range(0, self.dataFrame.pmus[p].numOfPhsrs):
                units = self.dataFrame.configFrame.stations[p].phunits[ph].voltORcurr
                pField = PhasorField(self.dataFrame.pmus[p].phasors[ph], ident, units)
                self.length += len(pField.fullFrameHexStr) / 2
                self.phasors.append(pField)
                ident = ident + 1
        self.numOfPhasors = len(self.phasors)

    def genCrc(self):
        """Generate CRC-CCITT"""
        crcCalc = CRCCCITT("FFFF")
        frameInBytes = bytes.fromhex(self.fullFrameHexStr)
        theCrc = hex(crcCalc.calculate(frameInBytes))[2:].zfill(4)
        self.crc = theCrc.upper()

    def createFullFrame(self):
        """Put all the pieces to together to create full transfer frame"""
        self.fullFrameHexStr += self.header
        self.fullFrameHexStr += pl.intToHexStr(int(self.length)).zfill(8).upper()
        self.fullFrameHexStr += pl.doubleToHexStr(self.timestamp).zfill(16).upper()
        self.fullFrameHexStr += pl.intToHexStr(self.numOfPhasors).zfill(4).upper()
        for pf in self.phasors:
            self.fullFrameHexStr += pf.fullFrameHexStr
        # Removed CRC because it was slowing down transfer rates
        # self.genCrc()
        # self.fullFrameHexStr += self.crc
        self.fullFrameBytes = bytes.fromhex(self.fullFrameHexStr)


# # # # # #
# Part of the frame that contains phasor values.
# This field is repeated as needed in the frame
# # # # # #
class PhasorField:
    """Class to hold simplified phasor fields

    :param phasor: Phasor containing measurements
    :type phasor: Phasor
    :param idNum: Frame ID to use
    :type idNum: int
    :param theUnits:  Volts or Amps
    :type theUnits: str
    """

    def __init__(self, phasor, idNum, theUnits):
        self.options = None
        self.fullFrameHexStr = None
        self.length = 0

        self.phasorFrame = phasor
        self.ident = idNum
        self.value = self.phasorFrame.mag
        self.angle = self.phasorFrame.rad
        self.units = theUnits
        self.parseOptions()
        self.createPhasorFieldFrame()

    def parseOptions(self):
        """Parse options and create bytes as hex str"""
        if self.units == "VOLTAGE":
            self.options = pl.intToHexStr(0).zfill(4)
        else:
            self.options = pl.intToHexStr(2**15)

    def createPhasorFieldFrame(self):
        """Create phasor field frame inside full transfer frame"""
        fullFrameHexStr = ""
        fullFrameHexStr += pl.intToHexStr(self.ident).zfill(4)
        fullFrameHexStr += pl.doubleToHexStr(self.value)
        fullFrameHexStr += pl.doubleToHexStr(self.angle)
        fullFrameHexStr += self.options
        self.fullFrameHexStr = fullFrameHexStr.upper()
        self.length = int(len(fullFrameHexStr) / 2)
