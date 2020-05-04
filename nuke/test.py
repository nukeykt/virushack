from pysnmp.carrier.asyncore.dispatch import AsyncoreDispatcher
from pysnmp.carrier.asyncore.dgram import udp, udp6, unix
from pyasn1.codec.ber import encoder, decoder
from pysnmp.proto import api
from time import time, sleep, strftime, gmtime
from datetime import datetime

snmp_obj = None
snmp_ans = []

def cbTimerFun(timeNow):
    if timeNow - snmp_obj.startedAt > 3:
        raise Exception("Request timed out")

def cbRecvFun(transportDispatcher, transportDomain, tranportAddress, wholeMsg, reqPDU=None):
    global snmp_obj
    if reqPDU is None:
        reqPDU = snmp_obj.reqPDU
    while wholeMsg:
        rspMsg, wholeMsg = decoder.decode(wholeMsg, asn1Spec=snmp_obj.pMod.Message())
        rspPDU = snmp_obj.pMod.apiMessage.getPDU(rspMsg)
        if snmp_obj.pMod.apiPDU.getRequestID(reqPDU) == snmp_obj.pMod.apiPDU.getRequestID(rspPDU):
            errorStatus = snmp_obj.pMod.apiPDU.getErrorStatus(rspPDU)
            if errorStatus:
                print("SNMP: {}".format(errorStatus.prettyPrint()))
            else:
                global snmp_ans
                snmp_ans = []
                for oid, val in snmp_obj.pMod.apiPDU.getVarBinds(rspPDU):
                    for x in snmp_obj.oid_list:
                        if x[0] == oid.prettyPrint():
                            snmp_ans.append([x[1], val.prettyPrint()])
                            #print('%s = %s' % (x[1], val.prettyPrint()))
            transportDispatcher.jobFinished(1)
    return wholeMsg

class SNMPPoll:
    def __init__(self):
        self.pMod = api.protoModules[api.protoVersion1]

        self.time_id = '1.3.6.1.2.1.1.3.0'
        self.temp_id = '1.3.6.1.4.1.8886.1.1.4.2.1.0'
        self.cpuload_id = '1.3.6.1.4.1.8886.1.1.1.5.1.1.1.3.1' # raisecomCPUUtilization1sec
        self.volt1_id = '1.3.6.1.4.1.8886.1.1.4.3.1.1.3.1'
        self.volt2_id = '1.3.6.1.4.1.8886.1.1.4.3.1.1.3.2'
        self.volt3_id = '1.3.6.1.4.1.8886.1.1.4.3.1.1.3.3'
        self.volt4_id = '1.3.6.1.4.1.8886.1.1.4.3.1.1.3.4'
        self.fanspeed_id = '1.3.6.1.4.1.8886.1.1.5.2.2.1.2.1'
        #cpuload_id = "1.3.6.1.4.2011.6.3.4.1.2" # huawei
        #cpuload_id = "1.3.6.1.4.1.8886.1.1.1.5.1.1.1.3.1"
        self.oid_list = [[self.time_id, "systime"],
                    [self.temp_id, "temperature"],
                    [self.cpuload_id, "CpuUtilization1sec"],
                    [self.fanspeed_id, "FanSpeed"],
                    [self.volt1_id, "Volt1"],
                    [self.volt2_id, "Volt2"],
                    [self.volt3_id, "Volt3"],
                    [self.volt4_id, "Volt4"],
        ]

        self.reqPDU = self.pMod.GetRequestPDU()
        self.pMod.apiPDU.setDefaults(self.reqPDU)
        self.pMod.apiPDU.setVarBinds(
            self.reqPDU, ((self.time_id, self.pMod.Null('')),
                     #(temp_id, pMod.Null('')),
                     (self.cpuload_id, self.pMod.Null('')),
                     (self.fanspeed_id, self.pMod.Null('')),
                     (self.volt1_id, self.pMod.Null('')),
                     (self.volt2_id, self.pMod.Null('')),
                     (self.volt3_id, self.pMod.Null('')),
                     (self.volt4_id, self.pMod.Null('')),
                     )
        )

        self.reqMsg = self.pMod.Message()
        self.pMod.apiMessage.setDefaults(self.reqMsg)
        self.pMod.apiMessage.setCommunity(self.reqMsg, 'vhack2020')
        self.pMod.apiMessage.setPDU(self.reqMsg, self.reqPDU)

        self.startedAt = 0

    def Poll(self):
        global snmp_obj, snmp_ans
        snmp_obj = self
        self.startedAt = time()

        transportDispatcher = AsyncoreDispatcher()

        transportDispatcher.registerRecvCbFun(cbRecvFun)
        transportDispatcher.registerTimerCbFun(cbTimerFun)

        transportDispatcher.registerTransport(
            udp.domainName, udp.UdpSocketTransport().openClientMode()
        )

        transportDispatcher.sendMessage(
            encoder.encode(self.reqMsg), udp.domainName, ('localhost', 161)
        )

        transportDispatcher.jobStarted(1)

        transportDispatcher.runDispatcher()

        transportDispatcher.closeDispatcher()

        return snmp_ans


test = SNMPPoll()

with open('poll.txt', 'w') as f:
    while True:
        ans = test.Poll()
        my_time =  datetime.now().strftime("%b %d %Y %H %M %S")
        ans.append(["localtime", my_time])
        print(ans)
        print(ans, file=f)
        sleep(3)
        f.flush()
