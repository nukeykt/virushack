from pysnmp.carrier.asyncore.dispatch import AsyncoreDispatcher
from pysnmp.carrier.asyncore.dgram import udp, udp6, unix
from pyasn1.codec.ber import encoder, decoder
from pysnmp.proto import api
from time import time, sleep

pMod = api.protoModules[api.protoVersion1]

time_id = '1.3.6.1.2.1.1.3.0'
temp_id = '1.3.6.1.4.1.8886.1.1.4.2.1.0'
cpuload_id = '1.3.6.1.4.1.8886.1.1.1.5.1.1.1.3.1' # raisecomCPUUtilization1sec
volt_id = '1.3.6.1.4.1.8886.1.1.4.3.1.1.3'
fanspeed_id = '1.3.6.1.4.1.8886.1.1.5.2.2.1.2'
oid_list = [[time_id, "systime"],
            [temp_id, "temperature"],
            [cpuload_id, "CpuUtilization1sec"],
            [volt_id, "VoltValue"],
]

reqPDU = pMod.GetRequestPDU()
pMod.apiPDU.setDefaults(reqPDU)
pMod.apiPDU.setVarBinds(
    reqPDU, ((time_id, pMod.Null('')),
             (temp_id, pMod.Null('')),
             (cpuload_id, pMod.Null('')),
             #(fanspeed_id, pMod.Null('')),
             )
)

reqMsg = pMod.Message()
pMod.apiMessage.setDefaults(reqMsg)
pMod.apiMessage.setCommunity(reqMsg, 'public')
pMod.apiMessage.setPDU(reqMsg, reqPDU)

startedAt = time()

def cbTimerFun(timeNow):
    if timeNow - startedAt > 3:
        raise Exception("Request timed out")

def cbRecvFun(transportDispatcher, transportDomain, tranportAddress, wholeMsg, reqPDU=reqPDU):
    while wholeMsg:
        rspMsg, wholeMsg = decoder.decode(wholeMsg, asn1Spec=pMod.Message())
        rspPDU = pMod.apiMessage.getPDU(rspMsg)
        if pMod.apiPDU.getRequestID(reqPDU) == pMod.apiPDU.getRequestID(rspPDU):
            errorStatus = pMod.apiPDU.getErrorStatus(rspPDU)
            if errorStatus:
                print(errorStatus.prettyPrint())
            else:
                for oid, val in pMod.apiPDU.getVarBinds(rspPDU):
                    for x in oid_list:
                        if x[0] == oid.prettyPrint():
                            print('%s = %s' % (x[1], val.prettyPrint()))
            transportDispatcher.jobFinished(1)
    return wholeMsg

for i in range(5000):
    transportDispatcher = AsyncoreDispatcher()

    transportDispatcher.registerRecvCbFun(cbRecvFun)
    transportDispatcher.registerTimerCbFun(cbTimerFun)

    transportDispatcher.registerTransport(
        udp.domainName, udp.UdpSocketTransport().openClientMode()
    )

    transportDispatcher.sendMessage(
        encoder.encode(reqMsg), udp.domainName, ('localhost', 161)
    )

    transportDispatcher.jobStarted(1)

    transportDispatcher.runDispatcher()

    transportDispatcher.closeDispatcher()
