package kaltura.analytics.test.env

object SimParams
{
     val _verbose = false

     val _recheckFrequency = 1

     var _partner = ""

     var _cassandraAddress = "192.168.31.91:9160"

     var _deltaStrategyCode = 0

     var _checkConsistency = false

     var _ipsListFileName = "/opt/ips.txt"// "/home/didi/Workspace/Data/ips.txt"

     var _nginxURL = "http://192.168.11.133/api_v3/index.php"

     def _simTimeCycle = 3600000

     def _minTimeResolution = 10000

     def _partnerStartId = 100

     var _entries: Vector[String] = Vector.empty
     
     var _nPartners = 1 //10

     var _nEntries = 1 //10

     var _nReferrers = 1 //5

     var _nLocations = 1 //40

     var _nBaseSessions = 1 //5

     var _nMaxDeltaSessions = 0 // 3

     var _nBaseDVRSessions = 0 //5

     var _nMaxDeltaDVRSessions = 0 // 3

     var _bitRate = 100

     var _bufferTime = 0

     def _timeReSyncThreshold = 3000

     def _timeReSyncRemainMargin = 1000

     var _resetSim: Boolean = true

}
