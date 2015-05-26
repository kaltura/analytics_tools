package kaltura.analytics.test.core.entities

import kaltura.analytics.test.env.SimParams
import kaltura.analytics.test.sim.SimLoadManager
import kaltura.analytics.test.util.SimIP

class SimLocation( val _locationNo: Int )
{
     val _locationCode = SimIP.locationIdToIp(_locationNo)

     var _sessionsManager: SimSessionsManager = new SimSessionsManager(_locationCode, 1)
     var _dvrSessionsManager: SimSessionsManager = new SimSessionsManager(_locationCode, 2)


     def generateTimeEvents( time: Long, partnerId: Int, entryId: String, referrerId: String )
     {
          _sessionsManager.generateTimeEvents(time, partnerId, entryId, referrerId)

          _dvrSessionsManager.generateTimeEvents(time, partnerId, entryId, referrerId)
     }
}
