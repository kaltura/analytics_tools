package kaltura.analytics.test.core.entities

import kaltura.analytics.test.com.HttpWrapper
import kaltura.analytics.test.session.SessionManager


class SimSession
{
     val _sessionId = SessionManager.getSession()

     var _eventIndex = 0

     val _httpWrapper = HttpWrapper.getInstance

     def generateTimeEvents( time: Long, partnerId: Int, entryId: String, referrerId: String, locationId: String )
     {
          _eventIndex = _eventIndex + 1

          _httpWrapper.send(time, partnerId.toString, entryId, referrerId, locationId, _eventIndex.toString, "1")
          _httpWrapper.send(time, partnerId.toString, entryId, referrerId, locationId, _eventIndex.toString, "2")
     }
}
