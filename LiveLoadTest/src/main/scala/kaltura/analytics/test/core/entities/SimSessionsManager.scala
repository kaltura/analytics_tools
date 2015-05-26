package kaltura.analytics.test.core.entities

import kaltura.analytics.test.sim.SimLoadManager
import kaltura.analytics.test.util.SimIP

/**
 * Created by didi on 5/6/15.
 */
class SimSessionsManager( val _locationCode: String, val _eventType: Int )
{
     var _sessions: Vector[SimSession] = Vector.empty

     private def addSessions( nNewSessions: Int )
     {
          for ( sessionNo <- 0 until nNewSessions )
          {
               _sessions = _sessions :+ new SimSession()
          }
     }

     private def removeSessions( nRemoveSessions: Int )
     {
          _sessions = _sessions.drop(nRemoveSessions)
     }

     private def updateSessionsPoolSize( nSessions: Int )
     {
          val sessionsPoolSize = _sessions.length

          if ( sessionsPoolSize < nSessions )
          {
               val nNewSessions = nSessions - sessionsPoolSize
               addSessions(nNewSessions)
          }
          else if ( sessionsPoolSize > nSessions )
          {
               val nRemoveSessions = sessionsPoolSize - nSessions
               removeSessions(nRemoveSessions)
          }
     }

     def generateTimeEvents( time: Long, partnerId: Int, entryId: String, referrerId: String )
     {
          val simTimeSessionPoolSize = SimLoadManager.getTimeOpenSession(time, _eventType)

          updateSessionsPoolSize(simTimeSessionPoolSize)

          _sessions.foreach(x => x.generateTimeEvents(time, partnerId, entryId, referrerId, _locationCode, _eventType) )
     }
}
