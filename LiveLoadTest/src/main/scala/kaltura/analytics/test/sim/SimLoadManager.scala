
package kaltura.analytics.test.sim

import kaltura.analytics.test.env.SimParams

object SimLoadManager
{
     val _minTimeResolution = SimParams._minTimeResolution

     val _simTimeCycle = SimParams._simTimeCycle

     var _iteration = 1

     def resetSessions()
     {
          _iteration = 1
     }

     def incrementSessions()
     {
          _iteration += 1
     }

     def baseSessions( eventType: Int ) =
     {
          val nSessions = eventType match
          {
               case 1 => SimParams._nBaseSessions
               case 2 => SimParams._nBaseDVRSessions
          }
          nSessions
     }

     def deltaSessions( eventType: Int ) =
     {
          val nSessions = eventType match
          {
               case 1 => SimParams._nMaxDeltaSessions
               case 2 => SimParams._nMaxDeltaDVRSessions
          }
          nSessions
     }

     def getTimeOpenSession( time: Long, eventType: Int ): Int =
     {
          val base = baseSessions(eventType)
          val delta = deltaSessions(eventType)

          // version 2
          var nSessions = base

          if ( SimParams._deltaStrategyCode == 1 )
          {
               if ( _iteration % 2 == 0 )
                    nSessions += delta
               else
                    nSessions -= delta
          }



          // version 1
//          val timeElapsedInCycle = time % _simTimeCycle
//
//          val cycleDegrees = timeElapsedInCycle.toFloat / _simTimeCycle * 360.0f
//
//          val cycleRadians = math.toRadians(cycleDegrees)
//
//          val deltaSessions = math.sin(cycleRadians) * SimParams._nMaxDeltaSessions
//
//          val nSessions = SimParams._nBaseSessions + deltaSessions.toInt

          nSessions
     }
}
