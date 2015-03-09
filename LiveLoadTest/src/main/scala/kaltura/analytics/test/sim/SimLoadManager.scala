
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

     def getTimeOpenSession( time: Long ): Int =
     {

          // version 2
          var nSessions = 0
          if ( _iteration % 2 == 0 )
               nSessions = SimParams._nBaseSessions + SimParams._nMaxDeltaSessions
          else
               nSessions = SimParams._nBaseSessions - SimParams._nMaxDeltaSessions


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
