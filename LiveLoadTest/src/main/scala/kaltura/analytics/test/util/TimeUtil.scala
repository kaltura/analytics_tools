package kaltura.analytics.test.util

/**
 * Created by didi on 10/22/14.
 */
object TimeUtil
{
     def isHour( time: Long ): Boolean =
     {
          time % 3600000 == 0
     }

     def toHour( time: Long ): Long =
     {
          time / 3600000 * 3600000
     }
}
