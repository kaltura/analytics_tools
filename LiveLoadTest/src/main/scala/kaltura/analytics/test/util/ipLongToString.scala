package kaltura.analytics.test.util

import scala.math.pow

/**
 * Created by didi on 1/13/15.
 */
object IpLongToStringConverter
{
     def convert( ipLong: Long ): String =
     {
          val ip0: Long = ipLong / pow(256,3).toLong

          val tmp0: Long = ipLong - (ip0 * pow(256,3).toLong )

          val ip1: Long = tmp0 / pow(256,2).toLong

          val tmp1: Long = tmp0 - (ip1 * pow(256,2).toLong )

          val ip2: Long = tmp1 / pow(256,1).toLong

          val tmp2: Long = tmp1 - (ip2 * pow(256,1).toLong )

          val ip3: Long = tmp2

          val ip: String = ip0.toString + "." + ip1.toString + "." + ip2.toString + "." + ip3.toString

          ip
     }
}
