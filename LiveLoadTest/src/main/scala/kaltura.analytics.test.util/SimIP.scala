package kaltura.analytics.test.util

import kaltura.analytics.test.env.SimParams

import scala.io.Source

object SimIP
{

     var _ipsList: Vector[String] = Vector.empty

     for( ipString <- Source.fromFile(SimParams._ipsListFileName).getLines() )
     {
          _ipsList = _ipsList :+ ipString.trim
     }

     def locationIdToIp( locationNo: Int ): String =
     {
          assert(locationNo < _ipsList.size )

          _ipsList(locationNo)
     }
}
