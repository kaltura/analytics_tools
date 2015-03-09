package kaltura.analytics.test

import kaltura.analytics.test.core.EventGenerator
import kaltura.analytics.test.env.SimParams
import kaltura.analytics.test.util.CreateIpStringFromLong

import scala.io.Source

object Runner
{
     def main(args: Array[String]): Unit =
     {
//          CreateIpStringFromLong.create()
//          sys.exit(0)

          if ( args.length == 0 )
               sys.exit(1)

          if ( args.length > 3 )
          {
               SimParams._checkConsistency = (args(3).length > 0)
               SimParams._cassandraAddress = args(3)
          }

          if ( args.length > 2 )
               SimParams._nginxURL = args(2)

          if ( args.length > 1 )
               SimParams._ipsListFileName = args(1)

          println(args(0))

          EventGenerator.run(Source
               .fromFile(args(0) )
               .getLines)
     }
}
