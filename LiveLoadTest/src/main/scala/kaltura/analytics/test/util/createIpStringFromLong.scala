package kaltura.analytics.test.util

import java.io.{File, PrintWriter, FileWriter}

import scala.io.Source

object CreateIpStringFromLong
{
     var _ipsList: Vector[String] = Vector.empty

     def create( )
     {

          for( ipLongString <- Source.fromFile("/opt/ip_long.txt").getLines() )
          {
               _ipsList = _ipsList :+ IpLongToStringConverter.convert(ipLongString.trim.toLong)
          }

          val file: File = new File("/tmp/ipsNew.txt");
          val out: PrintWriter = new PrintWriter(new FileWriter(file));

          // Write each string in the array on a separate line

          for ( ip <- _ipsList )
          {
               out.println(ip);
          }

          out.close();
     }
}
