package kaltura.analytics.test.util

/**
 * Created by didi on 5/19/15.
 */
object StringUtil
{
     def createInListWithApostrophe(strings: Array[String]): String =
     {
          val inList: StringBuilder = new StringBuilder(strings.mkString("','"))

          inList.insert(0, '\'')
          inList.append('\'')

          inList.toString()
     }

     def createInList(strings: Array[String]): String =
     {
          strings.mkString(",")
     }
}
