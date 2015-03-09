package kaltura.analytics.test.session

object SessionManager
{
     var _lastSessionIndex = 0

     def getSession() : Long =
     {
          _lastSessionIndex = _lastSessionIndex + 1
          _lastSessionIndex
     }
}
