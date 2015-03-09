package kaltura.analytics.test.core.entities

class SimReferrer( entryCode: String, referrerNo: Int, val _nLocations: Int )
{
     val _referrerId = entryCode + "ref" + referrerNo.toString

     val _locations = new Array[SimLocation](_nLocations)

     for ( locationNo <- 0 until _nLocations )
     {
          _locations(locationNo) = new SimLocation(locationNo)
     }

     def generateTimeEvents( time: Long, partnerId: Int, entryId: String )
     {
          for ( locationNo <- 0 until _nLocations )
          {
               _locations(locationNo).generateTimeEvents(time, partnerId, entryId, _referrerId)
          }
     }
}
