package kaltura.analytics.test.core.entities

import kaltura.analytics.test.env.SimParams


class SimEntry( partnerCode: String, entryNo: Int, val _nReferrers: Int, nLocations: Int )
{
     val _entryId = if ( SimParams._entries.isEmpty )
          partnerCode + "ent" + entryNo.toString
     else
          SimParams._entries(entryNo)

     val _referrers = new Array[SimReferrer](_nReferrers)

     for ( referrerNo <- 0 until _nReferrers )
     {
          _referrers(referrerNo) = new SimReferrer(_entryId, referrerNo, nLocations)
     }

     def getId(): String =
     {
          _entryId
     }

     def generateTimeEvents( time: Long, partnerId: Int )
     {
          for (referrerNo <- 0 until _nReferrers)
          {
               _referrers(referrerNo).generateTimeEvents(time, partnerId, _entryId)
          }
     }
}
