package kaltura.analytics.test.core.entities

import kaltura.analytics.test.env.SimParams

class SimPartner( partnerNo: Int, val _nEntries: Int, nReferrers: Int, nLocations: Int )
{

     val _partnerId =  if ( SimParams._partner.isEmpty )
          partnerNo + SimParams._partnerStartId
     else
          SimParams._partner.toInt

     val _partnerCode = if ( SimParams._partner.isEmpty )
          "prt" + _partnerId.toString
     else
          SimParams._partner

     val _entries = new Array[SimEntry](_nEntries)

     for ( entryNo <- 0 until _nEntries )
     {
          _entries(entryNo) = new SimEntry(_partnerCode, entryNo, nReferrers, nLocations)
     }

     def getEntries(): Array[String] =
     {
          var entriesIds: Array[String] = Array()

          for ( entry <- _entries )
          {
               val entryId = entry.getId()

               entriesIds = entriesIds :+ entryId
          }

          entriesIds
     }

     def generateTimeEvents( time: Long )
     {
          for ( entryNo <- 0 until _nEntries )
          {
               _entries(entryNo).generateTimeEvents(time, _partnerId)
          }
     }
}
