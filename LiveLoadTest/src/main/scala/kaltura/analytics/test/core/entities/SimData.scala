package kaltura.analytics.test.core.entities

/**
 * Created by didi on 9/10/14.
 */
class SimData( val _nPartners: Int, nEntries: Int, nReferrers: Int, nLocations: Int )
{
     val _partners = new Array[SimPartner](_nPartners)

     def getPartnersIds(): Array[String] =
     {
          var partnersIds: Array[String] = Array()

          for ( partner <- _partners )
          {
               val newPartnerId = partner._partnerCode

               partnersIds = partnersIds :+ newPartnerId
          }

          partnersIds
     }

     def getEntriesIds(): Array[String] =
     {
          var entriesIds: Array[String] = Array()

          for ( partner <- _partners )
          {
               val partnerEntries = partner.getEntries()

               val newEntriesIds = entriesIds ++ partnerEntries

               entriesIds = newEntriesIds
          }

          entriesIds
     }


     for ( partnerNo <- 0 until _nPartners )
     {
          _partners(partnerNo) = new SimPartner(partnerNo, nEntries, nReferrers, nLocations)
     }

     def generateTimeEvents( time: Long )
     {
          for ( partnerNo <- 0 until _nPartners )
          {
               _partners(partnerNo).generateTimeEvents(time)
          }
     }
}
