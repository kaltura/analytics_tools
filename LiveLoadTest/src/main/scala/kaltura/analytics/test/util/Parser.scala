package kaltura.analytics.test.util

import java.util.StringTokenizer

import kaltura.analytics.test.core.EventGenerator
import kaltura.analytics.test.env.SimParams

object Parser
{
     def processLine( line: String ): Int =
     {
          var nSessions = 0

          val pairs = line.substring(1, line.length - 1).trim  // hack off braces

          val pairsTokens: StringTokenizer = new StringTokenizer(pairs, ",")

          while ( pairsTokens.hasMoreTokens )
          {
               val keyValuePair: String = pairsTokens.nextToken.trim

               val keyValueToken: StringTokenizer = new StringTokenizer(keyValuePair, "=")

               val key = keyValueToken.nextToken.trim
               val value = keyValueToken.nextToken.trim


               key match
               {
                    case "n_sessions" => nSessions = value.toInt

                    case "n_partners" =>

                         SimParams._nPartners = value.toInt
                         SimParams._resetSim = true

                    case "n_entries" =>

                         SimParams._nEntries = value.toInt
                         SimParams._resetSim = true

                    case "n_referrers" =>

                         SimParams._nReferrers = value.toInt
                         SimParams._resetSim = true

                    case "n_locations" =>

                         SimParams._nLocations = value.toInt
                         SimParams._resetSim = true

                    case "n_alive" => SimParams._nBaseSessions = value.toInt

                    case "n_alive_delta" => SimParams._nMaxDeltaSessions= value.toInt

                    case "bit_rate" => SimParams._bitRate = value.toInt

                    case "buffer_time" => SimParams._bufferTime = value.toInt

                    case "entries" =>

                         var nEntries = 0

                         SimParams._entries = Vector.empty

                         val entries = value.substring(1, value.length - 1).trim  // hack off braces

                         val entriesTokens: StringTokenizer = new StringTokenizer(entries, "|")

                         while ( entriesTokens.hasMoreTokens )
                         {
                              val entry: String = entriesTokens.nextToken.trim

                              SimParams._entries = SimParams._entries :+ entry

                              nEntries += 1
                         }

                         SimParams._nPartners = 1
                         SimParams._nEntries = nEntries

                        SimParams._resetSim = true

                    case "partner" => SimParams._partner = value

                    case _  => println("invalid input: %s", keyValuePair)

               }

          }

          nSessions
     }
}
