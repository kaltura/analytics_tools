package kaltura.analytics.test.core


//import java.lang.System.currentTimeMillis

import java.io.PrintWriter
import java.lang.System._

import kaltura.analytics.test.com.{LiveEventsCassandraDriver, HttpWrapper}
import kaltura.analytics.test.core.entities.{SimData, SimPartner}
import kaltura.analytics.test.env.SimParams
import kaltura.analytics.test.sim.SimLoadManager
import kaltura.analytics.test.util.{Parser, TimeUtil}

import scala.util.control.Breaks._

object EventGenerator
{
     val connector: LiveEventsCassandraDriver = new LiveEventsCassandraDriver
     connector.init("Test Cluster", "kaltura_live", SimParams._cassandraAddress)

     def run( lines: Iterator[String] )
     {
//          connector.debugReadHourly(TimeUtil.toHour(currentTimeMillis) )
//          exit(0)

          val currentTime = currentTimeMillis

          val currentTimeMinResolution = currentTime / SimParams._minTimeResolution

          var nextStartTime = currentTimeMinResolution * SimParams._minTimeResolution

          println("start: " + nextStartTime)

          var simData = new SimData(0, 0, 0, 0)

          var partnersIds: Array[String] = Array.empty[String]
          var entriesIds: Array[String] = Array.empty[String]

          lines.foreach
          {
               line => val nSessions = Parser.processLine(line)

               breakable
               {
                    if ( nSessions == 0 )
                         break

                    if ( SimParams._resetSim )
                    {
                         SimParams._resetSim = false

                         simData = new SimData(SimParams._nPartners, SimParams._nEntries, SimParams._nReferrers, SimParams._nLocations)

                         partnersIds = simData.getPartnersIds
                         entriesIds = simData.getEntriesIds
                    }

                    var hourlyAliveEventsPerLocation = 0l
                    var hourlyPlaysEventsPerLocation = 0l

                    var lastTimeAliveEventsPerLocation = 0l

                    val recheckAggregates = true

                    //var forceHourlyCheck = false

                    var iterationCounter = 0l

                    val startTimeMinResolution = nextStartTime
                    val endTimeMinResolution = nextStartTime + nSessions * SimParams._minTimeResolution

                    nextStartTime = endTimeMinResolution

                    breakable
                    {
                         for (simTime <- startTimeMinResolution until endTimeMinResolution by SimParams._minTimeResolution)
                         {
                              print("time: ")
                              println(simTime)

                              simData.generateTimeEvents(simTime)

                              if (TimeUtil.isHour(simTime))
                              {
                                   if ( SimParams._checkConsistency )
                                        validateHourlyAggregations(partnersIds, entriesIds, simTime, hourlyAliveEventsPerLocation, hourlyPlaysEventsPerLocation)

                                   hourlyAliveEventsPerLocation = 0
                                   hourlyPlaysEventsPerLocation = 0
                              }

                              val timeAliveEventsPerLocation = SimLoadManager.getTimeOpenSession(simTime)
                              val timePlaysEventsPerLocation = math.max(0l, timeAliveEventsPerLocation - lastTimeAliveEventsPerLocation)

                              val actualTimeAliveEventsPerLocation = timeAliveEventsPerLocation - timePlaysEventsPerLocation
                              assert(actualTimeAliveEventsPerLocation >= 0)

                              lastTimeAliveEventsPerLocation = timeAliveEventsPerLocation

                              if ( SimParams._checkConsistency && recheckAggregates && ( iterationCounter % 30 == 0 ) )
                              {
                                   validateAggregations(entriesIds, simTime, actualTimeAliveEventsPerLocation, timePlaysEventsPerLocation)
                              }

                              hourlyAliveEventsPerLocation += actualTimeAliveEventsPerLocation
                              hourlyPlaysEventsPerLocation += timePlaysEventsPerLocation

                              val endIterationTime = currentTimeMillis

                              val nextSimTime = simTime + SimParams._minTimeResolution

                              val diffTime = nextSimTime - endIterationTime

                              if (diffTime > SimParams._timeReSyncThreshold)
                              {
                                   val sleepTime = diffTime - SimParams._timeReSyncRemainMargin
                                   Thread.sleep(sleepTime)
                              }

                              iterationCounter += 1

                              //                    if ( iterationCounter > 5 )
                              //                         forceHourlyCheck = true
                              //
                              //                    if ( forceHourlyCheck )
                              //                    {
                              //                         val nextHourTime = TimeUtil.toHour(simTime) + 3600000
                              //                         validateHourlyAggregations(entriesIds, nextHourTime, hourlyAliveEventsPerLocation, hourlyPlaysEventsPerLocation)
                              //                         break
                              //                    }

                              SimLoadManager.incrementSessions
                              if (TimeUtil.isHour(simTime))
                                   SimLoadManager.resetSessions
                         }
                    }
               }
          }

          // todo: replace the following with thread pool wait
          if ( SimParams._checkConsistency )
               Thread.sleep(90000)

          val endTime = currentTimeMillis

          println("end: " + endTime)

          HttpWrapper.getInstance.close

          println("total sends: " + HttpWrapper.getInstance.sendCounter )
     }

     def validateHourlyAggregations( partnersIds: Array[String], entriesIds: Array[String], endHourTime: Long, hourlyAliveEventsPerLocation: Long, hourlyPlaysEventsPerLocation: Long )
     {
          val hourTime = 3600000
          val startHourTime = endHourTime - hourTime

          val hourlyAliveEventsPerReferrer = hourlyAliveEventsPerLocation * SimParams._nLocations

          val hourlyAliveEventsPerEntry = hourlyAliveEventsPerReferrer * SimParams._nReferrers

          val hourlyAliveEventsPerPartner = hourlyAliveEventsPerEntry * SimParams._nEntries


          val hourlyPlaysEventsPerReferrer = hourlyPlaysEventsPerLocation * SimParams._nLocations

          val hourlyPlaysEventsPerEntry = hourlyPlaysEventsPerReferrer * SimParams._nReferrers

          val hourlyPlaysEventsPerPartner = hourlyPlaysEventsPerEntry * SimParams._nEntries

          new Thread() {
               override def run = {

                    val sleepDelayMs = 180000
                    Thread.sleep(sleepDelayMs)

                    connector.validateHourlyPartnerLiveEvents(partnersIds, startHourTime, hourlyAliveEventsPerPartner, hourlyPlaysEventsPerPartner)

                    connector.validateHourlyEntriesLiveEvents(entriesIds, startHourTime, hourlyAliveEventsPerEntry, hourlyPlaysEventsPerEntry)

                    connector.validateHourlyReferrersLiveEvents(entriesIds, startHourTime, hourlyAliveEventsPerReferrer, hourlyPlaysEventsPerPartner)
               }
          }.start()
     }

     def validateAggregations( entriesIds: Array[String], time: Long, aliveEventsPerLocation: Long, playsEventsPerLocation: Long )
     {
          val aliveEventsPerReferrer = aliveEventsPerLocation * SimParams._nLocations

          val aliveEventsPerEntry = aliveEventsPerReferrer * SimParams._nReferrers


          val playsEventsPerReferrer = playsEventsPerLocation * SimParams._nLocations

          val playsEventsPerEntry = playsEventsPerReferrer * SimParams._nReferrers

          new Thread() {
               override def run = {

                    val sleepDelayMs = 120000
                    Thread.sleep(sleepDelayMs)

                    connector.validateEntriesLiveEvents(entriesIds, time, aliveEventsPerEntry, playsEventsPerEntry)
               }
          }.start()
     }
}
