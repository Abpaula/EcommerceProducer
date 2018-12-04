package com.datamantra.loggenerator

import java.util.{Calendar, Date}

import com.datamantra.model.ApacheAccessLogCombined
import com.datamantra.outputchannel.OutputChannel
import org.apache.log4j.Logger


import scala.collection.mutable.ArrayBuffer
import scala.util.Random
import scala.util.control.Breaks._

/**
 * Created by kafka on 11/11/18.
 */
class Generator(settings: Settings) {


  val logger = Logger.getLogger(getClass.getName)

  val c: Calendar = Calendar.getInstance
  c.setTime(new Date)
  c.add(Calendar.DATE, 1)
  val d = c.getTime

  def eventGenerate(outputChannel: OutputChannel) = {

    val curr: Date = new Date
    var n_clicks_per_hour: Array[Int] = new Array[Int](24)
    var avg_time_between_clicks: Array[Double] = new Array[Double](24)
    var status: Array[Int] = Array[Int](200, 200, 200, 200, 200, 200, 200, 400, 404, 500)

    for(hour <- 0 to 23) {
      n_clicks_per_hour(hour) = Math.max(1, Math.floor(0.5 + settings.evenCount.toDouble *
        (settings.tot_weight_per_hour(hour).toDouble)/settings.tot_weight_per_day.toDouble).toInt)
      avg_time_between_clicks(hour) = (3600.toDouble / n_clicks_per_hour(hour)) * 1000
      logger.info(" clicks: " + n_clicks_per_hour(hour) + " clickstime: " + avg_time_between_clicks(hour))
    }

    val rand: Random = new Random(curr.getTime)

    val time_of_day_in_sec: Double = 0.0
    val hour: Int = 0
    var clicks_left: Int = 0
    //var ip4: String = ""
    var referrer: String = ""
    var user_agent: String = ""

    var month_abbr: Array[String] = Array[String]("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")

    val buffer = ArrayBuffer[String]()

    while(true) {
      val currdate: Date = new Date
      if (currdate.getTime > d.getTime) break //todo: break is not supported
      val h: Int = currdate.getHours

      val delay: Long = avg_time_between_clicks(h).toLong

      Thread.sleep(delay)
      val day: Int = currdate.getDate
      val month: Int = currdate.getMonth
      val year: Int = currdate.getYear + 1900

      // Pick random number for given hour, then look up country in cum weights, then pick random row for IP
      val r = 1 + rand.nextInt(settings.tot_weight_per_hour(hour))

      var ctry = 0
      while (r > settings.cum_hourly_weight_by_ctry(hour)(ctry)) {
        ctry += 1; ctry
      }


      val i = rand.nextInt(settings.tot_ips_by_ctry(ctry))


      var ipv4: String = "%d.%d.%d.%d".format(settings.ipA_by_ctry(i)(ctry), settings.ipB_by_ctry(i)(ctry), 2 + rand.nextInt(249), 2 + rand.nextInt(249))

      clicks_left = 1 + rand.nextInt(settings.maxClicksPerUser)
      referrer = settings.referrers(rand.nextInt(settings.n_referrers))
      user_agent = settings.user_agents(rand.nextInt(settings.n_user_agents))

      val timestamp: String =  "%02d:%02d:%02d".format(currdate.getHours, currdate.getMinutes, currdate.getSeconds)
      val output: String = "%s - %d [%02d/%3s/%4d:%8s -0500] \"%s\" %d %d \"%s\" \"%s\"\n".format( ipv4, i, day, month_abbr(month), year, timestamp, settings.requests(rand.nextInt(settings.n_requests)), status(rand.nextInt(10)), rand.nextInt(4096), referrer, user_agent)

      val datetime = f"[$day%02d/${month_abbr(month)}%3s/$year%4d:$timestamp%8s -0500]"

      val apacheLogEvent = ApacheAccessLogCombined(ipv4, "-", i.toString, datetime, settings.requests(rand.nextInt(settings.n_requests)), status(rand.nextInt(10)), rand.nextInt(4096).toLong, referrer, user_agent)
      logger.info("Output:" + output)
      outputChannel.process(apacheLogEvent)

      }


    }

  }

