package com.datamantra.loggenerator

import com.datamantra.producer.ProducerSettings
import com.typesafe.config.Config
import org.apache.log4j.Logger

import scala.collection.mutable.HashMap
import scala.io.Source

/**
 * Created by kafka on 11/11/18.
 */
class Settings(config: Config) {

  val logger = Logger.getLogger(getClass.getName)

  val requestsFile = config.getString("requests")
  val referrersFile = config.getString("referrers")
  val ipaddressFile = config.getString("ipaddresses")
  val userAgentsFile = config.getString("user.agents")
  val evenCount = config.getString("event.count").toInt
  val maxFileLines = config.getString("max.file.lines").toInt
  val noOfCountries = config.getString("n.countries").toInt
  val maxIpsPerCountry = config.getString("max.ips.per.country").toInt
  val maxClicksPerUser = config.getString("max.clicks.per.user").toInt

  val producerSetting = ProducerSettings(
        config.getString("kafka.bootstrap.servers"),
        config.getString("kafka.key.serializer"),
        config.getString("kafka.value.serializer"),
        config.getString("kafka.acks"),
        config.getString("kafka.retries"),
        config.getString("kafka.topic"),
        config.getString("kafka.schema.registry.url"),
        config.getString("kafka.schema.file")
      )

  val ipA_by_ctry = Array.ofDim[Int](maxIpsPerCountry,noOfCountries)
  val ipB_by_ctry = Array.ofDim[Int](maxIpsPerCountry,noOfCountries)
  val tot_ips_by_ctry: Array[Int] = new Array[Int](noOfCountries)
  val cum_hourly_weight_by_ctry = Array.ofDim[Int](24,noOfCountries)
  val tot_weight_per_hour: Array[Int] = new Array[Int](24)
  var tot_weight_per_day = 0


  val ctry_ind: HashMap[String, String] = new HashMap[String, String]
  var n_requests: Int = 0
  var n_referrers: Int = 0
  var n_user_agents: Int = 0

  val requests: Array[String] = new Array[String](maxFileLines)
  val referrers: Array[String] = new Array[String](maxFileLines)
  val user_agents: Array[String] = new Array[String](maxFileLines)

  val outputChannel = "kafka"



  try {
    logger.info("Begin Intialization")
    var ctry_abbr: Array[String] = Array[String]("CH", "US", "IN", "JP", "BR", "DE", "RU", "ID", "GB", "FR", "NG", "MX", "KR", "IR", "TR", "IT", "PH", "VN", "ES", "PK")
    for (i <- 0 to noOfCountries - 1) {
      tot_ips_by_ctry(i) = 0
      ctry_ind.put(ctry_abbr(i), String.valueOf(i))
    }

    for (i <- 0 to maxIpsPerCountry - 1) {
      for (j <- 0 to noOfCountries - 1) {
        ipA_by_ctry(i)(j) = 0
        ipB_by_ctry(i)(j) = 0
      }
    }
    initCountryIP
    initRequests
    initReferrers
    initUserAgents
    weight


  }
  catch {
    case e: Exception => throw new RuntimeException(s"Initialization failed!: $e")
  }

  def initCountryIP: Unit = {
    for (line <- Source.fromFile(ipaddressFile).getLines()) {
      val fields = line.split(" ")
      if (fields.length == 2) {
        val i_ctry_s: String = ctry_ind.getOrElse(fields(1), null)
        if (i_ctry_s != null) {
          val i_ctry = i_ctry_s.toInt
          val octets: Array[String] = fields(0).split("\\.")
          if (tot_ips_by_ctry(i_ctry) < maxIpsPerCountry) {
            ipA_by_ctry(tot_ips_by_ctry(i_ctry))(i_ctry) = octets(0).toInt
            ipB_by_ctry(tot_ips_by_ctry(i_ctry))(i_ctry) = octets(1).toInt
            tot_ips_by_ctry(i_ctry) += 1
          }
        }
      }
    }
  }


  def initRequests: Unit = {

    logger.info("Begin initRequest")
    var i = 0
    for (line <- Source.fromFile(requestsFile).getLines()) {
      requests(i) = line
      i += 1
    }
    n_requests = i

    logger.debug("Lines read= " + n_requests);
    for (i <- 0 to n_requests -1)
      logger.info("Requests line " + i + ": " + requests(i));

  }


  def initReferrers: Unit ={
    var i = 0
    for (line <- Source.fromFile(referrersFile).getLines()) {
      logger.debug(line)
      referrers(i) = line
      i += 1
    }
    n_referrers = i

    logger.debug("Lines read= " + n_requests);
    for (i <- 0 to n_requests -1)
      logger.debug("Referrers line " + i + ": " + requests(i));

  }

  def initUserAgents: Unit ={
    var i = 0
    for (line <- Source.fromFile(userAgentsFile).getLines()) {
      user_agents(i) = line
      i += 1
    }
    n_user_agents = i

    logger.debug("Lines read= " + n_requests);
    for (i <- 0 to n_requests -1)
      logger.debug("User agents line " + i + ": " + requests(i));

  }

  def weight: Unit= {

    var ctry_pct: Array[Int] = Array[Int](31, 13, 7, 5, 5, 4, 4, 3, 3, 3, 3, 3, 3, 3, 3, 2, 2, 1, 1, 1)
    var hourly_weight: Array[Int] = Array[Int](4, 3, 1, 1, 1, 1, 2, 2, 2, 2, 2, 2, 3, 3, 2, 2, 3, 4, 6, 8, 12, 12, 12, 10)
    var ctry_time_diff: Array[Int] = Array[Int](13, 0, 11, 14, 2, 7, 9, 12, 6, 7, 6, 0, 14, 10, 8, 7, 13, 12, 7, 10)
    var hourly_weight_by_ctry = Array.ofDim[Int](24,noOfCountries)


    for( hour <- 0 to 23){
      for(ctry <- 0 to noOfCountries - 1){
        val local_hour = (hour + ctry_time_diff(ctry)) % 24
        hourly_weight_by_ctry(hour)(ctry) = hourly_weight(local_hour) * ctry_pct(ctry)
      }
    }

    for( hour <- 0 to 23){
      var sum = 0
      for(ctry <- 0 to noOfCountries - 1){
        sum += hourly_weight_by_ctry(hour)(ctry)
        cum_hourly_weight_by_ctry(hour)(ctry) = sum
      }
      tot_weight_per_hour(hour) = sum
      tot_weight_per_day += sum;
    }
  }

}
