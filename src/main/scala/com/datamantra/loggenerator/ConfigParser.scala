package com.datamantra.loggenerator

import com.typesafe.config.{ConfigFactory, ConfigException}

/**
 * Created by kafka on 11/11/18.
 */
class ConfigParser {


  def loadSettings: Settings = {
    try {
      // load the generator.conf file
      val config = ConfigFactory.load("generator")
      val extractedConfig = config.getConfig("clickstream.generator")
      // validate the configuration against reference configuration file
      config.checkValid(ConfigFactory.defaultReference(), "clickstream.generator")
      new Settings(extractedConfig)

    } catch {
      case e: ConfigException => throw new RuntimeException(s"Configuration validation failed!: $e")
    }

  }

  def printSystemProperties() {


    val p = System.getProperties
    val keys = p.keys

    while (keys.hasMoreElements) {
      val k = keys.nextElement
      val v = p.get(k)
      println(k + ": " + v)
    }
  }

}


object  ConfigParserTesting {

  def main(args: Array[String]) {

    val configParser = new ConfigParser()
    configParser.loadSettings
   // configParser.printSystemProperties()
  }
}
