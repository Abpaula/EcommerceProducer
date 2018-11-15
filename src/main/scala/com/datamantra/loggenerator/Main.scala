package com.datamantra.loggenerator

import com.datamantra.producer.KafkaChannel

/**
 * Created by kafka on 11/11/18.
 */
object Main {


  def main(args: Array[String]) {

    val configParser = new ConfigParser()
    val settings = configParser.loadSettings
    val generator = new Generator(settings)
    settings.outputChannel match {
      case "kafka" => {
        val kafkaChannel = new KafkaChannel(settings)
        generator.eventGenerate(kafkaChannel)
      }
    }
  }
}
