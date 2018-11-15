package com.datamantra.producer

import java.util.Properties

import com.datamantra.loggenerator.Settings
import com.datamantra.model.ApacheAccessLogCombined
import com.datamantra.outputchannel.OutputChannel
import loganalysis.avro.ApacheLogEvents
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
/**
 * Created by kafka on 12/11/18.
 */
class KafkaChannel(settings: Settings) extends  OutputChannel{


  val avroProps = new Properties()
  avroProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, settings.producerSetting.bootstrapServer)
  avroProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, settings.producerSetting.keySerializer)
  avroProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, settings.producerSetting.valueSerializer)
  avroProps.put("schema.registry.url", settings.producerSetting.schemaRegistryUrl)

  val avroProducer = new KafkaProducer[String, ApacheLogEvents](avroProps)


  def process(event: Object): Unit = {

    val apacheRecord = event.asInstanceOf[ApacheAccessLogCombined]

    val apacheLogEvent = new ApacheLogEvents
    try {
      apacheLogEvent.setIpv4(apacheRecord.ipAddress)
      apacheLogEvent.setClientId(apacheRecord.clientId)
      apacheLogEvent.setTimestamp(apacheRecord.dateTime)
      apacheLogEvent.setRequestUri(apacheRecord.requestURI)
      apacheLogEvent.setStatus(apacheRecord.responseCode)
      apacheLogEvent.setContentSize(apacheRecord.contentSize)
      apacheLogEvent.setReferrer(apacheRecord.referrer)
      apacheLogEvent.setUseragent(apacheRecord.useragent)
      val producerRecord = new ProducerRecord[String, ApacheLogEvents](settings.producerSetting.topic, apacheLogEvent)
      avroProducer.send(producerRecord)
      System.out.println("Complete")
    }
    catch {
      case ex: Exception => {
        ex.printStackTrace(System.out)
      }
    } finally {
      //producer.close
    }

  }
}
