package com.datamantra.producer

import java.util.Properties

import com.datamantra.loggenerator.Settings
import com.datamantra.model.ApacheAccessLogCombined
import com.datamantra.outputchannel.OutputChannel
import com.twitter.bijection.Injection
import com.twitter.bijection.avro.GenericAvroCodecs
import io.confluent.kafka.schemaregistry.client.rest.RestService
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException
import org.apache.avro.Schema
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.Logger

import scala.io.Source

/**
 * Created by kafka on 12/11/18.
 */
class KafkaChannel(settings: Settings) extends  OutputChannel{


  val logger = Logger.getLogger(getClass.getName)

  val producer = new KafkaProducer[String, Array[Byte]](getProperites());

  val schema = getSchemaFromFile()

  val recordInjection: Injection[GenericRecord, Array[Byte]] = GenericAvroCodecs.toBinary(schema)


  def getProperites() = {
    val props = new Properties();
    props.put("bootstrap.servers", settings.producerSetting.bootstrapServer);
    props.put("key.serializer", settings.producerSetting.keySerializer);
    props.put("value.serializer", settings.producerSetting.valueSerializer);
    props
  }


  def getSchemaFromFile() = {

    val url = getClass.getResource(settings.producerSetting.schemaFile)
    val parser = new Schema.Parser
    parser.parse(Source.fromURL(url).mkString)

  }


  def registerSchema() = {
    try {
      val schemaRegistryURL: String = settings.producerSetting.schemaRegistryUrl
      val restService: RestService = new RestService(schemaRegistryURL)
      val subjectNameValue = settings.producerSetting.topic + "-value"
      restService.registerSchema(schema.toString, subjectNameValue)
    }
    catch {
      case e: RestClientException => {
          logger.info("RestClientException" + e)
      }
    }
  }


  def process(event: Object): Unit = {

    publishAvroGenericRecord(event)

  }


  def publishAvroGenericRecord(event: Object): Unit = {

    val apacheRecord = event.asInstanceOf[ApacheAccessLogCombined]
    try {

      val genericRecord = new GenericData.Record(schema);

      genericRecord.put("ipv4", apacheRecord.ipAddress)
      genericRecord.put("clientId", apacheRecord.clientId)
      genericRecord.put("timestamp", apacheRecord.dateTime)
      genericRecord.put("requestUri", apacheRecord.requestURI)
      genericRecord.put("status", apacheRecord.responseCode)
      genericRecord.put("contentSize", apacheRecord.contentSize)
      genericRecord.put("referrer", apacheRecord.referrer)
      genericRecord.put("useragent", apacheRecord.useragent)

      val bytes = recordInjection.apply(genericRecord);

      val producerRecord = new ProducerRecord[String, Array[Byte]](settings.producerSetting.topic, bytes)
      producer.send(producerRecord)

    } catch {
      case ex: Exception => {
        ex.printStackTrace(System.out)
      }
    } finally {
      //producer.close
    }

  }



  def getSchema() = {

    val schemaString = """{
                         "fields": [
                              {"name": "ipv4", "type": "string"},
                              {"name": "clientId", "type": "string"},
                              {"name": "timestamp", "type": "string"},
                              {"name": "requestUri", "type": "string"},
                              {"name": "status", "type": "int"},
                              {"name": "contentSize", "type": "long"},
                              {"name": "referrer", "type": "string"},
                              {"name": "useragent", "type": "string"}
                            ],
        "name": "EcommerceLogEvents",
        "type": "record"
    }""""

    schemaString
  }



}
