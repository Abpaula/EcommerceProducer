package com.datamantra.producer

/**
 * Created by kafka on 13/11/18.
 */
case class ProducerSettings(
  bootstrapServer: String,
  keySerializer:String,
  valueSerializer:String,
  acks:String,
  retries:String,
  topic:String,
  schemaRegistryUrl:String,
  schemaFile:String
)
