package com.xinze.hotItems

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

/**
  * Created by xiaoze on 2020-01-16-15:49
  */
object KafkaProducer
{

  def main(args: Array[String]): Unit =
  {
    writeToKafka("hotitems")
  }


  def writeToKafka(topic: String): Unit = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "hadooop102:9092")
    properties.setProperty("key.Serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.Serializer","org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("auto.offset.reset", "latest")

    val producer: KafkaProducer[String, String] = new KafkaProducer[String,String](properties)
    val bufferSource = io.Source.fromFile("D:\\ZxZ\\BigData\\AllProject\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
    for(line <- bufferSource.getLines())
      {
        val record: ProducerRecord[String, String] = new ProducerRecord[String,String](topic,line)
        producer.send(record)
      }
    producer.close()
  }
}
