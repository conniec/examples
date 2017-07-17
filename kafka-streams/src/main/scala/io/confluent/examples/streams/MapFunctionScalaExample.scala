/**
  * Copyright 2016 Confluent Inc.
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package io.confluent.examples.streams

import java.util.Properties

import org.apache.kafka.common.serialization._
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream.{GlobalKTable, KStream, KStreamBuilder}
import org.apache.kafka.streams.state.QueryableStoreTypes

/**
  * Demonstrates how to perform simple, state-less transformations via map functions.
  * Same as [[MapFunctionLambdaExample]] but in Scala.
  *
  * Use cases include e.g. basic data sanitization, data anonymization by obfuscating sensitive data
  * fields (such as personally identifiable information aka PII).  This specific example reads
  * incoming text lines and converts each text line to all-uppercase.
  *
  * Requires a version of Scala that supports Java 8 and SAM / Java lambda (e.g. Scala 2.11 with
  * `-Xexperimental` compiler flag, or 2.12).
  *
  * HOW TO RUN THIS EXAMPLE
  *
  * 1) Start Zookeeper and Kafka.
  * Please refer to <a href='http://docs.confluent.io/current/quickstart.html#quickstart'>QuickStart</a>.
  *
  * 2) Create the input and output topics used by this example.
  *
  * {{{
  * $ bin/kafka-topics --create --topic TextLinesTopic --zookeeper localhost:2181 --partitions 1 --replication-factor 1
  * $ bin/kafka-topics --create --topic UppercasedTextLinesTopic --zookeeper localhost:2181 --partitions 1 --replication-factor 1
  * $ bin/kafka-topics --create --topic OriginalAndUppercasedTopic --zookeeper localhost:2181 --partitions 1 --replication-factor 1
  * }}}
  *
  * Note: The above commands are for the Confluent Platform. For Apache Kafka it should be
  * `bin/kafka-topics.sh ...`.
  *
  * 3) Start this example application either in your IDE or on the command line.
  *
  * If via the command line please refer to
  * <a href='https://github.com/confluentinc/examples/tree/3.2.x/kafka-streams#packaging-and-running'>Packaging</a>.
  * Once packaged you can then run:
  *
  * {{{
  * $ java -cp target/streams-examples-3.2.2-standalone.jar io.confluent.examples.streams.MapFunctionScalaExample
  * }
  * }}}
  *
  * 4) Write some input data to the source topics (e.g. via `kafka-console-producer`.  The already
  * running example application (step 3) will automatically process this input data and write the
  * results to the output topics.
  *
  * {{{
  * # Start the console producer.  You can then enter input data by writing some line of text,
  * # followed by ENTER:
  * #
  * #   hello kafka streams<ENTER>
  * #   all streams lead to kafka<ENTER>
  * #
  * # Every line you enter will become the value of a single Kafka message.
  * $ bin/kafka-console-producer --broker-list localhost:9092 --topic TextLinesTopic
  * }}}
  *
  * 5) Inspect the resulting data in the output topics, e.g. via `kafka-console-consumer`.
  *
  * {{{
  * $ bin/kafka-console-consumer --new-consumer --bootstrap-server localhost:9092 --topic UppercasedTextLinesTopic --from-beginning
  * $ bin/kafka-console-consumer --new-consumer --bootstrap-server localhost:9092 --topic OriginalAndUppercasedTopic --from-beginning
  * }}}
  *
  * You should see output data similar to:
  * {{{
  * HELLO KAFKA STREAMS
  * ALL STREAMS LEAD TO KAFKA
  * }}}
  *
  * 6) Once you're done with your experiments, you can stop this example via `Ctrl-C`.  If needed,
  * also stop the Kafka broker (`Ctrl-C`), and only then stop the ZooKeeper instance (`Ctrl-C`).
  */
object MapFunctionScalaExample {

  val BOOTSTRAP_SERVERS = "localhost:9092"
  val APPLICATION_ID = "map-function-scala-example-testing"
  val CONFIG_TOPIC = "StreamConfig3"


  def getApplicationId(topic: String): String = {
    s"$APPLICATION_ID:$topic"
  }

  def main(args: Array[String]) {

    val configBuilder = new KStreamBuilder

    val streamingConfig = {
      val settings = new Properties
      settings.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID)
      settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
      // Specify default (de)serializers for record keys and for record values.
      settings.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      settings.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String.getClass.getName)
      settings.put(StreamsConfig.STATE_CLEANUP_DELAY_MS_CONFIG, "600000")
      settings
    }

    val config: GlobalKTable[String, String] = configBuilder.globalTable(CONFIG_TOPIC, "test-store")

    val configStream = new KafkaStreams(configBuilder, streamingConfig)
    configStream.start()

    val view = configStream.store("test-store", QueryableStoreTypes.keyValueStore[String, String])
    println("Looking at config")
    val iterator = view.all()
    println("\n\n\n\n\n iterator created")

    // Create a buffer to store all the stream creation results
    //var streamsCreated = new mutable.ListBuffer[Try[Unit]]()

    while(iterator.hasNext()) {
      val nextValue = iterator.next()
      val topic = nextValue.key.toString
      val dest = nextValue.value.toString
      println(s"HAS NEXT $topic $dest \n\n\n\n\n\n")

      // Create an application id unique to that topic. Each application id maps to one topic and one transformation, and one KStreamBuilder
      val applicationId = getApplicationId(topic)
      println(s"Application id is $applicationId")
      val transformer = new StreamTransformer(topic, dest, applicationId)
      transformer.run()
    }
    configStream.close()
  }

}
