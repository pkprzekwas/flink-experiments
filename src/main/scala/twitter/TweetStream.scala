package twitter

import java.util.Properties

import helpers.Context
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction.AggregationType
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08

case class Tweet(id: Long, user: User, text: String, source: String, place: Place, lang: String, favorite_count: Int)
case class User(id: Long, name: String, location: String, lang: String)
case class Place(coordinates: String, country: String, countryCode: String, name: String, placeType: String)

object TweetStream extends App with Context {
  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "localhost:9092")
  properties.setProperty("zookeeper.connect", "localhost:2181")
  properties.setProperty("group.id", "twitter_stream")

  val dataStream = sEnv
    .addSource(new FlinkKafkaConsumer08[String](
      "twitter_stream", new SimpleStringSchema(), properties)
    )

  val dataStreamTransformed = dataStream
    .map(tweetJson => {
      val jsonParser = new ObjectMapper()
      val tweetNode = jsonParser.readValue(tweetJson, classOf[JsonNode])
      val tweet = TweetParser.parse(tweetNode)
      tweet
    })

  val resultDataStream = dataStreamTransformed
    .map{ tweet => (tweet.lang, 1) }
    .keyBy(_._1)
    .timeWindow(Time.seconds(10))
    .aggregate(AggregationType.SUM, 1)

  resultDataStream.print()

  sEnv.execute("Twitter stream")
}

