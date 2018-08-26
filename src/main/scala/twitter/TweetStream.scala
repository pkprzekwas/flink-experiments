package twitter

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.streaming.api.windowing.time.Time

import helpers.Context

case class Tweet(id: Long, user: User, text: String, source: String, place: Place, lang: String)
case class User(id: Long, name: String, location: String, lang: String)
case class Place(coordinates: String, country: String, countryCode: String, name: String, placeType: String)

object TweetStream extends App with Context {
  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "localhost:9092")
  properties.setProperty("zookeeper.connect", "localhost:2181")
  properties.setProperty("group.id", "twitter-stream")

  val twitterStream = sEnv
    .addSource(new FlinkKafkaConsumer08[String](
      "twitter_stream", new SimpleStringSchema(), properties)
    )

  val parsedTwitterStream = twitterStream
    .map(tweetJson => {
      val jsonParser = new ObjectMapper()
      val tweetNode = jsonParser.readValue(tweetJson, classOf[JsonNode])
      val tweet = TweetParser.parse(tweetNode)
      tweet
    })

  val twitterStreamAsString = parsedTwitterStream
    .map { tweet =>
      s"""
        ID: ${tweet.user.id}
        USER: ${tweet.user.name}
        LOCATION: ${tweet.user.location}
        LANG: ${tweet.user.lang}
        TEXT: ${tweet.text}
        SOURCE: ${tweet.source}
        PLACE: ${tweet.place}
      """
    }

  val twitterStreamsSumInFiveSeconds = twitterStream
    .map { _ => ("Tweets from 5 seconds:", 1) }
    .keyBy(0)
    .timeWindow(Time.seconds(5))
    .sum(1)
    .map { t => s"${t._1} ${t._2}"}

  twitterStreamsSumInFiveSeconds.print()

  sEnv.execute("Twitter stream")
}

