package twitter

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.flink.streaming.api.windowing.time.Time

case class Tweet(id: Long, user: User, text: String, source: String, place: Place)
case class User(id: Long, name: String, location: String, lang: String)
case class Place(coordinates: String, country: String, countryCode: String, name: String, placeType: String)

object TwitterStream extends App {

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  val properties = new Properties()

  properties.setProperty("bootstrap.servers", "localhost:9092")
  properties.setProperty("zookeeper.connect", "localhost:2181")
  properties.setProperty("group.id", "twitter-stream")

  lazy val jsonParser = new ObjectMapper()

  val twitterStream = env
    .addSource(new FlinkKafkaConsumer08[String]("twitter_stream", new SimpleStringSchema(), properties))

  val parsedTwitterStream = twitterStream
    .map(tweetJson => {
      val tweetNode = jsonParser.readValue(tweetJson, classOf[JsonNode])
      val tweet = parseTweet(tweetNode)
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
    .print()

  env.execute("Twitter stream")

  def getOrNone(obj: JsonNode, attr: String): String = {
    if (obj.has(attr)) obj.get(attr).asText() else "null"
  }

  def parseTweet(tweet: JsonNode): Tweet = {
    val tweetId = tweet.get("id").asLong()
    val text = tweet.get("text").asText()
    val lang = tweet.get("lang").asText()
    val source = tweet.get("source").asText()

    val userNode = tweet.get("user")
    val placeNode = tweet.get("place")

    val user = parseUser(userNode)
    val place = parsePlace(placeNode)

    Tweet(tweetId, user, text, source, place)
  }

  def parseUser(user: JsonNode): User = {
    val userId = user.get("id").asLong()
    val name = user.get("name").asText()
    val location = if (user.get("location").asText() != "null")
      user.get("location").asText().replace("\"", "")
    else
      "Unknown"
    val lang = user.get("lang").asText()

    User(userId, name, location, lang)
  }

  def parsePlace(place: JsonNode): Place = {
    val coordinates = getOrNone(place, "coordinates")
    val country = getOrNone(place, "country")
    val countryCode = getOrNone(place, "country_code")
    val name = getOrNone(place, "name")
    val placeType = getOrNone(place, "place_type")

    Place(coordinates, country, countryCode, name, placeType)
  }
}

