package twitter

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode

object TweetParser {
  def parse(tweet: JsonNode): Tweet = {
    val tweetId = tweet.get("id").asLong()
    val text = tweet.get("text").asText()
    val lang = tweet.get("lang").asText()
    val source = tweet.get("source").asText()
    val favCnt = tweet.get("favorite_count").asInt()

    val userNode = tweet.get("user")
    val placeNode = tweet.get("place")

    val user = parseUser(userNode)
    val place = parsePlace(placeNode)

    Tweet(tweetId, user, text, source, place, lang, favCnt)
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
    val coordinates = _getOrNone(place, "coordinates")
    val country = _getOrNone(place, "country")
    val countryCode = _getOrNone(place, "country_code")
    val name = _getOrNone(place, "name")
    val placeType = _getOrNone(place, "place_type")

    Place(coordinates, country, countryCode, name, placeType)
  }

  def _getOrNone(obj: JsonNode, attr: String): String = {
    if (obj.has(attr)) obj.get(attr).asText() else "null"
  }
}
