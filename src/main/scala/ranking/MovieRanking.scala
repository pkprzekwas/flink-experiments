package ranking

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.sinks.CsvTableSink


case class RatingRecord(userId: Int, movieId: Int, rating: Double, timestamp: Long)

case class MovieRecord(id: Int, title: String)

object MovieRanking {
  def randomString(length: Int) = {
    val r = new scala.util.Random
    val sb = new StringBuilder
    for (i <- 1 to length) {
      sb.append(r.nextPrintableChar)
    }
    sb.toString
  }

  def main (args: Array[String] ): Unit = {
    val dataPath = "s3://pprzekwa-data/the-movie-dataset/"
    lazy val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    lazy val tEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)

      val ratings = env
    .readCsvFile[RatingRecord] (
    s"$dataPath/ratings.csv",
    ignoreFirstLine = true
    )
    tEnv.registerDataSet ("ratings", ratings,
    'userId, 'movieId, 'rating, 'timestamp)

    val movies = env
    .readCsvFile[MovieRecord] (
    s"$dataPath/movies_metadata.csv",
    quoteCharacter = '"',
    ignoreFirstLine = true,
    lenient = true,
    includedFields = Array (5, 8)
    ).toTable (tEnv, 'id, 'title)

    val globalAverageMovieRate = ratings
    .toTable (tEnv, 'userId, 'movieId, 'rating, 'timestamp)
    .select ('rating.avg)
    .toDataSet[Double]
    .collect ()
    .head
    val minVoteNumToConsider = 25000

    val ranking = tEnv.sqlQuery (
    s"""SELECT
         |  CAST(movieId AS INT) AS movieId,
         |  v AS numberOfVotes,
         |  R AS averageRate,
         |  (v/(v+m)) * R + (m /(v+m)) * $globalAverageMovieRate AS rankingRate
         |FROM (
         |  SELECT
         |    movieId,
         |    CAST($minVoteNumToConsider AS DOUBLE) AS m,
         |    CAST(COUNT(*) AS DOUBLE) AS v,
         |    AVG(rating) AS R
         |  FROM ratings
         |  GROUP BY movieId
         |) sub
      """.stripMargin)

    val moviesRatingsJoin = movies
    .join (
    ranking,
    'movieId === 'id)
    .orderBy ('rankingRate.desc)
    .fetch (100)

    val fileName = randomString(10)

    val sink = new CsvTableSink (
    s"s3://pprzekwa-data/the-movie-dataset/$fileName",
    "\t",
    1,
    WriteMode.OVERWRITE)
    moviesRatingsJoin.writeToSink (sink)

    env.execute ()
  }
}
