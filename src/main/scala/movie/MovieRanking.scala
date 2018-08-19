package movie

import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.sinks.CsvTableSink


case class RatingRecord(userId: Int, movieId: Int,
                        rating: Double, timestamp: Long)

case class MovieRecord(id: Int, title: String)

object MovieRanking extends App {
  val dataPath = "/tmp/the-movies-dataset"

  val env = ExecutionEnvironment.getExecutionEnvironment
  val tEnv = TableEnvironment.getTableEnvironment(env)

  val ratings = env
    .readCsvFile[RatingRecord](
    s"$dataPath/ratings.csv",
    ignoreFirstLine = true
  )
  tEnv.registerDataSet("ratings", ratings,
    'userId, 'movieId, 'rating, 'timestamp)

  val globalAverageMovieRate = ratings
    .toTable(tEnv, 'userId, 'movieId, 'rating, 'timestamp)
    .select('rating.avg)
    .toDataSet[Double]
    .collect()
    .head
  val minVoteNumToConsider = 25000

  val ranking = tEnv.sqlQuery(
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

  val movies = env
    .readCsvFile[MovieRecord](
      s"$dataPath/movies_metadata.csv",
      quoteCharacter = '"',
      ignoreFirstLine = true,
      lenient = true,
      includedFields = Array(5, 8)
  ).toTable(tEnv, 'id, 'title)

  val result = ranking
    .join(movies, 'movieId === 'id)
    .orderBy('rankingRate.desc)
    .fetch(100)

  val sink = new CsvTableSink(
    "/tmp/fl-out",
    "\t",
    1,
    WriteMode.OVERWRITE)
  result.writeToSink(sink)

  env.execute()
}