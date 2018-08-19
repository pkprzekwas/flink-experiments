package movielens

import org.apache.flink.api.scala._

object MovieFilter {

  final val dataPath = "/Users/pprzekwa/src/magisterka/datasets/ml-latest-small/movies.csv"

  case class Movie(id: String, title: String, genres: Array[String])

  private def getMovies(env: ExecutionEnvironment): DataSet[Movie] = {
    env.readTextFile(dataPath)
      .map {
        line => {
          val fieldsArray = line.split(',')
          (fieldsArray.head, fieldsArray.tail.dropRight(1).mkString(""), fieldsArray.last)
        }
      }
      .filter {
        mov => {
          var canBeLong = true
          try {
            mov._1.toLong
          }
          catch {
            case _: Throwable => canBeLong = false
          }
          canBeLong
        }
      }
      .map {
        movie => {
          val movieId = movie._1
          val movieName = movie._2.replace("\"", "")
          val genres = movie._3.split('|')
          Movie(movieId, movieName, genres)
        }
      }
  }

  def getGenres(movies: DataSet[Movie]): DataSet[String] = {
    movies.flatMap { _.genres }.distinct()
  }

  def filterByGenre(movies: DataSet[Movie], genre: String): DataSet[Movie] = {
    movies.filter { _.genres contains genre }
  }

  def prettyPrint(movies: DataSet[Movie]): Unit = {
    movies.map(movie => {
      s"${movie.id}, ${movie.title}, ${movie.genres.mkString(" | ")}"
    }).print()
  }

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    
    val movies = getMovies(env)

    val genres = getGenres(movies)

    val fantasyMovies = filterByGenre(movies, "Fantasy")

    prettyPrint(fantasyMovies)

    env.execute()
  }
}
