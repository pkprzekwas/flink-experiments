package wordcnt

import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.getExecutionEnvironment
    val text = env.readTextFile("/tmp/in.txt")

    val counts = text
      .flatMap {
        _.toLowerCase.split("\\W+") filter { _.nonEmpty }
      }
      .map { (_, 1) }
      .groupBy(0)
      .sum(1)

    counts.writeAsCsv("/tmp/out",
      "\n", " ")

    env.execute()
  }
}
