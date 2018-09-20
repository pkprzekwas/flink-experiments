package wordcnt

import org.apache.flink.api.scala._

object WordCount extends App {
  val env = ExecutionEnvironment.getExecutionEnvironment
  val text = env.readTextFile("s3://pprzekwa-data/pan-tadeusz.txt")

  val counts = text
    .flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
    .map { (_, 1) }
    .groupBy(0)
    .sum(1)

  counts.writeAsCsv("s3://pprzekwa-data/wc-flink-out-1",
    "\n", " ")

  env.execute()
}
