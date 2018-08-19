package pi

import org.apache.flink.api.scala._

object PiFlink {

  def main(args: Array[String]) {

    val numSamples: Long = 1000000

    val env = ExecutionEnvironment.getExecutionEnvironment

    val count = env.generateSequence(1, numSamples)
      .map  { _ =>
        val x = Math.random()
        val y = Math.random()
        if (x * x + y * y < 1) 1L else 0L
      }
      .reduce(_ + _)

    val pi = count
      .map ( _ * 4.0 / numSamples )

    println("We estimate Pi to be:")

    pi.print()

    env.execute()
  }

}