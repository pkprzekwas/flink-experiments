package helpers

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment

trait Context {
  lazy val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
  lazy val tEnv: BatchTableEnvironment = TableEnvironment.getTableEnvironment(env)
  lazy val sEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
}