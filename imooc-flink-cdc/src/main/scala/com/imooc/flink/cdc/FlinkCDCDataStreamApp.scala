package com.imooc.flink.cdc

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions
import com.alibaba.ververica.cdc.debezium.{DebeziumSourceFunction, StringDebeziumDeserializationSchema}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig


object FlinkCDCDataStreamApp {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)

    /**
      * 准备工作：
      * 1)mvn clean package -DskipTests   代码打包
      * 2)将 flink-cdc的jar下载或者是本地的maven仓库中找到 放到$FLINK_HOME/lib/
      * 3)重启flink集群
      *
      * 测试：正常运行后，挂掉了，重新运行时，是不是会重头读取mysql的数据还是从挂掉的点开始读取
      *
      * 1) flink run -c com.imooc.flink.cdc.FlinkCDCDataStreamApp ....jar(准备工作的第一步打出来的jar包)
      *   在运行过程中，对于本程序的state都会存储到指定的路径中
      *
      * 2) flink savepoint flink_job_id  hdfs://localhost:8020/flink-ck/savepoints
      *
      * 3) 往mysql中插入数据、修改数据、删除数据
      *
      * 4) 从指定的savepoint中去重启flink作业
      * flink run -s hdfs://localhost:8020/flink-ck/savepoints/jobid -c com.imooc.flink.cdc.FlinkCDCDataStreamApp ....jar
      */

    /**
      * TODO... 需要开启checkpoint机制
      */
    environment.enableCheckpointing(5000L)
    environment.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    environment.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    // TODO... 设置重启策略    建议大家不要硬编码，而是设置在flink.yaml文件中
    environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L))

    // TODO... 设置StateBackend    建议大家不要硬编码，而是设置在flink.yaml文件中
    environment.setStateBackend(new FsStateBackend("hdfs://localhost:8020/flink-ck/cdc"))


    /**
      * 我现在执行这段代码，终端上会有几条数据输出？
      */
    val source: DebeziumSourceFunction[String] = MySQLSource.builder()
      .hostname("localhost")
      .port(3306)
      .username("root")
      .password("pk123456")
      .databaseList("pk_cdc") // 指定要监听的数据库
      .tableList("pk_cdc.user") // 指定要监听的表，可以是多个表，多个表之间使用逗号分隔，注意：db.table
      .startupOptions(StartupOptions.initial())
      .deserializer(new StringDebeziumDeserializationSchema)
      .build()

    environment.addSource(source).print()


    environment.execute()
  }
}
