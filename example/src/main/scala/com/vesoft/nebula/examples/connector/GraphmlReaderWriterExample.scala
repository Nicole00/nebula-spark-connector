/* Copyright (c) 2023 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.examples.connector

import com.facebook.thrift.protocol.TCompactProtocol
import com.vesoft.nebula.connector.connector.NebulaDataFrameReader
import com.vesoft.nebula.connector.connector.NebulaDataFrameWriter
import com.vesoft.nebula.connector.{
  NebulaConnectionConfig,
  ReadNebulaConfig,
  WriteMode,
  WriteNebulaEdgeConfig,
  WriteNebulaVertexConfig
}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

/**
  * before run this example, please make you have execute these statement in your NebulaGraph.
  * CREATE SPACE IF NOT EXISTS test(vid_type=fixed_string(50));
  * USE test;
  * CREATE TAG person(name string, age string, born string);
  * CREATE EDGE friend(degree int64);
  * */
object GraphmlReaderWriterExample {
  private val LOG = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf
    sparkConf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array[Class[_]](classOf[TCompactProtocol]))
    val spark = SparkSession
      .builder()
      .master("local")
      .config(sparkConf)
      .getOrCreate()
    importGraphml2Nebula(spark)
    exportNebula2Graphml(spark)

  }

  /**
    * import the graphml file into NebulaGraph，vertex and edge are imported separately.
    *
    * .option("rowTag", "node") rowTag need to be modified according to your graphml file
    * .load("example/src/main/resources/example.graphml") graphml file path need to be modified
    *
    * schema name in spark dataframe is "_"+original name in graphml. Such as "id" in example.graphml,
    * the corresponding schema name in spark dataframe is "_id"
    *
    * please make sure the property name in spark dataframe is the same with them in NebulaGraph.
    * use node.withColumnRenamed(graphml_property_name, nebulagraph_property_name) to change your dataframe's schema name.
    * */
  def importGraphml2Nebula(spark: SparkSession): Unit = {
    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withGraphAddress("127.0.0.1:9669")
        .withConenctionRetry(2)
        .build()

    // import graphml into nebula vertex
    val node = spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "node")
      .load("example/src/main/resources/example.graphml")

    node.printSchema()
    node.show()
    val nebulaWriteVertexConfig: WriteNebulaVertexConfig = WriteNebulaVertexConfig
      .builder()
      .withSpace("test")
      .withTag("person")
      .withVidField("_id")
      .withWriteMode(WriteMode.DELETE)
      .withVidAsProp(false)
      .withBatch(1000)
      .build()
    node.write.nebula(config, nebulaWriteVertexConfig).writeVertices()

    // import graphml into nebula edge
    val edge = spark.read
      .format("com.databricks.spark.xml")
      .option("rowTag", "edge")
      .load("example/src/main/resources/example.graphml")
    edge.printSchema()
    edge.show()

    val nebulaWriteEdgeConfig: WriteNebulaEdgeConfig = WriteNebulaEdgeConfig
      .builder()
      .withSpace("test")
      .withEdge("friend")
      .withSrcIdField("_source")
      .withDstIdField("_target")
      .withSrcAsProperty(false)
      .withDstAsProperty(false)
      .withRankAsProperty(false)
      .withBatch(1000)
      .build()
    edge.write.nebula(config, nebulaWriteEdgeConfig).writeEdges()
  }

  /**
    * export NebulaGraph to graphml format. vertex and edge are exported separately.
    */
  def exportNebula2Graphml(spark: SparkSession): Unit = {
    val config =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress("127.0.0.1:9559")
        .withConenctionRetry(2)
        .build()
    val nebulaReadVertexConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace("test")
      .withLabel("person")
      .withNoColumn(false)
      .withReturnCols(List())
      .withLimit(10)
      .withPartitionNum(10)
      .build()
    val vertex = spark.read.nebula(config, nebulaReadVertexConfig).loadVerticesToDF()
    vertex.printSchema()
    vertex.show()

    vertex
      .repartition(1)
      .write
      .format("com.databricks.spark.xml")
      .option("rootTag", "graph")
      .option("rowTag", "node")
      .save("/tmp/node.graphml")
  }
}
