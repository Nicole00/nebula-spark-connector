/* Copyright (c) 2023 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

package com.vesoft.nebula.examples.connector

import com.facebook.thrift.protocol.TCompactProtocol
import com.vesoft.nebula.connector.{NebulaConnectionConfig, ReadNebulaConfig}
import com.vesoft.nebula.connector.connector.NebulaDataFrameReader
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Nebula2Csv {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf
    sparkConf
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array[Class[_]](classOf[TCompactProtocol]))
    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    val sourceMetaAddr: String = args(0)
    val sourceSpace: String    = args(1)
    val sourceEdge: String     = args(2)
    val limit: Int             = args(3).toInt
    val partition: Int         = args(4).toInt
    val path: String           = args(5)

    val sourceConnectConfig =
      NebulaConnectionConfig
        .builder()
        .withMetaAddress(sourceMetaAddr)
        .withConenctionRetry(2)
        .withTimeout(500000)
        .build()

    val nebulaReadEdgeConfig: ReadNebulaConfig = ReadNebulaConfig
      .builder()
      .withSpace(sourceSpace)
      .withLabel(sourceEdge)
      .withReturnCols(List())
      .withLimit(limit)
      .withPartitionNum(partition)
      .build()
    val edgeDf = spark.read.nebula(sourceConnectConfig, nebulaReadEdgeConfig).loadEdgesToDF()

    edgeDf.write.option("header", true).option("delimiter", ",").csv(path)
  }
}
