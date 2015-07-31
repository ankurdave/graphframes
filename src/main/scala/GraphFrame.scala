/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.HashPartitioner
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.{RDD, ShuffledRDD}
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.storage.StorageLevel

object GraphFrame {
  def apply(v: DataFrame, e: DataFrame): GraphFrame = {
    require(v.columns.contains("id"))
    require(e.columns.contains("srcId") && e.columns.contains("dstId"))
    new GraphFrame(v, e)
  }
}

class GraphFrame protected (
    @transient val v: DataFrame,
    @transient val e: DataFrame)
  extends Serializable {

  def vertices = v
  def edges = e

  private def sqlContext = v.sqlContext

  /** Default constructor is provided to support serialization */
  protected def this() = this(null, null)

  def find(patterns: Seq[Pattern]): DataFrame = {
    val unitDataFrame = sqlContext.createDataFrame(
      sqlContext.sparkContext.parallelize(List(Row.empty)),
      StructType(Nil))
    patterns.foldLeft(unitDataFrame) {
      case (acc, p) =>
        findIncremental(acc, p)
    }
  }

  import v.sqlContext.implicits._

  private def prefixWithName(name: String, col: String) = name + "_" + col
  private def vId(name: String) = prefixWithName(name, "id")
  private def eSrcId(name: String) = prefixWithName(name, "srcId")
  private def eDstId(name: String) = prefixWithName(name, "dstId")
  private def pfxE(name: String) = renameAll(edges, prefixWithName(name, _))
  private def pfxV(name: String) = renameAll(vertices, prefixWithName(name, _))

  private def findIncremental(prev: DataFrame, p: Pattern): DataFrame = p match {
    case NamedEdge(name, VertexReference(srcName), VertexReference(dstName)) =>
      val eRen = pfxE(name)
      prev.join(eRen,
        eRen(eSrcId(name)) === prev(vId(srcName)) && eRen(eDstId(name)) === prev(vId(dstName)))

    case NamedEdge(name, VertexReference(srcName), NamedVertex(dstName)) =>
      val eRen = pfxE(name)
      val dstV = pfxV(dstName)
      prev.join(eRen, eRen(eSrcId(name)) === prev(vId(srcName)))
        .join(dstV, eRen(eDstId(name)) === dstV(vId(dstName)))

    case NamedEdge(name, NamedVertex(srcName), VertexReference(dstName)) =>
      val eRen = pfxE(name)
      val srcV = pfxV(srcName)
      prev.join(eRen, eRen(eDstId(name)) === prev(vId(dstName)))
        .join(srcV, eRen(eSrcId(name)) === srcV(vId(srcName)))

    case NamedEdge(name, NamedVertex(srcName), NamedVertex(dstName)) =>
      val eRen = pfxE(name)
      val srcV = pfxV(srcName)
      val dstV = pfxV(dstName)
      prev.join(eRen)
        .join(srcV, eRen(eSrcId(name)) === srcV(vId(srcName)))
        .join(dstV, eRen(eDstId(name)) === dstV(vId(dstName)))

    case AnonymousEdge(src, dst) =>
      val tmpName = "_tmp"
      val result = findIncremental(prev, NamedEdge(tmpName, src, dst))
      dropAll(result, e.columns.map(col => prefixWithName(tmpName, col)))
  }

  private def dropAll(df: DataFrame, columns: Seq[String]): DataFrame =
    columns.foldLeft(df) { (df, col) => df.drop(col) }

  private def renameAll(df: DataFrame, f: String => String): DataFrame = {
    val colNames = df.schema.map { field =>
      val name = field.name
      new Column(name).as(f(name))
    }
    df.select(colNames : _*)
  }
}
