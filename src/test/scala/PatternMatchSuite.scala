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

import org.scalatest.FunSuite

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.Row

class PatternMatchSuite extends FunSuite {

  val conf = new SparkConf()
  val sc = new SparkContext("local", "test")
  val sqlContext = new SQLContext(sc)

  import sqlContext.implicits._

  val v = sqlContext.createDataFrame(List(
    (0L, "a"),
    (1L, "b"),
    (2L, "c"),
    (3L, "d"))).toDF("id", "attr")
  val e = sqlContext.createDataFrame(List(
    (0L, 1L),
    (1L, 2L),
    (2L, 3L),
    (2L, 0L))).toDF("src_id", "dst_id")
  val g = GraphFrame(v, e)

  test("triplets") {
    val triplets = g.find("(u)-[]->(v)")

    assert(triplets.columns === Array("u_id", "u_attr", "v_id", "v_attr"))
    assert(triplets.collect.toSet === Set(
      Row(0L, "a", 1L, "b"),
      Row(1L, "b", 2L, "c"),
      Row(2L, "c", 3L, "d"),
      Row(2L, "c", 0L, "a")
    ))
  }

  test("triangles with vertex attributes") {
    val triangles = g.find("(a)-[]->(b); (b)-[]->(c); (c)-[]->(a)")

    assert(triangles.columns === Array("a_id", "a_attr", "b_id", "b_attr", "c_id", "c_attr"))
    assert(triangles.collect.toSet === Set(
      Row(0L, "a", 1L, "b", 2L, "c"),
      Row(2L, "c", 0L, "a", 1L, "b"),
      Row(1L, "b", 2L, "c", 0L, "a")
    ))
  }

  test("triangles without vertex attributes") {
    val triangles = g.find("()-[e1]->(); (e1_dst)-[e2]->(); (e2_dst)-[]->(e1_src)")
    assert(triangles.columns === Array("e1_src_id", "e1_dst_id", "e2_src_id", "e2_dst_id"))
    assert(triangles.drop("e1_dst_id").collect.toSet === Set(
      Row(0L, 1L, 2L),
      Row(1L, 2L, 0L),
      Row(2L, 0L, 1L)
    ))
  }

 test("vertex queries") {
    val vertices = g.find("(a)")
    assert(vertices.columns === Array("a_id", "a_attr"))
    assert(vertices.collect.toSet === v.collect.toSet)

    val edges = g.find("()-[e]->(); (e_src)")
    assert(edges.columns === Array("e_src_id", "e_dst_id"))
    assert(edges.collect.toSet === e.collect.toSet)

    val empty = g.find("()")
    assert(empty.collect === Array.empty)
  }
}
