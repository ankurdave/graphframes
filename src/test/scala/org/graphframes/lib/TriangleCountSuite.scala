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

package org.graphframes.lib

import org.apache.spark.sql.Row

import org.graphframes.{GraphFrameTestSparkContext, GraphFrame, SparkFunSuite}

class TriangleCountSuite extends SparkFunSuite with GraphFrameTestSparkContext {

  test("Count a single triangle") {
    val edges = sqlContext.createDataFrame(Array(0L -> 1L, 1L -> 2L, 2L -> 0L)).toDF("src", "dst")
    val vertices = sqlContext.createDataFrame(Seq((0L, ""), (1L, ""), (2L, ""))).toDF("id", "v_attr1")
    val g = GraphFrame(vertices, edges)
    val g2 = TriangleCount.run(g)
    LabelPropagationSuite.testSchemaInvariants(g, g2)
    g2.vertices.select("id", "count", "v_attr1")
      .collect().foreach { case Row(vid: Long, count: Int, _) => assert(count === 1) }
  }

}
