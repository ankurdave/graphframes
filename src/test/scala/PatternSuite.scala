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

class PatternSuite extends FunSuite {

  test("parser") {
    assert(Pattern.parse("(abc)") ===
      Seq(NamedVertex("abc")))

    assert(Pattern.parse("()") ===
      Seq(AnonymousVertex()))

    assert(Pattern.parse("(u)-[e]->(v)") ===
      Seq(NamedEdge("e", NamedVertex("u"), NamedVertex("v"))))

    assert(Pattern.parse("()-[]->(v)") ===
      Seq(AnonymousEdge(AnonymousVertex(), NamedVertex("v"))))

    assert(Pattern.parse("(u); ()-[]->(v)") ===
      Seq(NamedVertex("u"), AnonymousEdge(AnonymousVertex(), NamedVertex("v"))))
  }

  test("references") {
    assert(Pattern.parse("(a)-[]->(b); (b)-[]->(c)") ===
      Seq(
        AnonymousEdge(NamedVertex("a"), NamedVertex("b")),
        AnonymousEdge(VertexReference("b"), NamedVertex("c"))))

    assert(Pattern.parse("(a)-[]->(b); (b)-[]->(c); (c)-[]->(a)") ===
      Seq(
        AnonymousEdge(NamedVertex("a"), NamedVertex("b")),
        AnonymousEdge(VertexReference("b"), NamedVertex("c")),
        AnonymousEdge(VertexReference("c"), VertexReference("a"))))

    assert(Pattern.parse("()-[e1]->(); (e1_dst)-[e2]->(); (e2_dst)-[]->(e1_src)") ===
      Seq(
        NamedEdge("e1", AnonymousVertex(), AnonymousVertex()),
        NamedEdge("e2", VertexReference("e1_dst"), AnonymousVertex()),
        AnonymousEdge(VertexReference("e2_dst"), VertexReference("e1_src"))))

  }

}