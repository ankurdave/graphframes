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

sealed trait Pattern

sealed trait Vertex extends Pattern

// case class AnonymousVertex() extends Vertex

case class NamedVertex(name: String) extends Vertex

case class VertexReference(name: String) extends Vertex

sealed trait Edge extends Pattern

case class AnonymousEdge(src: Vertex, dst: Vertex) extends Edge

case class NamedEdge(name: String, src: Vertex, dst: Vertex) extends Edge

// example: triangle
