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

import scala.util.parsing.combinator._

object PatternParser extends RegexParsers {
  def vertexName: Parser[Vertex] = "[a-zA-Z0-9_]+".r ^^ { NamedVertex(_) }
  def anonymousVertex: Parser[Vertex] = "" ^^ { x => AnonymousVertex() }
  def vertex: Parser[Vertex] = "(" ~> (vertexName | anonymousVertex) <~ ")"

  def namedEdge: Parser[Edge] =
    vertex ~ "-" ~ "[" ~ "[a-zA-Z0-9_]+".r ~ "]" ~ "->" ~ vertex ^^ {
      case src ~ "-" ~ "[" ~ name ~ "]" ~ "->" ~ dst => NamedEdge(name, src, dst)
    }
  def anonymousEdge: Parser[Edge] =
    vertex ~ "-" ~ "[" ~ "]" ~ "->" ~ vertex ^^ {
      case src ~ "-" ~ "[" ~ "]" ~ "->" ~ dst => AnonymousEdge(src, dst)
    }
  def edge: Parser[Edge] = namedEdge | anonymousEdge

  def pattern: Parser[Pattern] = edge | vertex

  def patterns: Parser[List[Pattern]] = repsep(pattern, ";")
}

object Pattern {
  def parse(s: String): Seq[Pattern] = {
    import PatternParser._
    addReferences(parseAll(patterns, s).get)
  }

  private def addReferences(
      ps: Seq[Pattern]): Seq[Pattern] =
    ps.foldLeft(List.empty[Pattern], Set.empty[String]) {
      case ((pAcc, vsAcc), p) =>
        val (newP, newVs) = addReferences(p, vsAcc)
        (pAcc :+ newP, newVs)
    }._1

  private def addReferencesToVertex(p: Vertex, verticesSeen: Set[String]): (Vertex, Set[String]) =
    p match {
      case AnonymousVertex() | VertexReference(_) =>
        (p, verticesSeen)
      case NamedVertex(name) if verticesSeen.contains(name) =>
        (VertexReference(name), verticesSeen)
      case NamedVertex(name) =>
        (NamedVertex(name), verticesSeen + name)
    }

  private def addReferences(p: Pattern, verticesSeen: Set[String]): (Pattern, Set[String]) =
    p match {
      case v: Vertex => addReferencesToVertex(v, verticesSeen)
      case AnonymousEdge(src, dst) =>
        val (newSrc, verticesSeen1) = addReferencesToVertex(src, verticesSeen)
        val (newDst, verticesSeen2) = addReferencesToVertex(dst, verticesSeen1)
        (AnonymousEdge(newSrc, newDst), verticesSeen2)
      case NamedEdge(name, src, dst) =>
        val verticesSeen1 = verticesSeen ++ Set(name + "_src", name + "_dst")
        val (newSrc, verticesSeen2) = addReferencesToVertex(src, verticesSeen1)
        val (newDst, verticesSeen3) = addReferencesToVertex(dst, verticesSeen2)
        (NamedEdge(name, newSrc, newDst), verticesSeen3)
    }
}

sealed trait Pattern

sealed trait Vertex extends Pattern

case class AnonymousVertex() extends Vertex

case class NamedVertex(name: String) extends Vertex

case class VertexReference(name: String) extends Vertex

sealed trait Edge extends Pattern

case class AnonymousEdge(src: Vertex, dst: Vertex) extends Edge

case class NamedEdge(name: String, src: Vertex, dst: Vertex) extends Edge
