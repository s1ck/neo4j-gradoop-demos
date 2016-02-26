/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.s1ck.demos;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.neo4j.Neo4jInputFormat;
import org.apache.flink.api.java.io.neo4j.Neo4jOutputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.gradoop.io.graph.tuples.ImportEdge;
import org.gradoop.io.graph.tuples.ImportVertex;
import org.gradoop.model.impl.EPGMDatabase;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.functions.epgm.SourceId;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.model.impl.properties.PropertyList;
import org.gradoop.util.GradoopFlinkConfig;

/**
 * Extracts the labels of vertices and edges from a Neo4j database and creates
 * a schema graph which is written back to another Neo4j instance. Each vertex
 * and edge in the schema graph stores the number of vertices and edges in the
 * source graph with the same label.
 */
public class SchemaDemo {

  public static final String NEO4J_INPUT_REST_URI = "http://localhost:7474/db/data/";

  public static final String NEO4J_OUTPUT_REST_URI = "http://localhost:4242/db/data/";

  public static final String NEO4J_USERNAME = "neo4j";

  public static final String NEO4J_PASSWORD = "password";

  public static final Integer NEO4J_CONNECT_TIMEOUT = 10_000;

  public static final Integer NEO4J_READ_TIMEOUT = 10_000;

  public static final String NEO4J_VERTEX_QUERY =
    "CYPHER RUNTIME=COMPILED " +
      "MATCH (n) " +
      "RETURN id(n), labels(n)[0]";

  public static final String NEO4J_EDGE_QUERY =
    "CYPHER RUNTIME=COMPILED " +
      "MATCH (a)-[e]->(b) " +
      "RETURN id(e), id(a), id(b), type(e)";

  public static final String NEO4J_CREATE_QUERY = "" +
    "UNWIND {triples} as t " +
    "MERGE (a:SchemaNode {epgmId : t.f, label : t.fL, count : t.fC}) " +
    "MERGE (b:SchemaNode {epgmId : t.t, label : t.tL, count : t.tC}) " +
    "CREATE (a)-[:SchemaEdge {label : t.eL, count : t.eC}]->(b)";

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // enter Gradoop
    EPGMDatabase<GraphHeadPojo, VertexPojo, EdgePojo> epgmDatabase =
      EPGMDatabase.fromExternalGraph(
        // get vertices from Neo4j
        getImportVertices(env),
        // get edges from Neo4j
        getImportEdges(env),
        GradoopFlinkConfig.createDefaultConfig(env)
      );

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> databaseGraph =
      epgmDatabase.getDatabaseGraph();

    // do a simple grouping by vertex and edge label + count group members
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> groupedGraph =
      databaseGraph.groupByVertexAndEdgeLabel();

    // write graph back to Neo4j
    writeTriplets(buildTriplet(groupedGraph));

    env.execute();

    System.out.println(env.getLastJobExecutionResult().getNetRuntime());
  }

  @SuppressWarnings("unchecked")
  public static DataSet<ImportVertex<Integer>> getImportVertices(ExecutionEnvironment env) {

    Neo4jInputFormat<Tuple2<Integer, String>> neoInput =
      Neo4jInputFormat.buildNeo4jInputFormat()
        .setRestURI(NEO4J_INPUT_REST_URI)
        .setCypherQuery(NEO4J_VERTEX_QUERY)
        .setUsername(NEO4J_USERNAME)
        .setPassword(NEO4J_PASSWORD)
        .setConnectTimeout(NEO4J_CONNECT_TIMEOUT)
        .setReadTimeout(NEO4J_READ_TIMEOUT)
        .finish();

    DataSet<Tuple2<Integer, String>> rows = env.createInput(neoInput,
      new TupleTypeInfo<Tuple2<Integer, String>>(
        BasicTypeInfo.INT_TYPE_INFO,       // vertex id
        BasicTypeInfo.STRING_TYPE_INFO));  // vertex label


    return rows.map(new MapFunction<Tuple2<Integer, String>, ImportVertex<Integer>>() {
      @Override
      public ImportVertex<Integer> map(Tuple2<Integer, String> row) throws
        Exception {
        ImportVertex<Integer> importVertex = new ImportVertex<>();
        importVertex.setId(row.f0);
        importVertex.setLabel(row.f1);
        PropertyList properties = PropertyList.createWithCapacity(0);
        importVertex.setProperties(properties);

        return importVertex;
      }
    });
  }

  @SuppressWarnings("unchecked")
  public static DataSet<ImportEdge<Integer>> getImportEdges(ExecutionEnvironment env) {
    Neo4jInputFormat<Tuple4<Integer, Integer, Integer, String>> neoInput =
      Neo4jInputFormat.buildNeo4jInputFormat()
        .setRestURI(NEO4J_INPUT_REST_URI)
        .setCypherQuery(NEO4J_EDGE_QUERY)
        .setUsername(NEO4J_USERNAME)
        .setPassword(NEO4J_PASSWORD)
        .setConnectTimeout(NEO4J_CONNECT_TIMEOUT)
        .setReadTimeout(NEO4J_READ_TIMEOUT)
        .finish();

    DataSource<Tuple4<Integer, Integer, Integer, String>> rows = env
      .createInput(neoInput,
        new TupleTypeInfo<Tuple4<Integer, Integer, Integer, String>>(
          BasicTypeInfo.INT_TYPE_INFO,      // edge id
          BasicTypeInfo.INT_TYPE_INFO,      // source vertex id
          BasicTypeInfo.INT_TYPE_INFO,      // target vertex id
          BasicTypeInfo.STRING_TYPE_INFO)); // edge label

    return rows.map(new MapFunction<Tuple4<Integer, Integer, Integer, String>, ImportEdge<Integer>>() {

      @Override
      public ImportEdge<Integer> map(Tuple4<Integer, Integer, Integer, String> row) throws Exception {
        ImportEdge<Integer> importEdge = new ImportEdge<>();
        importEdge.setId(row.f0);
        importEdge.setSourceVertexId(row.f1);
        importEdge.setTargetVertexId(row.f2);
        importEdge.setLabel(row.f3);
        importEdge.setProperties(PropertyList.createWithCapacity(0));

        return importEdge;
      }
    });
  }

  public static DataSet<Tuple3<VertexPojo, EdgePojo, VertexPojo>> buildTriplet(LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> graph) {
    return graph.getVertices()
      .join(graph.getEdges())
      .where(new Id<VertexPojo>()).equalTo(new SourceId<EdgePojo>())
      .with(new JoinFunction<VertexPojo, EdgePojo, Tuple2<VertexPojo, EdgePojo>>() {

        @Override
        public Tuple2<VertexPojo, EdgePojo> join(VertexPojo vertexPojo,
          EdgePojo edgePojo) throws Exception {
          return new Tuple2<>(vertexPojo, edgePojo);
        }
      })
      .join(graph.getVertices())
      .where("1.targetId").equalTo(new Id<VertexPojo>())
      .with(new JoinFunction<Tuple2<VertexPojo,EdgePojo>, VertexPojo, Tuple3<VertexPojo, EdgePojo, VertexPojo>>() {

        @Override
        public Tuple3<VertexPojo, EdgePojo, VertexPojo> join(
          Tuple2<VertexPojo, EdgePojo> sourceVertexAndEdge,
          VertexPojo targetVertex) throws Exception {
          return new Tuple3<>(
            sourceVertexAndEdge.f0, sourceVertexAndEdge.f1, targetVertex);
        }
      });
  }

  @SuppressWarnings("unchecked")
  public static void writeTriplets(DataSet<Tuple3<VertexPojo, EdgePojo, VertexPojo>> triplets) {
    triplets.map(new MapFunction<Tuple3<VertexPojo,EdgePojo,VertexPojo>,
      Tuple8<String, String, Long, String, String, Long, String, Long>>() {

      @Override
      public Tuple8<String, String, Long, String, String, Long, String, Long> map(
        Tuple3<VertexPojo, EdgePojo, VertexPojo> triplet) throws Exception {
        return new Tuple8<>(
          triplet.f0.getId().toString(),
          triplet.f0.getLabel(),
          triplet.f0.getPropertyValue("count").getLong(),
          triplet.f2.getId().toString(),
          triplet.f2.getLabel(),
          triplet.f2.getPropertyValue("count").getLong(),
          triplet.f1.getLabel(),
          triplet.f1.getPropertyValue("count").getLong()
        );
      }
    }).output(Neo4jOutputFormat.buildNeo4jOutputFormat()
      .setRestURI(NEO4J_OUTPUT_REST_URI)
      .setUsername(NEO4J_USERNAME)
      .setPassword(NEO4J_PASSWORD)
      .setConnectTimeout(NEO4J_CONNECT_TIMEOUT)
      .setReadTimeout(NEO4J_READ_TIMEOUT)
      .setCypherQuery(NEO4J_CREATE_QUERY)
      .addParameterKey(0, "f")  // from
      .addParameterKey(1, "fL") // from label
      .addParameterKey(2, "fC") // from count
      .addParameterKey(3, "t")  // to
      .addParameterKey(4, "tL") // to label
      .addParameterKey(5, "tC") // to count
      .addParameterKey(6, "eL") // edge label
      .addParameterKey(7, "eC") // edge count
      .finish())
      .setParallelism(1);
  }
}
