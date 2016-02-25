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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.neo4j.Neo4jInputFormat;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.gradoop.io.graph.tuples.ImportEdge;
import org.gradoop.io.graph.tuples.ImportVertex;
import org.gradoop.model.impl.EPGMDatabase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.operators.tostring.CanonicalAdjacencyMatrixBuilder;
import org.gradoop.model.impl.operators.tostring.functions.EdgeToDataString;
import org.gradoop.model.impl.operators.tostring.functions.GraphHeadToEmptyString;
import org.gradoop.model.impl.operators.tostring.functions.VertexToDataString;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.model.impl.properties.PropertyList;
import org.gradoop.util.GradoopFlinkConfig;

/**
 * Reads the movie example graph from Neo4j into Gradoop and groups it based
 * on vertex and edge labels.
 */
public class MovieGraphGroupingDemo {

  public static final String NEO4J_REST_URI = "http://localhost:7474/db/data/";

  public static final String NEO4J_USERNAME = "neo4j";

  public static final String NEO4J_PASSWORD = "password";

  public static final String NEO4J_VERTEX_QUERY =
    "CYPHER RUNTIME=COMPILED " +
      "MATCH (n) " +
      "RETURN id(n), labels(n)[0], " +
      "CASE WHEN n:Person THEN n.name WHEN n:Movie THEN n.title END";

  public static final String NEO4J_EDGE_QUERY =
    "CYPHER RUNTIME=COMPILED " +
      "MATCH (a)-[e]->(b) " +
      "RETURN id(e), id(a), id(b), type(e)";

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

    // print canonical label of the graph
    new CanonicalAdjacencyMatrixBuilder<>(
      new GraphHeadToEmptyString<GraphHeadPojo>(),
      new VertexToDataString<VertexPojo>(),
      new EdgeToDataString<EdgePojo>())
      .execute(GraphCollection.fromGraph(groupedGraph))
      .print();

    System.out.println(env.getLastJobExecutionResult().getNetRuntime());
  }

  @SuppressWarnings("unchecked")
  public static DataSet<ImportVertex<Integer>> getImportVertices(ExecutionEnvironment env) {

    Neo4jInputFormat<Tuple3<Integer, String, String>> neoInput =
      Neo4jInputFormat.buildNeo4jInputFormat()
        .setRestURI(NEO4J_REST_URI)
        .setCypherQuery(NEO4J_VERTEX_QUERY)
        .setUsername(NEO4J_USERNAME)
        .setPassword(NEO4J_PASSWORD)
        .setConnectTimeout(10000)
        .setReadTimeout(10000)
        .finish();

    DataSet<Tuple3<Integer, String, String>> rows = env.createInput(neoInput,
      new TupleTypeInfo<Tuple3<Integer, String, String>>(
        BasicTypeInfo.INT_TYPE_INFO,       // vertex id
        BasicTypeInfo.STRING_TYPE_INFO,    // vertex label
        BasicTypeInfo.STRING_TYPE_INFO));  // vertex property 'name' or 'title'

    return rows.map(new MapFunction<Tuple3<Integer, String, String>, ImportVertex<Integer>>() {
      @Override
      public ImportVertex<Integer> map(Tuple3<Integer, String, String> row) throws
        Exception {
        ImportVertex<Integer> importVertex = new ImportVertex<>();
        importVertex.setId(row.f0);
        importVertex.setLabel(row.f1);
        PropertyList properties = PropertyList.createWithCapacity(1);
        properties.set(row.f1.equals("Person") ? "name" : "title", row.f2);
        importVertex.setProperties(properties);

        return importVertex;
      }
    });
  }

  @SuppressWarnings("unchecked")
  public static DataSet<ImportEdge<Integer>> getImportEdges(ExecutionEnvironment env) {
    Neo4jInputFormat<Tuple4<Integer, Integer, Integer, String>> neoInput =
      Neo4jInputFormat.buildNeo4jInputFormat()
        .setRestURI(NEO4J_REST_URI)
        .setCypherQuery(NEO4J_EDGE_QUERY)
        .setUsername(NEO4J_USERNAME)
        .setPassword(NEO4J_PASSWORD)
        .setConnectTimeout(10000)
        .setReadTimeout(10000)
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
}
