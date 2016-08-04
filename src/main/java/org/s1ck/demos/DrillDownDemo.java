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
import org.apache.flink.api.java.io.neo4j.Neo4jOutputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.common.util.GConstants;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.graph.GraphDataSource;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Extracts the persons, companies and their worksAt relations from a Neo4j
 * database and creates a grouped graph where persons are represented by a
 * single node which is connected to each company. The edges store the number
 * of persons working at a specific company.
 */
public class DrillDownDemo {

  public static final String NEO4J_INPUT_REST_URI = "http://localhost:7474/db/data/";

  public static final String NEO4J_OUTPUT_REST_URI = "http://localhost:4242/db/data/";

  public static final String NEO4J_USERNAME = "neo4j";

  public static final String NEO4J_PASSWORD = "password";

  public static final Integer NEO4J_CONNECT_TIMEOUT = 10_000;

  public static final Integer NEO4J_READ_TIMEOUT = 10_000;

  /**
   * Read all vertices with label 'person' or 'company'. For companies
   * additionally read their name.
   */
  public static final String NEO4J_VERTEX_QUERY =
    "CYPHER RUNTIME=COMPILED " +
      "MATCH (n) " +
      "WHERE n:company OR n:person " +
      "RETURN id(n), head(labels(n)), CASE WHEN n:company THEN n.name ELSE \"\" END";

  /**
   * Read all edges between persons and companies of type 'worksAt'
   */
  public static final String NEO4J_EDGE_QUERY =
    "CYPHER RUNTIME=COMPILED " +
      "MATCH (a:person)-[e:workAt]->(b:company) " +
      "RETURN id(e), id(a), id(b)";

  /**
   * Create a new graph from insert tuples representing edges. Each tuple
   * consists of:
   *
   * - Person vertex: EPGM id and group count
   * - Company vertex: EPGM id, company name and group count
   * - WORKS_AT edge: group count
   */
  public static final String NEO4J_CREATE_QUERY = "" +
    "UNWIND {tuples} as t " +
    "MERGE (a:PersonGroup {epgmId : t.f, count : t.fC}) " +
    "MERGE (b:Company {epgmId : t.t, name: t.tName, count : t.tC}) " +
    "CREATE (a)-[:WORKS_AT {count : t.eC}]->(b)";

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

    // initialize Gradoop database from Neo4j graph
    DataSource dataSource = new GraphDataSource<>(
      // get vertices from Neo4j
      getImportVertices(env),
      // get edges from Neo4j
      getImportEdges(env),
      // gradoop config
      config);
    

    LogicalGraph neoGraph = dataSource.getLogicalGraph();

    // do a graph grouping by vertex and edge label (+ count group members)
    LogicalGraph groupedGraph = neoGraph
        // if a vertex represents a person return a vertex with the same label.
        // Otherwise, if a vertex represents a company return a vertex with the
        // company name as label
        .transformVertices(new TransformationFunction<Vertex>() {
          @Override
          public Vertex execute(Vertex current, Vertex transformed) {
            if(current.getLabel().equals("person")) {
              transformed.setLabel("person");
            } else if (current.getLabel().equals("company")) {
              transformed.setLabel(current.getPropertyValue("name").getString());
            }
            return transformed;
          }
        })
        // group graph by vertex label
        .groupByVertexLabel();

    // write graph back to Neo4j
    writeTriplets(Utils.buildTriplets(groupedGraph));

    env.execute();

    System.out.println(env.getLastJobExecutionResult().getNetRuntime());
  }

  @SuppressWarnings("unchecked")
  public static DataSet<ImportVertex<Integer>> getImportVertices(ExecutionEnvironment env) {

    Neo4jInputFormat<Tuple3<Integer, String, String>> neoInput =
      Neo4jInputFormat.buildNeo4jInputFormat()
        .setRestURI(NEO4J_INPUT_REST_URI)
        .setCypherQuery(NEO4J_VERTEX_QUERY)
        .setUsername(NEO4J_USERNAME)
        .setPassword(NEO4J_PASSWORD)
        .setConnectTimeout(NEO4J_CONNECT_TIMEOUT)
        .setReadTimeout(NEO4J_READ_TIMEOUT)
        .finish();

    DataSet<Tuple3<Integer, String, String>> rows = env.createInput(neoInput,
      new TupleTypeInfo<Tuple3<Integer, String, String>>(
        BasicTypeInfo.INT_TYPE_INFO,       // vertex id
        BasicTypeInfo.STRING_TYPE_INFO,    // vertex label
        BasicTypeInfo.STRING_TYPE_INFO));  // company name

    return rows.map(new MapFunction<Tuple3<Integer, String, String>, ImportVertex<Integer>>() {
      @Override
      public ImportVertex<Integer> map(Tuple3<Integer, String, String> row) throws
        Exception {
        ImportVertex<Integer> importVertex = new ImportVertex<>();
        importVertex.setId(row.f0);
        importVertex.setLabel(row.f1);

        PropertyList properties;
        if (row.f1.equals("person")) {
          properties = PropertyList.createWithCapacity(0);
        } else {
          properties = PropertyList.createWithCapacity(1);
          properties.set("name", row.f2);
        }
        importVertex.setProperties(properties);

        return importVertex;
      }
    });
  }

  @SuppressWarnings("unchecked")
  public static DataSet<ImportEdge<Integer>> getImportEdges(ExecutionEnvironment env) {
    Neo4jInputFormat<Tuple3<Integer, Integer, Integer>> neoInput =
      Neo4jInputFormat.buildNeo4jInputFormat()
        .setRestURI(NEO4J_INPUT_REST_URI)
        .setCypherQuery(NEO4J_EDGE_QUERY)
        .setUsername(NEO4J_USERNAME)
        .setPassword(NEO4J_PASSWORD)
        .setConnectTimeout(NEO4J_CONNECT_TIMEOUT)
        .setReadTimeout(NEO4J_READ_TIMEOUT)
        .finish();

    DataSet<Tuple3<Integer, Integer, Integer>> rows = env
      .createInput(neoInput,
        new TupleTypeInfo<Tuple3<Integer, Integer, Integer>>(
          BasicTypeInfo.INT_TYPE_INFO,      // edge id
          BasicTypeInfo.INT_TYPE_INFO,      // source vertex id
          BasicTypeInfo.INT_TYPE_INFO));    // target vertex id

    return rows.map(new MapFunction<Tuple3<Integer, Integer, Integer>, ImportEdge<Integer>>() {

      @Override
      public ImportEdge<Integer> map(Tuple3<Integer, Integer, Integer> row) throws Exception {
        ImportEdge<Integer> importEdge = new ImportEdge<>();
        importEdge.setId(row.f0);
        importEdge.setLabel(GConstants.DEFAULT_EDGE_LABEL);
        importEdge.setSourceId(row.f1);
        importEdge.setTargetId(row.f2);
        importEdge.setProperties(PropertyList.createWithCapacity(0));

        return importEdge;
      }
    });
  }

  @SuppressWarnings("unchecked")
  public static void writeTriplets(DataSet<Tuple3<Vertex, Edge, Vertex>> triplets) {
    triplets.map(new MapFunction<Tuple3<Vertex,Edge,Vertex>,
      Tuple6<String, Long, String, String, Long, Long>>() {

      @Override
      public Tuple6<String, Long, String, String, Long, Long> map(
        Tuple3<Vertex, Edge, Vertex> triplet) throws Exception {
        return new Tuple6<>(
          triplet.f0.getId().toString(),
          triplet.f0.getPropertyValue("count").getLong(),
          triplet.f2.getId().toString(),
          triplet.f2.getLabel(),
          triplet.f2.getPropertyValue("count").getLong(),
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
      .addParameterKey(0, "f")      // from
      .addParameterKey(1, "fC")     // from count
      .addParameterKey(2, "t")      // to
      .addParameterKey(3, "tName")  // to name
      .addParameterKey(4, "tC")     // to count
      .addParameterKey(5, "eC")     // edge count
      .finish()).setParallelism(1);
  }
}
