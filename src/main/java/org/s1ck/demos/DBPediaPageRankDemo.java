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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.PageRank;

/**
 * Reads a DBPedia graph edge-wise from Neo4j, computes the Page
 * Rank for each vertex and updates each vertex in Neo4j with its Page Rank.
 */
public class DBPediaPageRankDemo {

  public static final String NEO4J_REST_URI = "http://localhost:7474/db/data/";

  public static final String NEO4J_USERNAME = "neo4j";

  public static final String NEO4J_PASSWORD = "password";

  public static final Integer NEO4J_CONNECT_TIMEOUT = 10_000;

  public static final Integer NEO4J_READ_TIMEOUT = 10_000;

  /**
   * Read all edges between DBPedia pages and return source and target id
   */
  public static final String NEO4J_EDGE_QUERY =
    "CYPHER RUNTIME=COMPILED " +
      "MATCH (p1:Page)-[:Link]->(p2) " +
      "RETURN id(p1), id(p2)";

  /**
   * Update existing vertices with a new property 'pagerank'
   */
  public static final String NEO4J_UPDATE_QUERY =
    "UNWIND {updates} as update " +
      "MATCH (p) " +
      "WHERE id(p) = update.id " +
      "SET p.pagerank = update.rank";

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    Neo4jInputFormat<Tuple2<Integer, Integer>> neoInput = Neo4jInputFormat
      .buildNeo4jInputFormat()
      .setRestURI(NEO4J_REST_URI)
      .setCypherQuery(NEO4J_EDGE_QUERY)
      .setUsername(NEO4J_USERNAME)
      .setPassword(NEO4J_PASSWORD)
      .setConnectTimeout(NEO4J_CONNECT_TIMEOUT)
      .setReadTimeout(NEO4J_READ_TIMEOUT)
      .finish();

    // read rows from Neo4j
    DataSet<Tuple2<Integer, Integer>> rows = env.createInput(neoInput,
      new TupleTypeInfo<Tuple2<Integer, Integer>>(
        BasicTypeInfo.INT_TYPE_INFO,    // page id 1
        BasicTypeInfo.INT_TYPE_INFO));  // page id 2

    // convert rows into edges (tuple 3) and init with edge value 1.0
    DataSet<Tuple3<Integer, Integer, Double>> edges = rows
      .map(new MapFunction<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Double>>() {
        @Override
        public Tuple3<Integer, Integer, Double> map(Tuple2<Integer, Integer> row) throws Exception {
          return new Tuple3<>(row.f0, row.f1, 1.0);
        }
      }).withForwardedFields("f0;f1");

    // create Gelly graph
    Graph<Integer, Double, Double> graph = Graph
      .fromTupleDataSet(edges, new VertexInitializer(), env);

    // Run Page Rank for 5 iterations
    DataSet<Vertex<Integer, Double>> ranks =
      graph.run(new PageRank<Integer>(0.85, 5));

    // update vertices in Neo4j with computed pagerank
    Neo4jOutputFormat<Vertex<Integer, Double>> neoOutput = Neo4jOutputFormat
      .buildNeo4jOutputFormat()
      .setRestURI(NEO4J_REST_URI)
      .setUsername(NEO4J_USERNAME)
      .setPassword(NEO4J_PASSWORD)
      .setConnectTimeout(NEO4J_CONNECT_TIMEOUT)
      .setReadTimeout(NEO4J_READ_TIMEOUT)
      .setCypherQuery(NEO4J_UPDATE_QUERY)
      .addParameterKey("id")
      .addParameterKey("rank")
      .setTaskBatchSize(10_000)
      .finish();

    ranks.output(neoOutput);

    env.execute();

    System.out.println(String.format("Took: %d ms", env.getLastJobExecutionResult().getNetRuntime()));  }

  private static class VertexInitializer implements MapFunction<Integer, Double> {

    @Override
    public Double map(Integer integer) throws Exception {
      return 1.0;
    }
  }
}
