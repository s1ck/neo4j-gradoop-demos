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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.library.PageRank;

/**
 * Reads the DBPedia knowledge graph edge-wise from Neo4j and computes the Page
 * Rank for each vertex.
 */
public class DBPediaPageRankDemo {

  public static final String NEO4J_REST_URI = "http://localhost:7474/db/data/";

  public static final String NEO4J_USERNAME = "neo4j";

  public static final String NEO4J_PASSWORD = "password";

  public static final String NEO4J_EDGE_QUERY = "MATCH (p1:Page)-[:Link]->(p2) RETURN id(p1), id(p2)";

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    Neo4jInputFormat<Tuple2<Integer, Integer>> neoInput = Neo4jInputFormat
      .buildNeo4jInputFormat()
      .setRestURI(NEO4J_REST_URI)
      .setCypherQuery(NEO4J_EDGE_QUERY)
      .setUsername(NEO4J_USERNAME)
      .setPassword(NEO4J_PASSWORD)
      .setConnectTimeout(10000)
      .setReadTimeout(10000)
      .finish();

    // read edges and init with weight 1.0
    DataSet<Tuple3<Integer, Integer, Double>> edges = env.createInput(neoInput,
      new TupleTypeInfo<Tuple2<Integer, Integer>>(
        BasicTypeInfo.INT_TYPE_INFO,  // page id 1
        BasicTypeInfo.INT_TYPE_INFO)) // page id 2
     .map(new MapFunction<Tuple2<Integer, Integer>, Tuple3<Integer, Integer, Double>>() {
        @Override
        public Tuple3<Integer, Integer, Double> map(Tuple2<Integer, Integer> row) throws Exception {
          return new Edge<>(row.f0, row.f1, 1.0);
        }
      }).withForwardedFields("f0;f1");

    // create Gelly graph and run Page Rank
    Graph<Integer, Double, Double> graph = Graph
      .fromTupleDataSet(edges, new VertexInitializer(), env);
    graph.run(new PageRank<Integer>(0.85, 5)).collect();
  }

  private static class VertexInitializer implements MapFunction<Integer, Double> {

    @Override
    public Double map(Integer integer) throws Exception {
      return 1.0;
    }
  }
}
