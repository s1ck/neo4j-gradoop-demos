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

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.neo4j.Neo4jInputFormat;
import org.apache.flink.api.java.io.neo4j.Neo4jOutputFormat;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.flink.algorithms.labelpropagation.GradoopLabelPropagation;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.graph.GraphDataSource;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.aggregation.ApplyAggregation;
import org.gradoop.flink.model.impl.operators.aggregation.functions.count.VertexCount;
import org.gradoop.flink.model.impl.operators.combination.ReduceCombination;
import org.gradoop.flink.util.GradoopFlinkConfig;


/**
 * Demo is using the LDBC SNB social network graph to read a subgraph from Neo4j
 * into Flink and uses Gradoop to perform graph analytics. See {@link #main} for
 * details. The resulting graph is written to Neo4j for visualization / inspection.
 *
 * For more info on the data generator: https://github.com/ldbc/ldbc_snb_datagen
 */
public class CommunityRollUpDemo {

  public static final String NEO4J_INPUT_REST_URI = "http://localhost:7474/db/data/";

  public static final String NEO4J_OUTPUT_REST_URI = "http://localhost:4242/db/data/";

  public static final String NEO4J_USERNAME = "neo4j";

  public static final String NEO4J_PASSWORD = "password";

  public static final Integer NEO4J_CONNECT_TIMEOUT = 10_000;

  public static final Integer NEO4J_READ_TIMEOUT = 10_000;

  /**
   * Read all vertices of type 'person', return their id and property 'gender'
   */
  public static final String NEO4J_VERTEX_QUERY =
    "CYPHER RUNTIME=COMPILED " +
      "MATCH (n:person) " +
      "RETURN id(n), n.gender";

  /**
   * Read all edges of type 'knows' between vertices of type 'person', return
   * edge id, source vertex id and target vertex id
   */
  public static final String NEO4J_EDGE_QUERY =
    "CYPHER RUNTIME=COMPILED " +
      "MATCH (a:person)-[e]->(b:person) " +
      "RETURN id(e), id(a), id(b), type(e)";

  /**
   * Create a new graph from insert tuples representing edges. Each tuple
   * consists of:
   *
   * - source EPGM id, community id, gender and count
   * - target EPGM id, community id, gender and count
   * - edge and count
   */
  public static final String NEO4J_CREATE_QUERY = "" +
    "UNWIND {tuples} as t " +
    "MERGE (a:UserGroup {epgmId : t.f, community : t.fC, gender : t.fG, count : t.fCnt}) " +
    "MERGE (b:UserGroup {epgmId : t.t, community : t.tC, gender : t.tG, count : t.tCnt}) " +
    "CREATE (a)-[:KNOWS {count : t.eCnt}]->(b)";

  // used to store external identifiers at the EPGM vertices/edges
  public static final String COMMUNITY_KEY = "_id";

  public static void main(String[] args) throws Exception {

    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

    // initialize Gradoop database from Neo4j graph
    DataSource dataSource = new GraphDataSource<>(
      // get vertices from Neo4j
      getImportVertices(env),
      // get edges from Neo4j
      getImportEdges(env),
      // store the external (Neo4j) identifier at the vertices / edges using that key
      COMMUNITY_KEY,
      // gradoop config
      config);
    
    LogicalGraph neoGraph = dataSource.getLogicalGraph();


    LogicalGraph groupedGraph = neoGraph
        // run community detection using the external id as propagation value
        .callForGraph(new GradoopLabelPropagation(4, COMMUNITY_KEY))
        // split the resulting graph into a graph collection using the community id
        .splitBy(COMMUNITY_KEY)
        // compute vertex count for each community
        .apply(new ApplyAggregation("vertexCount", new VertexCount()))
        // select communities with more than 100 users
        .select(new FilterFunction<GraphHead>() {
          @Override
          public boolean filter(GraphHead graphHead) throws Exception {
            return graphHead.getPropertyValue("vertexCount").getLong() > 100;
          }
        })
        // combine those communities to a single graph
        .reduce(new ReduceCombination())
        // group the graph by vertex properties 'community id' and 'gender'
        .groupBy(Lists.newArrayList("gender", COMMUNITY_KEY));

    // write vertices and edges to Neo4j
    writeTriplets(Utils.buildTriplets(groupedGraph));

    env.execute();

    System.out.println(String.format("Took: %d ms", env.getLastJobExecutionResult().getNetRuntime()));
  }

  @SuppressWarnings("unchecked")
  public static DataSet<ImportVertex<Integer>> getImportVertices(
    ExecutionEnvironment env) {

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
        BasicTypeInfo.INT_TYPE_INFO,      // vertex id
        BasicTypeInfo.STRING_TYPE_INFO,   // vertex property gender
        BasicTypeInfo.STRING_TYPE_INFO)); // vertex property city

    return rows.map(new BuildImportVertex());
  }

  @SuppressWarnings("unchecked")
  public static DataSet<ImportEdge<Integer>> getImportEdges(
    ExecutionEnvironment env) {
    Neo4jInputFormat<Tuple4<Integer, Integer, Integer, String>> neoInput =
      Neo4jInputFormat.buildNeo4jInputFormat()
        .setRestURI(NEO4J_INPUT_REST_URI)
        .setCypherQuery(NEO4J_EDGE_QUERY)
        .setUsername(NEO4J_USERNAME)
        .setPassword(NEO4J_PASSWORD)
        .setConnectTimeout(NEO4J_CONNECT_TIMEOUT)
        .setReadTimeout(NEO4J_READ_TIMEOUT)
        .finish();

    DataSet<Tuple4<Integer, Integer, Integer, String>> rows = env
      .createInput(neoInput,
        new TupleTypeInfo<Tuple4<Integer, Integer, Integer, String>>(
          BasicTypeInfo.INT_TYPE_INFO,      // edge id
          BasicTypeInfo.INT_TYPE_INFO,      // source vertex id
          BasicTypeInfo.INT_TYPE_INFO,      // target vertex id
          BasicTypeInfo.STRING_TYPE_INFO)); // edge label

    return rows.map(new BuildImportEdge());
  }

  @SuppressWarnings("unchecked")
  public static void writeTriplets(DataSet<Tuple3<Vertex, Edge, Vertex>> triplets) {
    triplets.map(new MapFunction<Tuple3<Vertex,Edge,Vertex>, Tuple9<String, Integer, String, Long, String, Integer, String, Long, Long>>() {
      @Override
      public Tuple9<String, Integer, String, Long, String, Integer, String, Long, Long> map(
        Tuple3<Vertex, Edge, Vertex> triplet) throws Exception {
        return new Tuple9<>(
          triplet.f0.getId().toString(),
          triplet.f0.getPropertyValue(COMMUNITY_KEY).getInt(),
          triplet.f0.getPropertyValue("gender").getString(),
          triplet.f0.getPropertyValue("count").getLong(),
          triplet.f2.getId().toString(),
          triplet.f2.getPropertyValue(COMMUNITY_KEY).getInt(),
          triplet.f2.getPropertyValue("gender").getString(),
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
      .addParameterKey(0, "f")    // from
      .addParameterKey(1, "fC")   // from community
      .addParameterKey(2, "fG")   // from gender
      .addParameterKey(3, "fCnt") // from count
      .addParameterKey(4, "t")    // to
      .addParameterKey(5, "tC")   // to community
      .addParameterKey(6, "tG")   // to gender
      .addParameterKey(7, "tCnt") // to count
      .addParameterKey(8, "eCnt") // edge count
      .finish()).setParallelism(1);
  }

  public static class BuildImportVertex implements
    MapFunction<Tuple3<Integer, String, String>, ImportVertex<Integer>> {

    private final ImportVertex<Integer> importVertex = new ImportVertex<>();

    @Override
    public ImportVertex<Integer> map(Tuple3<Integer, String, String> row)
      throws Exception {
      importVertex.setId(row.f0);
      importVertex.setLabel("Person");
      PropertyList properties = PropertyList.createWithCapacity(2);
      properties.set("gender", row.f1);
      properties.set("city", row.f2);
      importVertex.setProperties(properties);
      return importVertex;
    }
  }

  public static class BuildImportEdge implements
    MapFunction<Tuple4<Integer, Integer, Integer, String>, ImportEdge<Integer>> {

    private final ImportEdge<Integer> importEdge = new ImportEdge<>();

    @Override
    public ImportEdge<Integer> map(Tuple4<Integer, Integer, Integer, String> row)
      throws Exception {
      importEdge.setId(row.f0);
      importEdge.setLabel(row.f3);
      importEdge.setSourceId(row.f1);
      importEdge.setTargetId(row.f2);
      importEdge.setProperties(PropertyList.createWithCapacity(0));

      return importEdge;
    }
  }
}
