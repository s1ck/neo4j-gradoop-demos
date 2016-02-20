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
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.gradoop.io.graph.tuples.ImportEdge;
import org.gradoop.io.graph.tuples.ImportVertex;
import org.gradoop.model.impl.EPGMDatabase;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.algorithms.labelpropagation.GellyLabelPropagation;
import org.gradoop.model.impl.operators.aggregation.ApplyAggregation;
import org.gradoop.model.impl.operators.aggregation.functions.EdgeCount;
import org.gradoop.model.impl.operators.aggregation.functions.VertexCount;
import org.gradoop.model.impl.operators.combination.ReduceCombination;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.model.impl.properties.PropertyList;
import org.gradoop.util.GradoopFlinkConfig;

/**
 * Demo is using the LDBC SNB social network graph to read a subgraph from Neo4j
 * into Flink and uses Gradoop to perform graph analytics. See {@link #main} for
 * details
 *
 * For more info on the data generator: https://github.com/ldbc/ldbc_snb_datagen
 */
public class SocialNetworkGroupingDemo {

  public static final String NEO4J_REST_URI = "http://localhost:7474/db/data/";

  public static final String NEO4J_USERNAME = "neo4j";

  public static final String NEO4J_PASSWORD = "password";

  public static final String NEO4J_VERTEX_QUERY =
    "MATCH (n:person) RETURN id(n), n.gender, n.city, n.birthday";

  public static final String NEO4J_EDGE_QUERY =
    "MATCH (a:person)-[e]->(b:person) RETURN id(e), id(a), id(b), type(e)";

  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

    // enter Gradoop
    EPGMDatabase<GraphHeadPojo, VertexPojo, EdgePojo> epgmDatabase =
      EPGMDatabase.fromExternalGraph(
        // get vertices from Neo4j
        getImportVertices(env),
        // get edges from Neo4j
        getImportEdges(env), GradoopFlinkConfig.createDefaultConfig(env));

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> result =
      epgmDatabase.getDatabaseGraph()
        // run community detection using 'birthday' property as propagation value
        .callForGraph(new GellyLabelPropagation<GraphHeadPojo, VertexPojo, EdgePojo>(4, "birthday"))
        // split the resulting graph into a graph collection
        .splitBy("birthday")
        // compute vertex counts for each community
        .apply(new ApplyAggregation<>("vertexCount", new VertexCount<GraphHeadPojo, VertexPojo, EdgePojo>()))
        // select communities with more than 50 users
        .select(new FilterFunction<GraphHeadPojo>() {
          @Override
          public boolean filter(GraphHeadPojo graphHead) throws Exception {
            return graphHead.getPropertyValue("vertexCount").getLong() > 50;
          }
        })
        // combine those communities to a single graph
        .reduce(new ReduceCombination<GraphHeadPojo, VertexPojo, EdgePojo>())
        // group the graph
        .groupByVertexAndEdgeLabelAndVertexProperties(Lists.newArrayList("city", "gender"))
        // compute number of vertices
        .aggregate("vertexCount", new VertexCount<GraphHeadPojo, VertexPojo, EdgePojo>())
        // compute number of edges
        .aggregate("edgeCount", new EdgeCount<GraphHeadPojo, VertexPojo, EdgePojo>());

    // write to JSON
    result.writeAsJson("output/vertices.json", "output/edges.json", "output/graphHeads.json");
  }


  @SuppressWarnings("unchecked")
  public static DataSet<ImportVertex<Integer>> getImportVertices(
    ExecutionEnvironment env) {

    Neo4jInputFormat<Tuple4<Integer, String, String, Long>> neoInput =
      Neo4jInputFormat.buildNeo4jInputFormat().setRestURI(NEO4J_REST_URI)
        .setCypherQuery(NEO4J_VERTEX_QUERY).setUsername(NEO4J_USERNAME)
        .setPassword(NEO4J_PASSWORD).setConnectTimeout(10000)
        .setReadTimeout(10000).finish();

    DataSet<Tuple4<Integer, String, String, Long>> rows = env.createInput(neoInput,
      new TupleTypeInfo<Tuple4<Integer, String, String, Long>>(
        BasicTypeInfo.INT_TYPE_INFO,      // vertex id
        BasicTypeInfo.STRING_TYPE_INFO,   // vertex property gender
        BasicTypeInfo.STRING_TYPE_INFO,   // vertex property city
        BasicTypeInfo.LONG_TYPE_INFO));   // vertex property birthday

    return rows.map(new BuildImportVertex());
  }

  @SuppressWarnings("unchecked")
  public static DataSet<ImportEdge<Integer>> getImportEdges(
    ExecutionEnvironment env) {
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

    return rows.map(new BuildImportEdge());
  }

  public static class BuildImportVertex implements
    MapFunction<Tuple4<Integer, String, String, Long>, ImportVertex<Integer>> {

    private final ImportVertex<Integer> importVertex = new ImportVertex<>();

    @Override
    public ImportVertex<Integer> map(Tuple4<Integer, String, String, Long> row)
      throws Exception {
      importVertex.setId(row.f0);
      importVertex.setLabel("Person");
      PropertyList properties = PropertyList.createWithCapacity(2);
      properties.set("gender", row.f1);
      properties.set("city", row.f2);
      properties.set("birthday", row.f3);
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
      importEdge.setSourceVertexId(row.f1);
      importEdge.setTargetVertexId(row.f2);
      importEdge.setProperties(PropertyList.createWithCapacity(0));

      return importEdge;
    }
  }
}
