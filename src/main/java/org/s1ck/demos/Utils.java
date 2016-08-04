package org.s1ck.demos;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.SourceId;


public class Utils {

  /**
   * Returns the graph as a dataset of triples, where each triple consists of
   * source vertex, edge and target vertex.
   *
   * @param graph logical graph
   * @return triplet dataset
   */
  public static DataSet<Tuple3<Vertex, Edge, Vertex>> buildTriplets(LogicalGraph graph) {
    return graph.getVertices()
      .join(graph.getEdges())
      .where(new Id<Vertex>()).equalTo(new SourceId<>())
      .with(new JoinFunction<Vertex, Edge, Tuple2<Vertex, Edge>>() {

        @Override
        public Tuple2<Vertex, Edge> join(Vertex vertexPojo,
          Edge edgePojo) throws Exception {
          return new Tuple2<>(vertexPojo, edgePojo);
        }
      })
      .join(graph.getVertices())
      .where("1.targetId").equalTo(new Id<Vertex>())
      .with(new JoinFunction<Tuple2<Vertex,Edge>, Vertex, Tuple3<Vertex, Edge, Vertex>>() {

        @Override
        public Tuple3<Vertex, Edge, Vertex> join(
          Tuple2<Vertex, Edge> sourceVertexAndEdge,
          Vertex targetVertex) throws Exception {
          return new Tuple3<>(
            sourceVertexAndEdge.f0, sourceVertexAndEdge.f1, targetVertex);
        }
      });
  }
}
