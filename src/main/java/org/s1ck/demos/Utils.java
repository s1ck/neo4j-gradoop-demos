package org.s1ck.demos;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.functions.epgm.SourceId;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;

public class Utils {

  /**
   * Returns the graph as a dataset of triples, where each triple consists of
   * source vertex, edge and target vertex.
   *
   * @param graph logical graph
   * @return triplet dataset
   */
  public static DataSet<Tuple3<VertexPojo, EdgePojo, VertexPojo>> buildTriplets(
    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> graph) {
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
}
