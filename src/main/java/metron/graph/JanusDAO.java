/*
    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package metron.graph;

import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;

import static org.apache.tinkerpop.gremlin.process.traversal.Compare.eq;
import static org.apache.tinkerpop.gremlin.process.traversal.P.eq;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal.Symbols.unfold;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.addE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.unfold;

public class JanusDAO {

	private GraphTraversalSource g;

	// private int DEFAULT_TTL_DAYS;

	private Logger logger = LoggerFactory.getLogger(ConfigHandler.class);
	
	private String GRAPH_NAME = "janus_test";

	public JanusDAO(String configFIle, int ttlDays) throws Exception {

		File file = new File(configFIle);

		logger.info("Loading config from: " + configFIle);

		if (!file.exists()) {
			logger.error("Cannot find: " + configFIle);
			System.exit(0);
		}

		g = EmptyGraph.instance().traversal().withRemote(configFIle);
		// DEFAULT_TTL_DAYS = ttlDays; TODO: implement this later

		logger.info("Testing the graph connection....");
		

		Long vertexCount = g.V().count().next();
		Long edgeCount = g.E().count().next();
		
		logger.info(String.format("Number of vertices is: %d And number of edges is: %d", vertexCount, edgeCount));

	}

	public synchronized void singleTraversalLinkNodes(String node1Label, String node2Label, String node1PropertyKey,
									   String node1PropertyValue, String node2PropertyKey, String node2PropertyValue, String edgeLabel) {

		String currentTime = String.valueOf(System.currentTimeMillis());

		g
				// Upsert vertex 1
				.V().has(node1Label, node1PropertyKey, node1PropertyValue).fold()
				.coalesce(
						__.unfold(),
						__.addV(node1Label)
								.property(node1PropertyKey, node1PropertyValue))
				.property("created", currentTime).aggregate("from")
				// Upser vertex 2
				.V().has(node2Label, node2PropertyKey, node2PropertyValue).fold()
				.coalesce(
						__.unfold(),
						__.addV(node2Label)
								.property(node2PropertyKey, node2PropertyValue))
				.property("created", currentTime).as("to")
				// Upsert edge
				.select("from").unfold()
				.coalesce(
						out(edgeLabel).where(eq("to")).inE(),
						addE(edgeLabel).to("to").property("created", currentTime)).iterate();
	}

	public synchronized void linkNodes(String node1Label, String node2Label, String node1PropertyKey,
			String node1PropertyValue, String node2PropertyKey, String node2PropertyValue, String edgeLabel) {

		String currentTime = String.valueOf(System.currentTimeMillis());

		Vertex a = null;
		Vertex b = null;

		boolean node1IsNew = false;
		boolean node2IsNew = false;


		logger.debug("Examining vertex: " + node1Label + " : " + node1PropertyKey + " : " + node1PropertyValue);
		logger.debug("Examining vertex: " + node2Label + " : " + node2PropertyKey + " : " + node2PropertyValue);

		boolean sourceNodeExists = g.V().has(node1PropertyKey, node1PropertyValue).hasLabel(node1Label)
				.hasNext();
		if (!sourceNodeExists) {
			logger.debug("Creating new source node because does not exist: " + node1Label + " : " + node1PropertyKey
					+ " : " + node1PropertyValue);

//			a = tx.addVertex(T.label, node1Label, node1PropertyKey, node1PropertyValue, "created", currentTime);
			a = g.addV(node1Label)
					.property(node1PropertyKey, node1PropertyValue)
					.property("created", currentTime).next();
			logger.debug("Created an outVertex: " + node1PropertyValue + " with node id " + a.id());

			node1IsNew = true;

		}

		boolean destNodeExists = g.V().has(node2PropertyKey, node2PropertyValue).hasLabel(node2Label)
				.hasNext();

		if (!destNodeExists) {
			logger.debug("Creating new dest node because does not exist: " + node2Label + " : " + node2PropertyKey
					+ " : " + node2PropertyValue);
//			b = tx.addVertex(T.label, node2Label, node2PropertyKey, node2PropertyValue, "created", currentTime);
			b = g.addV(node2Label)
					.property(node2PropertyKey, node2PropertyValue)
					.property("created", currentTime).next();
			logger.debug("Created an inVertex:" + node2PropertyValue + " with node id " + b.id());

			node2IsNew = true;

		}
		a = g.V().has(node1PropertyKey, node1PropertyValue).hasLabel(node1Label).next();
		b = g.V().has(node2PropertyKey, node2PropertyValue).hasLabel(node2Label).next();

		if (node1IsNew && node2IsNew) {
			logger.debug("Both nodes are new " + node1PropertyValue + " : " + node2PropertyValue);
//			tx.getVertex(a.longId()).addEdge(edgeName, tx.getVertex(b.longId()), "created", currentTime);
			g.V(a).as("from").V(b).addE(edgeLabel).from("from").property("created", currentTime).next();
		} else if (node1IsNew && !node2IsNew) {
			logger.debug("Only node1 is new " + node1PropertyValue + " : " + node2PropertyValue);
//			tx.getVertex(a.longId()).addEdge(edgeName, tx.getVertex(b.longId()), "created", currentTime);
			g.V(a).as("from").V(b).addE(edgeLabel).from("from").property("created", currentTime).next();
		} else if (!node1IsNew && node2IsNew) {
			logger.debug("Only node2 is new " + node1PropertyValue + " : " + node2PropertyValue);
//			tx.getVertex(a.longId()).addEdge(edgeName, tx.getVertex(b.longId()), "created", currentTime);
			g.V(a).as("from").V(b).addE(edgeLabel).from("from").property("created", currentTime).next();
		} else if (!node1IsNew && !node2IsNew) {
//			boolean edgeExists = tx.traversal().V().has(node1PropertyKey, node1PropertyValue).hasLabel(node1Label)
//					.outE(edgeName).inV().has(node2PropertyKey, node2PropertyValue).hasNext();
			boolean edgeExists = g.V(a).out(edgeLabel).is(b).hasNext();
			logger.debug("Both nodes are not new, does the edge between them already exist?: " + edgeExists);

			if (!edgeExists) {
				logger.debug("Creating a new edge between " + node1PropertyValue + " : " + node2PropertyValue);
//				tx.getVertex(a.longId()).addEdge(edgeName, tx.getVertex(b.longId()), "created", currentTime);
				g.V(a).as("from").V(b).addE(edgeLabel).from("from").property("created", currentTime).next();
			} else {
				// TODO figure out how to set TTL
			}
		}



	}

	public boolean nodeExists(String key1, String prop1, String key2, String prop2) {
		return true;
	}

}
