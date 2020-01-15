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

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.empty.EmptyGraph;
import org.janusgraph.core.ConfiguredGraphFactory;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JanusDAO {

	private GraphTraversalSource g;

	// private int DEFAULT_TTL_DAYS;

	private String CONFIG_FILE;
	private Logger logger = LoggerFactory.getLogger(ConfigHandler.class);
	
	private String GRAPH_NAME = "janus_test";

	public JanusDAO(String configFIle, int ttlDays) throws Exception {
		CONFIG_FILE = configFIle;

		File file = new File(CONFIG_FILE);

		logger.info("Loading config from: " + CONFIG_FILE);

		if (!file.exists()) {
			logger.error("Cannot find: " + CONFIG_FILE);
			System.exit(0);
		}

		g = EmptyGraph.instance().traversal().withRemote("conf/");
		// DEFAULT_TTL_DAYS = ttlDays; TODO: implement this later
		
		logger.info("Testing the graph connection....");
		

		Long vertexCount = g.V().count().next();
		Long edgeCount = g.E().count().next();
		
		logger.info(String.format("Number of vertices is: %d And number of edges is: %d", vertexCount, edgeCount));

	}

	public synchronized void linkNodes(String node1Label, String node2Label, String node1PropertyKey,
			String node1PropertyValue, String node2PropertyKey, String node2PropertyValue, String edgeName) {

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
			g.V(a).as("from").V(b).addE(edgeName).from("from").property("created", currentTime).next();
		} else if (node1IsNew && !node2IsNew) {
			logger.debug("Only node1 is new " + node1PropertyValue + " : " + node2PropertyValue);
			tx.getVertex(a.longId()).addEdge(edgeName, tx.getVertex(b.longId()), "created", currentTime);
		} else if (!node1IsNew && node2IsNew) {
			logger.debug("Only node2 is new " + node1PropertyValue + " : " + node2PropertyValue);
			tx.getVertex(a.longId()).addEdge(edgeName, tx.getVertex(b.longId()), "created", currentTime);
		} else if (!node1IsNew && !node2IsNew) {
			boolean edgeExists = tx.traversal().V().has(node1PropertyKey, node1PropertyValue).hasLabel(node1Label)
					.outE(edgeName).inV().has(node2PropertyKey, node2PropertyValue).hasNext();
			logger.debug("Both nodes are not new, does the edge between them already exist?: " + edgeExists);

			if (!edgeExists) {
				logger.debug("Creating a new edge between " + node1PropertyValue + " : " + node2PropertyValue);
				tx.getVertex(a.longId()).addEdge(edgeName, tx.getVertex(b.longId()), "created", currentTime);
			} else {
				// TODO figure out how to set TTL
			}
		}



	}

	public boolean nodeExists(String key1, String prop1, String key2, String prop2) {
		return true;
	}

}
