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

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.apache.log4j.Logger;

public class JanusDAO {

	private JanusGraph g;

	private int DEFAULT_TTL_DAYS;

	private String CONFIG_FILE;
	private String KEY_ID = "nodeName";
	private String GLOBAL_VERTEX_INDEX_NAME = "MetronGraphVertex";
	private String GLOBAL_EDGE_INDEX_NAME = "MetronGraphEdge";
	private Logger logger = Logger.getLogger(ConfigHandler.class);;

	public JanusDAO(String configFIle, int ttlDays) throws ConfigurationException, InterruptedException {
		CONFIG_FILE = configFIle;

		File file = new File(CONFIG_FILE);

		logger.info("Loading config from: " + CONFIG_FILE);

		if (!file.exists()) {
			logger.error("Cannot find: " + CONFIG_FILE);
			System.exit(0);
		}
		Configuration conf = new PropertiesConfiguration(CONFIG_FILE);

		g = JanusGraphFactory.open(conf);

		DEFAULT_TTL_DAYS = ttlDays;

	}

	public synchronized void linkNodes(String node1Label, String node2Label, String node1PropertyKey,
			String node1PropertyValue, String node2PropertyKey, String node2PropertyValue, String edgeName) {

		String currentTime = String.valueOf(System.currentTimeMillis());
		JanusGraphVertex a = null;
		JanusGraphVertex b = null;

		boolean node1IsNew = false;
		boolean node2IsNew = false;

		JanusGraphTransaction tx = g.newTransaction();

		System.out.println("Examining vertex: " + node1Label + " : " + node1PropertyKey + " : " + node1PropertyValue);
		System.out.println("Examining vertex:: " + node2Label + " : " + node2PropertyKey + " : " + node2PropertyValue);

		boolean sourceNodeExists = tx.traversal().V().has(node1PropertyKey, node1PropertyValue).hasLabel(node1Label)
				.hasNext();
		if (!sourceNodeExists) {
			logger.debug("Creating new source node because does not exist: " + node1Label + " : " + node1PropertyKey
					+ " : " + node1PropertyValue);

			a = tx.addVertex(T.label, node1Label, node1PropertyKey, node1PropertyValue, "created", currentTime);
			System.out.println("-----CREATED A: " + node1PropertyValue + " with node id " + a.longId());

			node1IsNew = true;

		}

		boolean destNodeExists = tx.traversal().V().has(node2PropertyKey, node2PropertyValue).hasLabel(node2Label)
				.hasNext();

		if (!destNodeExists) {
			logger.debug("Creating new dest node because does not exist: " + node2Label + " : " + node2PropertyKey
					+ " : " + node2PropertyValue);
			b = tx.addVertex(T.label, node2Label, node2PropertyKey, node2PropertyValue, "created", currentTime);
			System.out.println("-----CREATED B: " + node2PropertyValue + " with node id " + b.longId());

			node2IsNew = true;

		}
		a = (JanusGraphVertex) tx.traversal().V().has(node1PropertyKey, node1PropertyValue).hasLabel(node1Label).next();
		b = (JanusGraphVertex) tx.traversal().V().has(node2PropertyKey, node2PropertyValue).hasLabel(node2Label).next();

		if (node1IsNew && node2IsNew) {
			logger.debug("Both nodes are new " + node1PropertyValue + " : " + node2PropertyValue);
			tx.getVertex(a.longId()).addEdge(edgeName, tx.getVertex(b.longId()), "created", currentTime);
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

		tx.commit();
		tx.close();

	}

	public boolean nodeExists(String key1, String prop1, String key2, String prop2) {
		return true;
	}

}
