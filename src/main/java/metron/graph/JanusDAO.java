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

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;


import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;

public class JanusDAO implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -8968557226200658053L;
	private String CONFIG_FILE;
	private String KEY_ID;
	private JanusGraph g;
	private JanusGraphManagement management;
	private GraphTraversalSource traversal;
	private int DEFAULT_TTL_DAYS;

	Logger logger = LogManager.getLogger(JanusDAO.class);

	public JanusDAO(String configFIle, int ttlDays) {
		CONFIG_FILE = configFIle;
		g = JanusGraphFactory.open(CONFIG_FILE);
		management = g.openManagement();
		traversal = g.traversal();
		KEY_ID = "nodeName";
		DEFAULT_TTL_DAYS = ttlDays;

	}

	public void createConnectsRelationshipSchema() {

		// index by KEY_ID, make composite
		final PropertyKey src = management.makePropertyKey(KEY_ID).dataType(String.class).make();
		management.setTTL(src, Duration.ofDays(DEFAULT_TTL_DAYS));
		JanusGraphManagement.IndexBuilder srcIdx = management.buildIndex(KEY_ID, Vertex.class).addKey(src);
		srcIdx.unique();
		JanusGraphIndex srcIndex = srcIdx.buildCompositeIndex();
		management.setConsistency(srcIndex, ConsistencyModifier.LOCK);

		// index edges
		final PropertyKey connects = management.makePropertyKey(EdgeTypes.CONNECTS_TO).dataType(Integer.class).make();
		management.setTTL(connects, Duration.ofDays(DEFAULT_TTL_DAYS));
		management.buildIndex(EdgeTypes.CONNECTS_TO, Edge.class).addKey(connects).buildCompositeIndex();
		management.makeEdgeLabel(EdgeTypes.CONNECTS_TO).multiplicity(Multiplicity.MULTI).make();

		final PropertyKey uses = management.makePropertyKey(EdgeTypes.USES).dataType(Integer.class).make();
		management.setTTL(uses, Duration.ofDays(DEFAULT_TTL_DAYS));
		management.buildIndex(EdgeTypes.USES, Edge.class).addKey(uses).buildCompositeIndex();
		management.makeEdgeLabel(EdgeTypes.USES).multiplicity(Multiplicity.MULTI).make();

		management.commit();

	}

	public void linkNodes(String source, String relation, String dest, String node1type, String node2type) {
		Vertex src;
		Vertex dst;

		if (!relationExists(source, relation, dest)) {
			
			logger.trace("Relation does not exist: " + source + " -> " + relation + " -> " + dest);
			
			if (!vertexExists(source)) {
				logger.trace("New Vertex Detected: " + source);
				JanusGraphTransaction tx = g.newTransaction();
				src = tx.addVertex(KEY_ID, source, T.label, node1type);
				tx.commit();
			}

			if (!vertexExists(dest)) {
				logger.trace("New Vertex Detected: " + dest);
				JanusGraphTransaction tx = g.newTransaction();
				dst = tx.addVertex(KEY_ID, dest, T.label, node2type);
				tx.commit();
			}

			JanusGraphTransaction tx = g.newTransaction();
			
			logger.trace("Creating vertex: " + KEY_ID + " -> " + source);
			src = traversal.V().has(KEY_ID, source).next();
			
			logger.trace("Creating vertex: " + KEY_ID + " -> " + dest);
			dst = traversal.V().has(KEY_ID, dest).next();

			logger.trace("Creating edge: " + relation);
			src.addEdge(relation, dst).bothVertices();

			tx.commit();
			tx.close();
		} else {
			// [TODO]if relation exists, recreate it to reset the TTL
			logger.trace("Relation already exists: " + source + " -> " + relation + " -> " + dest);
		}

	}

	public ArrayList<String> getEdgesForVertex(String hostname) {

		ArrayList<String> edgeList = new ArrayList<String>();
		Vertex fromNode = traversal.V().has(KEY_ID, hostname).next();

		Iterator<Edge> edg = fromNode.edges(Direction.BOTH);

		while (edg.hasNext()) {
			String e = edg.next().label();
			logger.trace("New edge found: " + e);
			edgeList.add(e);

		}

		return edgeList;

	}

	public boolean relationExists(String hostname, String edge, String dst) {

		GraphTraversal<Vertex, Vertex> fromNode = traversal.V().has(KEY_ID, hostname);

		if (!fromNode.hasNext())
			return false;

		GraphTraversal<Vertex, Vertex> toNode = traversal.V().has(KEY_ID, dst);

		if (!toNode.hasNext())
			return false;

		if (!fromNode.next().edges(Direction.OUT, edge).hasNext())
			return false;

		if (!toNode.next().edges(Direction.IN, edge).hasNext())
			return false;

		return true;

	}

	public boolean vertexExists(String hostname) {

		if (traversal.V().has(KEY_ID, hostname).hasNext())
			return true;
		else
			return false;
	}

	public ArrayList<String> getConnection(String hostname, String edge) {
		ArrayList<String> nodeList = new ArrayList<String>();
		GraphTraversal<Vertex, Vertex> fromNode = traversal.V().has(KEY_ID, hostname);

		Vertex nn = fromNode.next();

		Iterator<Edge> ee1 = nn.edges(Direction.OUT, edge);
		while (ee1.hasNext()) {
			String nName = (String) ee1.next().inVertex().value(KEY_ID);

			if (!nName.equals(hostname))
				nodeList.add(nName);
		}

		Iterator<Edge> ee2 = nn.edges(Direction.IN, edge);
		while (ee2.hasNext()) {
			String nName = (String) ee2.next().outVertex().value(KEY_ID);

			if (!nName.equals(hostname))
				nodeList.add(nName);
		}

		return nodeList;

	}

}
