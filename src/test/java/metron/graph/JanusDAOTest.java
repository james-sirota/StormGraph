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
import java.io.FileNotFoundException;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.hadoop.hbase.shaded.org.junit.Before;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.diskstorage.BackendException;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class JanusDAOTest extends TestCase {

	private String node1type;
	private String node2type;
	private static final Logger logger = LoggerFactory.getLogger(JanusDAOTest.class);
	String filename;
	JanusGraph g;
	JanusDAO jd;

	public JanusDAOTest(String testName) {
		super(testName);
	}

	public static Test suite() {
		return new TestSuite(JanusDAOTest.class);
	}

	@Before
	public synchronized void setUp() throws Exception {

		node1type = "test1";
		node2type = "test2";

		filename = "src/test/resources/janusgraph-cassandra-es.properties";
		g = JanusGraphFactory.open(filename);
		jd = new JanusDAO(filename, 5);

		File file = new File(filename);

		logger.info("Loading config from: " + filename);

		if (!file.exists()) {
			logger.error("Cannot find: " + filename);
			throw new FileNotFoundException("Cannot find: " + filename);
		}
	}

	public synchronized void testA_SingleNodeCreation() throws ConfigurationException, InterruptedException {

		JanusGraphTransaction tx = g.newTransaction();

		jd.linkNodes(node1type, node2type, "test_ipAddress", "test_1.1.1.1", "test_ipAddress", "test_12.12.12.12",
				"test_connectsTo");

		Long result = tx.traversal().V().has("test_ipAddress", "test_1.1.1.1").hasLabel(node1type)
				.outE("test_connectsTo").count().next();
		 assertTrue(result == 1);

		String label1 = tx.traversal().V().has("test_ipAddress", "test_1.1.1.1").hasLabel(node1type).label().next();
		assertTrue(label1.equals(node1type));

		String edgeLabel = tx.traversal().V().has("test_ipAddress", "test_1.1.1.1").hasLabel(node1type).outE().next()
				.label();
		assertTrue(edgeLabel.equals("test_connectsTo"));

		String label2 = tx.traversal().V().has("test_ipAddress", "test_1.1.1.1").hasLabel(node1type).outE().inV()
				.label().next();

		assertTrue(label2.equals(node2type));

		String ip = tx.traversal().V().has("test_ipAddress", "test_1.1.1.1").hasLabel(node1type).outE().inV().next()
				.value("test_ipAddress").toString();

		assertTrue(ip.equals("test_12.12.12.12"));
		tx.commit();
		tx.close();

	}

	public synchronized void testB_DuplicateNodeCreation() throws ConfigurationException, InterruptedException {

		JanusGraphTransaction tx = g.newTransaction();
		
		jd.linkNodes(node1type, node2type, "test_ipAddress", "test_1.1.1.1", "test_ipAddress", "test_12.12.12.12",
				"test_connectsTo");
		jd.linkNodes(node1type, node2type, "test_ipAddress", "test_1.1.1.1", "test_ipAddress", "test_12.12.12.12",
				"test_connectsTo");
		jd.linkNodes(node1type, node2type, "test_ipAddress", "test_1.1.1.1", "test_ipAddress", "test_12.12.12.12",
				"test_connectsTo");

		Long result = tx.traversal().V().has("test_ipAddress", "test_1.1.1.1").hasLabel(node1type)
				.outE("test_connectsTo").count().next();
		assertTrue(result == 1);
		tx.commit();
		tx.close();
	}

	public synchronized void testC_DifferentNode2() throws ConfigurationException, InterruptedException {

		JanusGraphTransaction tx = g.newTransaction();

		jd.linkNodes(node1type, node2type, "test_ipAddress", "test_1.1.1.1", "test_ipAddress", "test_12.12.12.13",
				"test_connectsTo");

		Long result = tx.traversal().V().has("test_ipAddress", "test_1.1.1.1").hasLabel(node1type)
				.outE("test_connectsTo").count().next();
		assertTrue(result == 2);

		jd.linkNodes(node1type, node2type, "test_ipAddress", "test_1.1.1.1", "test_ipAddress", "test_12.12.12.13",
				"test_connectsTo");
		
		result = tx.traversal().V().has("test_ipAddress", "test_1.1.1.1").hasLabel(node1type)
				.outE("test_connectsTo").count().next();
		
		assertTrue(result == 2);


		tx.commit();
		tx.close();

	}

	public synchronized void testD_DifferentNode1() throws ConfigurationException, InterruptedException {

		JanusGraphTransaction tx = g.newTransaction();
		jd.linkNodes(node1type, node2type, "test_ipAddress", "test_1.1.1.14", "test_ipAddress", "test_12.12.12.13",
				"test_connectsTo");

		Long result = tx.traversal().V().has("test_ipAddress", "test_1.1.1.14").hasLabel(node1type)
				.outE("test_connectsTo").count().next();
		assertTrue(result == 1);

		GraphTraversal<Vertex, Vertex> a = tx.traversal().V().has("test_ipAddress", "test_1.1.1.14").hasLabel(node1type)
				.outE().inV();

		assertTrue(a.next().value("test_ipAddress").toString().equals("test_12.12.12.13"));
		
		tx.commit();
		tx.close();

	}
	
	public synchronized void testE_TestSecondRelationType() throws ConfigurationException, InterruptedException {

		JanusGraphTransaction tx = g.newTransaction();
		
		jd.linkNodes("user", node2type, "username", "test_user_55", "test_ipAddress", "test_12.12.12.13",
				"test_uses");

		Long result = tx.traversal().V().has("username", "test_user_55").hasLabel("user")
				.outE("test_uses").count().next();
		assertTrue(result == 1);

		GraphTraversal<Vertex, Vertex> a = tx.traversal().V().has("username", "test_user_55").hasLabel("user")
				.outE().inV();

		assertTrue(a.next().value("test_ipAddress").toString().equals("test_12.12.12.13"));
		
		
		Long inConnects = tx.traversal().V().has("test_ipAddress", "test_12.12.12.13").inE("test_connectsTo").count().next();
		
		assertTrue(inConnects == 2);
		
		Long inUsers = tx.traversal().V().has("test_ipAddress", "test_12.12.12.13").inE("test_uses").count().next();
		
		assertTrue(inUsers == 1);
		
		tx.commit();
		tx.close();

	}

	public synchronized void testZ_CleanUp() throws BackendException {
		JanusGraphTransaction tx = g.newTransaction();
		tx.traversal().V().propertyMap("test_ipAddress").V().drop().iterate();
		tx.traversal().E().has("test_connectsTo").drop().iterate();

		tx.commit();
		tx.close();
	}

}
