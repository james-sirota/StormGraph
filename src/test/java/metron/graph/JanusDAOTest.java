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

import java.util.ArrayList;

import org.apache.hadoop.hbase.shaded.org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class JanusDAOTest extends TestCase {

	private JanusDAO jd;
	private String node1type;
	private String node2type;
	private static final Logger logger = LoggerFactory.getLogger(JanusDAOTest.class);

	public JanusDAOTest(String testName) {
		super(testName);
	}

	public static Test suite() {
		return new TestSuite(JanusDAOTest.class);
	}

	@Before
	protected void setUp() throws Exception {
		
		
		node1type = "test1";
		node2type = "test2";
		
		String filename = "src/test/resources/janusgraph-cassandra-es.properties";
		jd = new JanusDAO(filename,5);

	}

	public void testSchemaCreation() {
		// jd.createConnectsRelationshipSchema();
		assertTrue(true);
	}

	public void testSingleNodeCreation() {

		jd.linkNodes("1.1.1.1", EdgeTypes.CONNECTS_TO, "2.2.2.2", node1type, node2type);
		assertTrue(jd.vertexExists("1.1.1.1"));
		assertTrue(jd.vertexExists("2.2.2.2"));
		assertFalse(jd.vertexExists("1.1.1.2"));
		assertTrue(jd.relationExists("1.1.1.1", EdgeTypes.CONNECTS_TO, "2.2.2.2"));
		assertFalse(jd.relationExists("1.1.1.2", EdgeTypes.CONNECTS_TO, "2.2.2.2"));
		assertFalse(jd.relationExists("1.1.1.1", EdgeTypes.USES, "2.2.2.2"));
	}
	
	public void checkRelationExists() {
		
		assertFalse(jd.relationExists("1.1.1.1", EdgeTypes.CONNECTS_TO, "2.2.2.2"));
		jd.linkNodes("1.1.1.1", EdgeTypes.CONNECTS_TO, "2.2.2.2", node1type, node2type);
		ArrayList<String> el = jd.getEdgesForVertex("1.1.1.1");
		assertTrue(el.size() == 1);
		assertTrue(el.get(0).equals(EdgeTypes.CONNECTS_TO));

		assertTrue(jd.relationExists("1.1.1.1", EdgeTypes.CONNECTS_TO, "2.2.2.2"));
		
	}

	public void testDuplicateInsertionEdge() {

		ArrayList<String> el = jd.getEdgesForVertex("1.1.1.1");
		

		jd.linkNodes("1.1.1.1", EdgeTypes.CONNECTS_TO, "2.2.2.2", node1type, node2type);
		el = jd.getEdgesForVertex("1.1.1.1");
		assertTrue(el.size() == 1);

		assertFalse(jd.relationExists("1.1.1.1", EdgeTypes.CONNECTS_TO, "3.3.3.3"));

		jd.linkNodes("1.1.1.1", EdgeTypes.CONNECTS_TO, "3.3.3.3", node1type, node2type);
		el = jd.getEdgesForVertex("1.1.1.1");
		assertTrue(el.size() == 2);

		el = jd.getEdgesForVertex("3.3.3.3");
		assertTrue(el.size() == 1);

		jd.linkNodes("1.1.1.1", EdgeTypes.CONNECTS_TO, "3.3.3.3", node1type, node2type);
		el = jd.getEdgesForVertex("1.1.1.1");
		assertTrue(el.size() == 2);

		assertTrue(jd.relationExists("1.1.1.1", EdgeTypes.CONNECTS_TO, "3.3.3.3"));

		jd.linkNodes("1.1.1.1", EdgeTypes.CONNECTS_TO, "4.4.4.4", node1type, node2type);
		el = jd.getEdgesForVertex("1.1.1.1");
		assertTrue(el.size() == 3);

		jd.linkNodes("1.1.1.1", EdgeTypes.CONNECTS_TO, "5.5.5.5", node1type, node2type);
		el = jd.getEdgesForVertex("1.1.1.1");
		assertTrue(el.size() == 4);

		jd.linkNodes("1.1.1.1", EdgeTypes.CONNECTS_TO, "5.5.5.5", node1type, node2type);
		el = jd.getEdgesForVertex("1.1.1.1");
		assertTrue(el.size() == 4);

		el = jd.getEdgesForVertex("5.5.5.5");
		assertTrue(el.size() == 1);

	}

	public void testNodeEdgeCombinations() {

		jd.linkNodes("1.1.1.1", EdgeTypes.CONNECTS_TO, "2.2.2.2", node1type, node2type);
		jd.linkNodes("1.1.1.1", EdgeTypes.CONNECTS_TO, "3.3.3.3", node1type, node2type);
		jd.linkNodes("1.1.1.1", EdgeTypes.CONNECTS_TO, "4.4.4.4", node1type, node2type);
		


		ArrayList<String> el = jd.getConnection("1.1.1.1", EdgeTypes.CONNECTS_TO);
		assertTrue(el.size() == 3);
		
		logger.debug("Checkpoint 2");

		assertTrue(el.get(0).equals("2.2.2.2"));
		assertTrue(el.get(1).equals("3.3.3.3"));
		assertTrue(el.get(2).equals("4.4.4.4"));
		
		logger.debug("Checkpoint 3");

		el = jd.getConnection("2.2.2.2", EdgeTypes.CONNECTS_TO);
		assertTrue(el.size() == 1);
		assertTrue(el.get(0).equals("1.1.1.1"));
		
		logger.debug("Checkpoint 4");

		jd.linkNodes("2.2.2.2", EdgeTypes.CONNECTS_TO, "3.3.3.3", node1type, node2type);
		el = jd.getConnection("2.2.2.2", EdgeTypes.CONNECTS_TO);
		assertTrue(el.size() == 2);
		
		logger.debug("Checkpoint 5");

		assertTrue(el.contains("1.1.1.1"));
		assertTrue(el.contains("3.3.3.3"));
		
		logger.debug("Checkpoint 6");

		el = jd.getConnection("3.3.3.3", EdgeTypes.CONNECTS_TO);
		assertTrue(el.size() == 2);
		assertTrue(el.contains("1.1.1.1"));
		assertTrue(el.contains("2.2.2.2"));
		
		logger.debug("Checkpoint 7");

		el = jd.getConnection("4.4.4.4", EdgeTypes.CONNECTS_TO);
		assertTrue(el.size() == 1);
		assertTrue(el.contains("1.1.1.1"));
		
		logger.debug("Checkpoint 8");
	}

	public void testRelationCombinations() {

		jd.linkNodes("1.1.1.1", EdgeTypes.CONNECTS_TO, "2.2.2.2", node1type, node2type);
		jd.linkNodes("user1", EdgeTypes.USES, "2.2.2.2", node1type, node2type);

		ArrayList<String> el = jd.getConnection("2.2.2.2", EdgeTypes.CONNECTS_TO);
		assertTrue(el.size() == 1);
		assertTrue(el.contains("1.1.1.1"));

		el = jd.getConnection("2.2.2.2", EdgeTypes.USES);
		assertTrue(el.size() == 1);
		System.out.println(el.get(0));
		assertTrue(el.contains("user1"));

		jd.linkNodes("user1", EdgeTypes.USES, "1.1.1.1", node1type, node2type);
		el = jd.getConnection("user1", EdgeTypes.USES);
		assertTrue(el.size() == 2);
		assertTrue(el.contains("1.1.1.1"));
		assertTrue(el.contains("2.2.2.2"));

		jd.linkNodes("user2", EdgeTypes.USES, "3.3.3.3", node1type, node2type);
		jd.linkNodes("user3", EdgeTypes.USES, "3.3.3.3", node1type, node2type);
		jd.linkNodes("user4", EdgeTypes.USES, "3.3.3.3", node1type, node2type);
		jd.linkNodes("1.1.1.1", EdgeTypes.CONNECTS_TO, "3.3.3.3", node1type, node2type);
		jd.linkNodes("3.3.3.3", EdgeTypes.CONNECTS_TO, "4.4.4.4", node1type, node2type);

		el = jd.getConnection("3.3.3.3", EdgeTypes.USES);
		assertTrue(el.size() == 3);
		assertTrue(el.contains("user2"));
		assertTrue(el.contains("user3"));
		assertTrue(el.contains("user4"));

		el = jd.getConnection("3.3.3.3", EdgeTypes.CONNECTS_TO);
		assertTrue(el.size() == 2);
		assertTrue(el.contains("1.1.1.1"));
		assertTrue(el.contains("4.4.4.4"));

	}

}
