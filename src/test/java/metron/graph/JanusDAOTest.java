package metron.graph;

import java.util.ArrayList;

import org.apache.hadoop.hbase.shaded.org.junit.Before;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import metron.graph.EdgeTypes;
import metron.graph.JanusDAO;

public class JanusDAOTest extends TestCase {

	private JanusDAO jd;

	public JanusDAOTest(String testName) {
		super(testName);
	}

	public static Test suite() {
		return new TestSuite(JanusDAOTest.class);
	}

	@Before
	protected void setUp() throws Exception {
		jd = new JanusDAO("/Users/jsirota/Downloads/janusgraph-0.3.1-hadoop2/conf/janusgraph-cassandra-es.properties",
				5);

	}

	public void testSchemaCreation() {
		// jd.createConnectsRelationshipSchema();
		assertTrue(true);
	}

	public void testSingleNodeCreation() {

		jd.linkNodes("1.1.1.1", EdgeTypes.CONNECTS_TO, "2.2.2.2");
		assertTrue(jd.vertexExists("1.1.1.1"));
		assertTrue(jd.vertexExists("2.2.2.2"));
		assertFalse(jd.vertexExists("1.1.1.2"));
		assertTrue(jd.relationExists("1.1.1.1", EdgeTypes.CONNECTS_TO, "2.2.2.2"));
		assertFalse(jd.relationExists("1.1.1.2", EdgeTypes.CONNECTS_TO, "2.2.2.2"));
		assertFalse(jd.relationExists("1.1.1.1", EdgeTypes.USES, "2.2.2.2"));
	}

	public void testDuplicateInsertionEdge() {

		assertFalse(jd.relationExists("1.1.1.1", EdgeTypes.CONNECTS_TO, "2.2.2.2"));
		jd.linkNodes("1.1.1.1", EdgeTypes.CONNECTS_TO, "2.2.2.2");
		ArrayList<String> el = jd.getEdgesForVertex("1.1.1.1");
		assertTrue(el.size() == 1);
		assertTrue(el.get(0).equals(EdgeTypes.CONNECTS_TO));

		assertTrue(jd.relationExists("1.1.1.1", EdgeTypes.CONNECTS_TO, "2.2.2.2"));

		jd.linkNodes("1.1.1.1", EdgeTypes.CONNECTS_TO, "2.2.2.2");
		el = jd.getEdgesForVertex("1.1.1.1");
		assertTrue(el.size() == 1);

		assertFalse(jd.relationExists("1.1.1.1", EdgeTypes.CONNECTS_TO, "3.3.3.3"));

		jd.linkNodes("1.1.1.1", EdgeTypes.CONNECTS_TO, "3.3.3.3");
		el = jd.getEdgesForVertex("1.1.1.1");
		assertTrue(el.size() == 2);

		el = jd.getEdgesForVertex("3.3.3.3");
		assertTrue(el.size() == 1);

		jd.linkNodes("1.1.1.1", EdgeTypes.CONNECTS_TO, "3.3.3.3");
		el = jd.getEdgesForVertex("1.1.1.1");
		assertTrue(el.size() == 2);

		assertTrue(jd.relationExists("1.1.1.1", EdgeTypes.CONNECTS_TO, "3.3.3.3"));

		jd.linkNodes("1.1.1.1", EdgeTypes.CONNECTS_TO, "4.4.4.4");
		el = jd.getEdgesForVertex("1.1.1.1");
		assertTrue(el.size() == 3);

		jd.linkNodes("1.1.1.1", EdgeTypes.CONNECTS_TO, "5.5.5.5");
		el = jd.getEdgesForVertex("1.1.1.1");
		assertTrue(el.size() == 4);

		jd.linkNodes("1.1.1.1", EdgeTypes.CONNECTS_TO, "5.5.5.5");
		el = jd.getEdgesForVertex("1.1.1.1");
		assertTrue(el.size() == 4);

		el = jd.getEdgesForVertex("5.5.5.5");
		assertTrue(el.size() == 1);

	}

	public void testNodeEdgeCombinations() {

		jd.linkNodes("1.1.1.1", EdgeTypes.CONNECTS_TO, "2.2.2.2");
		jd.linkNodes("1.1.1.1", EdgeTypes.CONNECTS_TO, "3.3.3.3");
		jd.linkNodes("1.1.1.1", EdgeTypes.CONNECTS_TO, "4.4.4.4");

		ArrayList<String> el = jd.getConnection("1.1.1.1", EdgeTypes.CONNECTS_TO);
		assertTrue(el.size() == 3);

		assertTrue(el.get(0).equals("2.2.2.2"));
		assertTrue(el.get(1).equals("3.3.3.3"));
		assertTrue(el.get(2).equals("4.4.4.4"));

		el = jd.getConnection("2.2.2.2", EdgeTypes.CONNECTS_TO);
		assertTrue(el.size() == 1);
		assertTrue(el.get(0).equals("1.1.1.1"));

		jd.linkNodes("2.2.2.2", EdgeTypes.CONNECTS_TO, "3.3.3.3");
		el = jd.getConnection("2.2.2.2", EdgeTypes.CONNECTS_TO);
		assertTrue(el.size() == 2);

		assertTrue(el.contains("1.1.1.1"));
		assertTrue(el.contains("3.3.3.3"));

		el = jd.getConnection("3.3.3.3", EdgeTypes.CONNECTS_TO);
		assertTrue(el.size() == 2);
		assertTrue(el.contains("1.1.1.1"));
		assertTrue(el.contains("2.2.2.2"));

		el = jd.getConnection("4.4.4.4", EdgeTypes.CONNECTS_TO);
		assertTrue(el.size() == 1);
		assertTrue(el.contains("1.1.1.1"));
	}

	public void testRelationCombinations() {

		jd.linkNodes("1.1.1.1", EdgeTypes.CONNECTS_TO, "2.2.2.2");
		jd.linkNodes("user1", EdgeTypes.USES, "2.2.2.2");

		ArrayList<String> el = jd.getConnection("2.2.2.2", EdgeTypes.CONNECTS_TO);
		assertTrue(el.size() == 1);
		assertTrue(el.contains("1.1.1.1"));

		el = jd.getConnection("2.2.2.2", EdgeTypes.USES);
		assertTrue(el.size() == 1);
		System.out.println(el.get(0));
		assertTrue(el.contains("user1"));

		jd.linkNodes("user1", EdgeTypes.USES, "1.1.1.1");
		el = jd.getConnection("user1", EdgeTypes.USES);
		assertTrue(el.size() == 2);
		assertTrue(el.contains("1.1.1.1"));
		assertTrue(el.contains("2.2.2.2"));

		jd.linkNodes("user2", EdgeTypes.USES, "3.3.3.3");
		jd.linkNodes("user3", EdgeTypes.USES, "3.3.3.3");
		jd.linkNodes("user4", EdgeTypes.USES, "3.3.3.3");
		jd.linkNodes("1.1.1.1", EdgeTypes.CONNECTS_TO, "3.3.3.3");
		jd.linkNodes("3.3.3.3", EdgeTypes.CONNECTS_TO, "4.4.4.4");

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
