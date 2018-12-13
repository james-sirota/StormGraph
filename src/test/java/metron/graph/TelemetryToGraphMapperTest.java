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


import org.json.simple.JSONObject;
import org.junit.Before;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

public class TelemetryToGraphMapperTest extends TestCase {

	private ArrayList<TrippleStoreConf> mappings;
	private TelemetryToGraphMapper mpr;
	private JSONObject jsonObject;

	public TelemetryToGraphMapperTest(String testName) {
		super(testName);
	}

	public static Test suite() {
		return new TestSuite(TelemetryToGraphMapperTest.class);
	}

	@SuppressWarnings("unchecked")
	@Before
	protected void setUp() throws Exception {

		String configString = "ip_src,ip_dst,CONNECTS_TO,host,host;username,ip_src,USES,user,host";
		mappings = ConfigHandler.getAndValidateMappings(configString);
		mpr = new TelemetryToGraphMapper(mappings);

		jsonObject = new JSONObject();
		jsonObject.put("ip_src", "1.1.1.1");
		jsonObject.put("ip_dst", "2.2.2.2");
		jsonObject.put("username", "someuser1");
		jsonObject.put("somevalue1", "blah1");
		jsonObject.put("somevalue2", "blah2");
	}

	public void testMappings() {

		ArrayList<Ontology> ontologies = mpr.getOntologies(jsonObject);

		assertTrue(ontologies.size() == 2);

		assertTrue(ontologies.get(0).getVerb().equals("CONNECTS_TO"));
		assertTrue(ontologies.get(1).getVerb().equals("USES"));

		assertTrue(ontologies.get(0).getVertex1().equals("1.1.1.1"));
		assertTrue(ontologies.get(0).getVertex2().equals("2.2.2.2"));
		assertTrue(ontologies.get(0).getVertex1type().equals("host"));
		assertTrue(ontologies.get(0).getVertex2type().equals("host"));

		assertTrue(ontologies.get(1).getVertex1().equals("someuser1"));
		assertTrue(ontologies.get(1).getVertex2().equals("1.1.1.1"));
		assertTrue(ontologies.get(1).getVertex1type().equals("user"));
		assertTrue(ontologies.get(1).getVertex2type().equals("host"));

	}

	@SuppressWarnings("unchecked")
	public void testNullMappings() {
		JSONObject jo = new JSONObject();
		jo.putAll(jsonObject);

		jo.remove("ip_src");

		ArrayList<Ontology> ontologies = mpr.getOntologies(jo);

		assertTrue(ontologies.isEmpty());
	}

	@SuppressWarnings("unchecked")
	public void testSingleMapping() {
		JSONObject jo = new JSONObject();
		jo.putAll(jsonObject);

		 jo.remove("ip_dst");

		ArrayList<Ontology> ontologies = mpr.getOntologies(jo);

		assertTrue(ontologies.size()==1);
		assertTrue(ontologies.get(0).getVertex1().equals("someuser1"));
		assertTrue(ontologies.get(0).getVertex2().equals("1.1.1.1"));
		assertTrue(ontologies.get(0).getVertex1type().equals("user"));
		assertTrue(ontologies.get(0).getVertex2type().equals("host"));
		
		jo = new JSONObject();
		jo.putAll(jsonObject);
		
		jo.remove("username");
		ontologies = mpr.getOntologies(jo);
		
		assertTrue(ontologies.size()==1);
		assertTrue(ontologies.get(0).getVertex1().equals("1.1.1.1"));
		assertTrue(ontologies.get(0).getVertex2().equals("2.2.2.2"));
		assertTrue(ontologies.get(0).getVertex1type().equals("host"));
		assertTrue(ontologies.get(0).getVertex2type().equals("host"));
		
		jo.remove("ip_dst");
		ontologies = mpr.getOntologies(jo);
		assertTrue(ontologies.size()==0);

		
	}
	
	public void testAddingMappings()
	{
		TrippleStoreConf cnf = new TrippleStoreConf("ip_dst", "ip_src", "CONNECTED_FROM", "host", "host");
		
		mappings.add(cnf);
		assertTrue(mappings.size() == 3);
		
		mpr = new TelemetryToGraphMapper(mappings);
		
		ArrayList<Ontology> ontologies = mpr.getOntologies(jsonObject);	
		assertTrue(ontologies.size() == 3);
		

		assertTrue(ontologies.get(0).getVerb().equals("CONNECTS_TO"));
		assertTrue(ontologies.get(1).getVerb().equals("USES"));
		assertTrue(ontologies.get(2).getVerb().equals("CONNECTED_FROM"));

		assertTrue(ontologies.get(0).getVertex1().equals("1.1.1.1"));
		assertTrue(ontologies.get(0).getVertex2().equals("2.2.2.2"));
		assertTrue(ontologies.get(0).getVertex1type().equals("host"));
		assertTrue(ontologies.get(0).getVertex2type().equals("host"));

		assertTrue(ontologies.get(1).getVertex1().equals("someuser1"));
		assertTrue(ontologies.get(1).getVertex2().equals("1.1.1.1"));
		assertTrue(ontologies.get(1).getVertex1type().equals("user"));
		assertTrue(ontologies.get(1).getVertex2type().equals("host"));
		
		assertTrue(ontologies.get(2).getVertex1().equals("2.2.2.2"));
		assertTrue(ontologies.get(2).getVertex2().equals("1.1.1.1"));
		assertTrue(ontologies.get(2).getVertex1type().equals("host"));
		assertTrue(ontologies.get(2).getVertex2type().equals("host"));
		
	}
	
	public void testForNullConfigs()
	{
		mappings.clear();
		assertTrue(mappings.size() == 0);
		
		mpr = new TelemetryToGraphMapper(mappings);
		
		ArrayList<Ontology> ontologies = mpr.getOntologies(jsonObject);	
		assertTrue(ontologies.size() == 0);
		
	}
	
	public void testForInvalidConfig()
	{
		TrippleStoreConf cnf = new TrippleStoreConf("a", "b", "CONNECTED_FROM", "host", "host");
		TrippleStoreConf cnf2 = new TrippleStoreConf("c", "d", "CONNECTED_TO", "host", "host");
		
		mappings.clear();
		mappings.add(cnf);
		mappings.add(cnf2);
		
		assertTrue(mappings.size() == 2);
		
		mpr = new TelemetryToGraphMapper(mappings);
		
		ArrayList<Ontology> ontologies = mpr.getOntologies(jsonObject);	
		assertTrue(ontologies.size() == 0);
		
	}

}
