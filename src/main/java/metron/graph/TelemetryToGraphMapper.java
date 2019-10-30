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
import java.util.ArrayList;

import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TelemetryToGraphMapper implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -5705431851614623697L;
	private ArrayList<TrippleStoreConf> mapperConfig;
	private static final Logger logger = LoggerFactory.getLogger(TelemetryToGraphMapper.class);

	public TelemetryToGraphMapper(ArrayList<TrippleStoreConf> mapconfig) {
		mapperConfig = mapconfig;

		logger.info("Mapper config initialized with the following properties");

		mapperConfig.forEach((k) -> {
			logger.info("MapperConfigItem : " + k.printElement());
			validateRelation(k);

		});

	}

	public ArrayList<Ontology> getOntologies(JSONObject jsonObject, String tid) {
		ArrayList<Ontology> ontologies = new ArrayList<Ontology>();

		mapperConfig.forEach((k) -> {
			logger.info("Tuple id:" + tid + "::"+"Looking at config item : " + k.printElement() + " for json: " + jsonObject);
			validateRelation(k);
			validateMessage(jsonObject);

			if (jsonObject.containsKey(k.getNode1name()) && jsonObject.containsKey(k.getNode2name())) {
				logger.info("Tuple id:" + tid + "::"+"Matched rule : " + k.printElement() + " for json: " + jsonObject);

				if (jsonObject.get(k.getNode1name()) == null) {
					logger.info("Tuple id:" + tid + "::"+"Unable to set relation: Node1 value is null for relation : " + k.printElement()
							+ " for json: " + jsonObject);
				} else if (jsonObject.get(k.getNode2name()) == null) {
					logger.info("Tuple id:" + tid + "::"+"Unable to set relation: Node2 value is null for relation : " + k.printElement()
							+ " for json: " + jsonObject);
				
				} else {

					String node1 = jsonObject.get(k.getNode1name()).toString();
					String node2 = jsonObject.get(k.getNode2name()).toString();
					String node1type = k.getNode1type();
					String node2type = k.getNode2type();
					String verb = k.getVerbname();

					logger.info("Tuple id:" + tid + "::"+"Extracted relation: " + node1 + " " + verb + " " + node2 + " " + node1type + " "
							+ node2type + " from object: " + jsonObject + " via rule " + k.printElement()
							+ " for item: " + jsonObject);

					Ontology ont = new Ontology(node1, verb, node2, node1type, node2type);
					ontologies.add(ont);
				}
			}

			else {
				if (!jsonObject.containsKey(k.getNode1name()))
					logger.info("Tuple id:" + tid + "::"+"No source vertex " + k.getNode1name() + " in object " + jsonObject);

				if (!jsonObject.containsKey(k.getNode2name()))
					logger.info("Tuple id:" + tid + "::"+"No dest vertex " + k.getNode2name() + " in object " + jsonObject);

			}
		});

		return ontologies;
	}

	private boolean validateRelation(TrippleStoreConf k) throws IllegalArgumentException {

		if (k.getNode1name() == null || k.getNode1name().toString().length() == 0)
			throw new IllegalArgumentException("node1Name is invalid in relation" + k.printElement());

		if (k.getNode2name() == null || k.getNode2name().toString().length() == 0)
			throw new IllegalArgumentException("node2Name is invalid in relation" + k.printElement());

		if (k.getNode1type() == null || k.getNode1type().toString().length() == 0)
			throw new IllegalArgumentException("Node1type is invalid in relation" + k.printElement());

		if (k.getNode2type() == null || k.getNode2name().toString().length() == 0)
			throw new IllegalArgumentException("Node2type is invalid in relation" + k.printElement());

		if (k.getVerbname() == null || k.getVerbname().toString().length() == 0)
			throw new IllegalArgumentException("Verbname is invalid in relation" + k.printElement());

		return true;
	}

	private boolean validateMessage(JSONObject jo) throws IllegalArgumentException {
		if (jo.isEmpty())
			throw new IllegalArgumentException("Received empty JSON " + jo);

		if (jo.values().isEmpty())
			throw new IllegalArgumentException("No values in JSON " + jo);

		return true;
	}

}
