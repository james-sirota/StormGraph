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

public class TelemetryToGraphMapper implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = -5705431851614623697L;
	private ArrayList<TrippleStoreConf> mapperConfig;
	private static final Logger logger = LoggerFactory.getLogger(TelemetryToGraphMapper.class);

	public TelemetryToGraphMapper(ArrayList<TrippleStoreConf> mapconfig) {
		mapperConfig = mapconfig;
	}

	public ArrayList<Ontology> getOntologies(JSONObject jsonObject) {
		ArrayList<Ontology> ontologies = new ArrayList<Ontology>();

		for (int i = 0; i < mapperConfig.size(); i++) {
			TrippleStoreConf configItem = mapperConfig.get(i);
			if (jsonObject.containsKey(configItem.getNode1name())
					&& jsonObject.containsKey(configItem.getNode2name())) {

				logger.debug("MATCHED RULE: " + configItem.printElement());

				String node1 = jsonObject.get(configItem.getNode1name()).toString();
				String node2 = jsonObject.get(configItem.getNode2name()).toString();
				String node1type = configItem.getNode1type();
				String node2type = configItem.getNode2type();
				String verb = configItem.getVerbname();

				logger.debug("Extracted relation: " + node1 + " " + verb + " " + node2 + " " + node1type + " "
						+ node2type + " from object: " + jsonObject + " via rule " + configItem.printElement());

				Ontology ont = new Ontology(node1, verb, node2, node1type, node2type);
				ontologies.add(ont);

			} else {
				if (!jsonObject.containsKey(configItem.getNode1name()))
					logger.debug("No source vertex " + configItem.getNode1name() + " in object " + jsonObject);

				if (!jsonObject.containsKey(configItem.getNode2name()))
					logger.debug("No dest vertex " + configItem.getNode2name() + " in object " + jsonObject);
			}
		}

		return ontologies;
	}

}
