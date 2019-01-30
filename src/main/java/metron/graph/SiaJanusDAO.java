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

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Map;

public class SiaJanusDAO {

	private JanusGraph g;

	// private int DEFAULT_TTL_DAYS;

	private String CONFIG_FILE;
	private Logger logger = LoggerFactory.getLogger(ConfigHandler.class);

	public SiaJanusDAO(Map conf, String configFIle, int ttlDays) {
		CONFIG_FILE = configFIle;
		g = JanusGraphFactory.open(configFIle);

		// DEFAULT_TTL_DAYS = ttlDays; TODO: implement this later

		// Initialize graph schema and properties
		GraphManager.initializeSchema(g);

	}

	public synchronized void saveJson(JSONObject jsonObject){
		GraphImporter.importJsonObject(g, jsonObject);
	}

}
