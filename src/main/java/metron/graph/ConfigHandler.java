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
import java.util.Map;

import org.apache.storm.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigHandler {

	private static final Logger logger = LoggerFactory.getLogger(ConfigHandler.class);

	public static String checkForNullConfigAndLoad(String configName, Config conf) throws IllegalArgumentException {
		if (!conf.containsKey(configName))
			throw new IllegalArgumentException(configName + " param cannot be null.");

		String value = conf.get(configName).toString();

		logger.debug("Value of " + configName + " is " + value);

		return value;
	}

	@SuppressWarnings("rawtypes")
	public static String checkForNullConfigAndLoad(String configName, Map conf) throws IllegalArgumentException {
		if (!conf.containsKey(configName))
			throw new IllegalArgumentException(configName + " param cannot be null.");

		String value = conf.get(configName).toString();

		logger.debug("Value of " + configName + " is " + value);

		return value;
	}

	public static ArrayList<TrippleStoreConf> getAndValidateMappings(String configString) {
		ArrayList<TrippleStoreConf> mapperConfig = new ArrayList<TrippleStoreConf>();

		String[] mc = configString.split(";");

		if (mc.length == 0)
			throw new IllegalArgumentException("No mappings defined for mapper bolt");

		for (int i = 0; i < mc.length; i++) {
			String[] parts = mc[i].split(",");

			if (parts.length != 5)
				throw new IllegalArgumentException("Incorrect mappings defined in string: " + mc[i]);

			for (int j = 0; j < parts.length; j++)
				if (parts[j].length() == 0)
					throw new IllegalArgumentException(
							"Incorrect value argument number " + j + " and value " + parts[j]);

			logger.debug("Setting mapping to " + parts[0] + " : " + parts[1] + " : " + parts[2] + " : " + parts[3]
					+ " : " + parts[4]);

			TrippleStoreConf tc = new TrippleStoreConf(parts[0], parts[1], parts[2], parts[3], parts[4]);
			mapperConfig.add(tc);

		}

		return mapperConfig;
	}

}
