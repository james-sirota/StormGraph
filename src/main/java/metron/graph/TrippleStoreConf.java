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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrippleStoreConf {

	private String FROM;
	private String FROM_NODE_TYPE;
	private String VERB;
	private String TO;
	private String TO_NODE_TYPE;
	
	private static final Logger logger = LoggerFactory.getLogger(TrippleStoreConf.class);

	public TrippleStoreConf(String from, String to, String verb, String fromNodeType, String toNodeType) {
		
		logger.trace("Initializing a new mapper config item...");
		
		FROM = from;
		FROM_NODE_TYPE = fromNodeType;
		VERB = verb;
		TO = to;
		TO_NODE_TYPE = toNodeType;
		
		logger.debug("SET RULE: " + printElement());
	}

	public String printElement() {
		return ("From: " + FROM + " TO: " + TO + " VERB: " + VERB + " FROM_NODE_TYPE: " + FROM_NODE_TYPE
				+ " TO_NODE_TYPE: " + TO_NODE_TYPE);
	}

	public String getFrom() {
		return FROM;
	}

	public String getFromNodeType() {
		return FROM_NODE_TYPE;
	}

	public String getVerb() {
		return VERB;
	}

	public String getTo() {
		return TO;
	}

	public String getToNodeType() {
		return TO_NODE_TYPE;
	}

}
