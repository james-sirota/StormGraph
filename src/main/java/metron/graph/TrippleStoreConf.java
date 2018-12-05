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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TrippleStoreConf implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = -8419684640170569296L;
	private String node1name;
	private String node1type;
	private String verbname;
	private String node2name;
	private String node2type;

	private static final Logger logger = LoggerFactory.getLogger(TrippleStoreConf.class);

	public TrippleStoreConf(String node1name, String node2name, String verbname, String node1type, String node2type) {

		logger.trace("Initializing a new mapper config item...");

		this.node1name = checkItem(node1name);
		this.node1type = checkItem(node1type);
		this.verbname = checkItem(verbname);
		this.node2name = checkItem(node2name);
		this.node2type = checkItem(node2type);

		logger.debug("Set relation rule: " + printElement());
	}

	public String printElement() {
		return ("node1name: " + node1name + " node2name: " + node2name + " verbname: " + verbname + " node1type: " + node1type
				+ " node2type: " + node2type);
	}

	public String getNode1name() {
		return node1name;
	}

	public void setNode1name(String node1name) {
		this.node1name = node1name;
	}

	public String getNode1type() {
		return node1type;
	}

	public void setNode1type(String node1type) {
		this.node1type = node1type;
	}

	public String getVerbname() {
		return verbname;
	}

	public void setVerbname(String verbname) {
		this.verbname = verbname;
	}

	public String getNode2name() {
		return node2name;
	}

	public void setNode2name(String node2name) {
		this.node2name = node2name;
	}

	public String getNode2type() {
		return node2type;
	}

	public void setNode2type(String node2type) {
		this.node2type = node2type;
	}
	
	private String checkItem(String item)
	{
		if(item.length()==0 && item.isEmpty())
			throw new IllegalArgumentException("Unable to set configuration item " + item);
		
		logger.trace("checked item " + item);
			
		return item;
	}
}
