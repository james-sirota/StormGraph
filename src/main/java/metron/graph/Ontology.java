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

public class Ontology implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 4299756439753678888L;
	private String vertex1;
	private String vertex2;
	private String vertex1type;
	private String vertex2type;
	private String verb;
	private static final Logger logger = LoggerFactory.getLogger(Ontology.class);

	public Ontology(String vertex1, String verb, String vertex2, String vertex1type, String vertex2type) {
		
		logger.trace("Initializing a new ontology item...");
		
		this.vertex1 = checkItem(vertex1);
		this.vertex2 = checkItem(vertex2);
		this.vertex1type = checkItem(vertex1type);
		this.vertex2type = checkItem(vertex2type);
		this.verb = checkItem(verb);
		
		logger.debug("Created a new ontology: " + printElement());
	}

	public String printElement() {
		return ("Vertex1: " + vertex1 + " Vertex2: " + vertex2 + " Verb: " + verb + " Vertex1type: " + vertex1type
				+ " Vertex2type: " + vertex2type);
	}

	public String getVertex1() {
		return vertex1;
	}

	public void setVertex1(String vertex1) {
		this.vertex1 = vertex1;
	}

	public String getVertex2() {
		return vertex2;
	}

	public void setVertex2(String vertex2) {
		this.vertex2 = vertex2;
	}

	public String getVertex1type() {
		return vertex1type;
	}

	public void setVertex1type(String vertex1type) {
		this.vertex1type = vertex1type;
	}

	public String getVertex2type() {
		return vertex2type;
	}

	public void setVertex2type(String vertex2type) {
		this.vertex2type = vertex2type;
	}

	public String getVerb() {
		return verb;
	}

	public void setVerb(String verb) {
		this.verb = verb;
	}
	
	private String checkItem(String item)
	{
		if(item.length()==0 && item.isEmpty())
			throw new IllegalArgumentException("Unable to set ontology item " + item);
		
		logger.trace("checked item " + item);
			
		return item;
	}

}
