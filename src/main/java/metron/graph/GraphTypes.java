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

public class GraphTypes implements Serializable{


	
	/**
	 * 
	 */
	private static final long serialVersionUID = 8767342937103255700L;
	private ArrayList<String> edgeTypes = new ArrayList<String>();
	private ArrayList<String> vertexTypes = new ArrayList<String>();
	
	public GraphTypes()
	{
		edgeTypes.add("connectsTo");
		edgeTypes.add("uses");
		edgeTypes.add("usedBy");
		
		vertexTypes.add("host");
		vertexTypes.add("user");
		vertexTypes.add("ipAddress");
	}
	
	public ArrayList<String> getEdgeTypes()
	{
		return edgeTypes;
	}
	
	public ArrayList<String> getVertexTypes()
	{
		return vertexTypes;
	}
	
	
}