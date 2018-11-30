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

public class TrippleStoreConf {
	
	private String FROM;
	private String FROM_NODE_TYPE;
	private String VERB;
	private String TO;
	private String TO_NODE_TYPE;

	public TrippleStoreConf(String from, String to, String verb, String fromNodeType, String toNodeType)
	{
		FROM=from;
		FROM_NODE_TYPE = fromNodeType;
		VERB=verb;
		TO=to;
		TO_NODE_TYPE = toNodeType;
	}
	
	public String getFrom()
	{
		return FROM;
	}
	public String getFromNodeType()
	{
		return FROM_NODE_TYPE;
	}
	
	public String getVerb()
	{
		return VERB;
	}
	
	public String getTo()
	{
		return TO;
	}
	
	public String getToNodeType()
	{
		return TO_NODE_TYPE;
	}
	
}
