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

public class EdgeTypes implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 5857601064882508913L;
	// if you want to add more, make sure you edit the index
	public static String CONNECTS_TO = "CONNECTS_TO";
	public static String USES = "USES";
}