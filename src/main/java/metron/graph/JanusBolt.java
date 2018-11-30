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

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

public class JanusBolt extends BaseRichBolt {

	/**
	* 
	*/
	private static final long serialVersionUID = 3984660977031068498L;

	private String JANUS_CONFIG;
	private int TTL_VALUE;

	private JanusDAO jd;

	@SuppressWarnings("rawtypes")
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {

		JANUS_CONFIG = conf.get("top.graphbolt.backEndConfigLocation").toString();
		TTL_VALUE = Integer.parseInt(conf.get("top.graphbolt.ttlDays").toString());

		jd = new JanusDAO(JANUS_CONFIG, TTL_VALUE);
		// jd.createConnectsRelationshipSchema();
	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		String source = tuple.getString(0);
		String relation = tuple.getString(1);
		String dest = tuple.getString(2);
		String node1type = tuple.getString(3);
		String node2type = tuple.getString(4);

		jd.linkNodes(source, relation, dest, node1type, node2type);
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub

	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub

	}

}
