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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.minlog.Log;

public class JanusBolt extends BaseRichBolt {

	/**
	* 
	*/
	private static final long serialVersionUID = 3984660977031068498L;

	private String JANUS_CONFIG;
	private int TTL_VALUE;
	private Logger logger;
	private JanusDAO jd;

	@SuppressWarnings("rawtypes")
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		logger = LoggerFactory.getLogger(MapperBolt.class);

		logger.trace("Initializing janus bolt...");

		JANUS_CONFIG = ConfigHandler.checkForNullConfigAndLoad("top.graphbolt.backEndConfigLocation", conf);
		TTL_VALUE = Integer.parseInt(ConfigHandler.checkForNullConfigAndLoad("top.graphbolt.ttlDays", conf));

		logger.trace("Initializing Janus DAO...");
		jd = new JanusDAO(JANUS_CONFIG, TTL_VALUE);
		// jd.createConnectsRelationshipSchema();
	}

	public void execute(Tuple tuple, BasicOutputCollector collector) {

		if (!tuple.contains("source"))
			throw new IllegalArgumentException("Source tuple is not present");

		String source = tuple.getStringByField("source");

		if (!tuple.contains("edge"))
			throw new IllegalArgumentException("Edge tuple is not present");

		String relation = tuple.getStringByField("edge");

		if (!tuple.contains("dest"))
			throw new IllegalArgumentException("Dest tuple is not present");

		String dest = tuple.getStringByField("dest");

		if (!tuple.contains("node1type"))
			throw new IllegalArgumentException("node1type tuple is not present");

		String node1type = tuple.getStringByField("node1type");

		if (!tuple.contains("node2type"))
			throw new IllegalArgumentException("node2type tuple is not present");

		String node2type = tuple.getStringByField("node2type");

		logger.debug("Processing: " + tuple.toString());

		jd.linkNodes(source, relation, dest, node1type, node2type);
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub

	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub

	}

}
