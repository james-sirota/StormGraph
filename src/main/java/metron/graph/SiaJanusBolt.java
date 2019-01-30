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

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SiaJanusBolt extends BaseRichBolt {

	/**
	* 
	*/
	private static final long serialVersionUID = 3984660977031068498L;

	private String JANUS_CONFIG;
	private int TTL_VALUE;
	private Logger logger;
	private SiaJanusDAO jd;
	private String FIELD_TO_LOOK_FOR = "jsonObject";
	private OutputCollector collector;

	@SuppressWarnings("rawtypes")
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		logger = LoggerFactory.getLogger(SiaJanusBolt.class);

		logger.trace("Initializing janus bolt...");

		JANUS_CONFIG = ConfigHandler.checkForNullConfigAndLoad("top.graphbolt.backEndConfigLocation", conf);
		TTL_VALUE = Integer.parseInt(ConfigHandler.checkForNullConfigAndLoad("top.graphbolt.ttlDays", conf));

		logger.trace("Initializing Janus DAO...");

		//java.net.URL fileURL = JanusDAO.class.getResource(JANUS_CONFIG);
		//File file = new File(fileURL);

		logger.info("Loading config from: " + JANUS_CONFIG);
		jd = new SiaJanusDAO(conf, JANUS_CONFIG, TTL_VALUE);

		logger.debug("Janus bolt initialized...");
	}

	public void execute(Tuple tuple) {

		try {

			if (!tuple.contains(FIELD_TO_LOOK_FOR))
				throw new IllegalArgumentException(
						"JsonObject is not present, invalid input in field: " + FIELD_TO_LOOK_FOR);

			JSONObject jsonObject = (JSONObject) tuple.getValueByField(FIELD_TO_LOOK_FOR);
			jd.saveJson(jsonObject);

			collector.ack(tuple);
		} catch (Exception e) {
			collector.fail(tuple);
			e.printStackTrace();
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub

	}

}
