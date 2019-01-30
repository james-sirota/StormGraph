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
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class SiaMapperBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3035757397365170506L;
	private OutputCollector collector;
	private String tupleToLookFor;
	private JSONParser parser;
	private Logger logger;

	@SuppressWarnings("rawtypes")
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		logger = LoggerFactory.getLogger(SiaMapperBolt.class);

		logger.trace("Initializing parser...");
		parser = new JSONParser();
		tupleToLookFor = ConfigHandler.checkForNullConfigAndLoad("top.mapperbolt.tupleToLookFor", conf);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("jsonObject"));

	}

	public void execute(Tuple tuple) {

		try {

			if (!tuple.contains(tupleToLookFor))
				throw new IllegalArgumentException(tupleToLookFor + " tuple is not present");

			JSONObject jsonObject = (JSONObject) parser.parse(tuple.getStringByField(tupleToLookFor));

			logger.debug("Parsed json ojbect: " + jsonObject);

			if (jsonObject.keySet().size() == 0)
				throw new IllegalArgumentException(jsonObject + " is not a valid message");

			collector.emit(new Values(jsonObject));

			collector.ack(tuple);
		}
		catch (ParseException e) {
			collector.fail(tuple);
			logger.error("Failed to pasre object" + tuple.getStringByField(tupleToLookFor));
			e.printStackTrace();
		}
		catch(IllegalArgumentException ex){
			collector.fail(tuple);
			logger.error("Failed to pasre object" + ex.getMessage());
			ex.printStackTrace();
		}

	}

}
