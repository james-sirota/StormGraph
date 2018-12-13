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
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.json.simple.JSONObject;
import org.apache.log4j.Logger;

public class TelemetryLoaderSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(TelemetryLoaderSpout.class);
	private SpoutOutputCollector collector;
	private Random rand;
	private int sleep;
	private int randSize;
	private String sourceName;
	private String destName;
	private String user;

	@SuppressWarnings("unchecked")
	public void nextTuple() {
		Utils.sleep(sleep);
		logger.debug("Spout woke up from sleep");

		JSONObject json = new JSONObject();
		
		

		json.put(sourceName, rand.nextInt(randSize) + "." + rand.nextInt(randSize) + "." + rand.nextInt(randSize) + "."
				+ rand.nextInt(randSize));
		
		json.put(destName, rand.nextInt(randSize) + "." + rand.nextInt(randSize) + "." + rand.nextInt(randSize) + "."
				+ rand.nextInt(randSize));
		
		json.put(user, "user_" + rand.nextInt(randSize)); 
				
		collector.emit(new Values(json.toString()));

	}

	@SuppressWarnings("rawtypes")
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
		this.rand = new Random();

		sleep = Integer.parseInt(ConfigHandler.checkForNullConfigAndLoad("top.spout.generator.sleep", conf));
		randSize = Integer.parseInt(ConfigHandler.checkForNullConfigAndLoad("top.spout.generator.randSize", conf));
		sourceName = ConfigHandler.checkForNullConfigAndLoad("top.spout.generator.sourceFieldName", conf);
		destName = ConfigHandler.checkForNullConfigAndLoad("top.spout.generator.destFieldName", conf);
		user = ConfigHandler.checkForNullConfigAndLoad("top.spout.generator.userField", conf);

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("value"));

	}

}