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

import java.util.ArrayList;
import java.util.Map;

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

public class MapperBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3035757397365170506L;
	private OutputCollector collector;
	private JSONParser parser;
	private ArrayList<TrippleStoreConf> mapperConfig;

	@SuppressWarnings("rawtypes")
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		parser = new JSONParser();

		mapperConfig = new ArrayList<TrippleStoreConf>();

		String[] mc = conf.get("top.mapperbolt.mappings").toString().split(";");

		for (int i = 0; i < mc.length; i++) {
			String[] parts = mc[i].split(",");
			TrippleStoreConf tc = new TrippleStoreConf(parts[0], parts[1], parts[2], parts[3], parts[4]);
			mapperConfig.add(tc);
		}

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("source", "edge", "dest", "node1type", "node2type"));

	}

	public void execute(Tuple tuple) {

		try {
			JSONObject jsonObject = (JSONObject) parser.parse(tuple.getStringByField("value"));

			System.out.println("PARSED JSON: " + jsonObject);

			for (int i = 0; i < mapperConfig.size(); i++) {
				TrippleStoreConf configItem = mapperConfig.get(i);
				if (jsonObject.containsKey(configItem.getFrom()) && jsonObject.containsKey(configItem.getTo())) {

					System.out.println("EMITTED MAPPED " + jsonObject.get(configItem.getFrom()) + " "
							+ configItem.getVerb() + " " + jsonObject.get(configItem.getTo()) + " " + configItem.getFromNodeType()
							+ " " + configItem.getToNodeType());
					
					collector.emit(new Values(jsonObject.get(configItem.getFrom()), configItem.getVerb(),
							jsonObject.get(configItem.getTo()), configItem.getFromNodeType(), configItem.getToNodeType()));
				}
			}

		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


	}

}
