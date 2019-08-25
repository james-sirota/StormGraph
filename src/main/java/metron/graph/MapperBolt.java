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
import java.util.Iterator;
import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.MessageId;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapperBolt extends BaseRichBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3035757397365170506L;
	private OutputCollector collector;
	private JSONParser parser;
	// private ArrayList<TrippleStoreConf> mapperConfig;
	private String tupleToLookFor;
	private Logger logger;
	private TelemetryToGraphMapper mapper;

	private final String ARRAY_PREFIX = "array";

	@SuppressWarnings("rawtypes")
	public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		logger = LoggerFactory.getLogger(MapperBolt.class);

		logger.trace("Initializing parser...");
		parser = new JSONParser();

		logger.trace("Initializing mapping config...");
		String mappingString = ConfigHandler.checkForNullConfigAndLoad("top.mapperbolt.mappings", conf);
		// mapperConfig = ConfigHandler.getAndValidateMappings(mappingString);
		mapper = new TelemetryToGraphMapper(ConfigHandler.getAndValidateMappings(mappingString));

		tupleToLookFor = ConfigHandler.checkForNullConfigAndLoad("top.mapperbolt.tupleToLookFor", conf);

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("ont"));

	}

	public void execute(Tuple tuple) {

		try {
			
			MessageId tid = tuple.getMessageId();

			logger.info("Tuple id:" + tid + "::"+ "Received a new tuple from spout: " + tuple);

			String extractedContent = tuple.getStringByField(tupleToLookFor).trim();
			logger.info("Tuple id:" + tid + "::"+"Extracted tupple content from original spout tuple: " + extractedContent);

			if (extractedContent.length() == 0 || extractedContent == null || extractedContent == "") 
				throw new NoSuchFieldException("Tuple id:" + tid + "::"+"Invalid content JSON inside tuple: " + tuple);
				
				Object obj = parser.parse("{ \""+ ARRAY_PREFIX +"\":" + extractedContent + "}");

				logger.info("Tuple id:" + tid + "::"+"Attempting to parse: " + obj + " from content: " + extractedContent);

				JSONObject jsonObject = (JSONObject) obj;

				JSONArray jList = (JSONArray) jsonObject.get(ARRAY_PREFIX);

				logger.info("Tuple id:" + tid + "::"+"Parsed array tupple: " + jList + " from " + extractedContent);
				
				if (jList.isEmpty())
					throw new IllegalArgumentException("Tuple id:" + tid + "::"+"List of valid JSON messages does not exist inside: " + tuple);

				Iterator<?> iter = jList.iterator();
				

				while (iter.hasNext()) {

					JSONObject jo = (JSONObject) iter.next();
					logger.info("Tuple id:" + tid + "::"+"Prased object is: " + jo + "from JSON list " + jList);

					if (jo.isEmpty())
						throw new NoSuchFieldException(jo + " is a zero-sized message");

					ArrayList<Ontology> ontologyList = mapper.getOntologies(jo, tid.toString());

					if (ontologyList.isEmpty())
						logger.info("Tuple id:" + tid + "::"+"No ontologies found for object: " + jo);
					
					ontologyList.forEach((ont) -> {
						logger.info("Tuple id:" + tid + "::"+"Emmiting ontology: " + ont.printElement());
						collector.emit(new Values(ont));
					});

				/*	for (int i = 0; i < ontologyList.size(); i++) {
						Ontology ont = ontologyList.get(i);

						logger.info("Emmiting ontology: " + ont.printElement());

						collector.emit(new Values(ont));

					}*/
				}

				collector.ack(tuple);
			
		}

		catch (Exception e) {
			collector.fail(tuple);
			logger.error("Tuple id:" + tuple.getMessageId() + "::"+"Failed to parse object tuple:" + tuple);
			e.printStackTrace();
		}

	}

}