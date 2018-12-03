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

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphTopology {

	private static final Logger logger = LoggerFactory.getLogger(GraphTopology.class);

	public static void main(String[] args) throws Exception {

		if (args[0] == null)
			System.out.println("Please specify the location of the file graphtopology_config.conf");

		Config conf = readConfigFromFile(args[0], logger);

		TopologyBuilder builder = new TopologyBuilder();

		String spoutName = checkForNullConfigAndLoad("top.spout.name", conf);
		int spoutParallelism = Integer.parseInt(checkForNullConfigAndLoad("top.spout.parallelism", conf));
		boolean generateData = Boolean.parseBoolean(checkForNullConfigAndLoad("top.generatorSpoutEnabled", conf));

		if (generateData) {

			logger.trace("Started initializing generator spout");
			logger.debug(
					"Setting up generator spout with name " + spoutName + " and parallelism of " + spoutParallelism);
			builder.setSpout(spoutName, new TelemetryLoaderSpout(), spoutParallelism);
			logger.trace("Finished initializing the generator spout");

		} else {

			String bootStrapServers = checkForNullConfigAndLoad("top.spout.kafka.bootStrapServers", conf);
			String topic = checkForNullConfigAndLoad("top.spout.kafka.topic", conf);
			String consumerGroupId = checkForNullConfigAndLoad("top.spout.kafka.consumerGroupId", conf);
			Long offsetCommitPeriodMs = Long
					.parseLong(checkForNullConfigAndLoad("top.spout.kafka.consumerGroupId", conf));
			int initialDelay = Integer.parseInt(checkForNullConfigAndLoad("top.spout.kafka.retry.initialDelay", conf));
			int delayPeriod = Integer.parseInt(checkForNullConfigAndLoad("top.spout.kafka.retry.delayPeriod", conf));
			int maxDelay = Integer.parseInt(checkForNullConfigAndLoad("top.spout.kafka.retry.maxDelay", conf));
			int maxUncommittedOffsets = Integer
					.parseInt(checkForNullConfigAndLoad("top.spout.kafka.maxUncommittedOffsets", conf));

			logger.trace("Started initializing kafkaSpoutRetryService");
			logger.debug("Initializing kafkaSpoutRetryService " + " with initial delay " + initialDelay
					+ " delay period " + delayPeriod + " max delay " + maxDelay);

			KafkaSpoutRetryService kafkaSpoutRetryService = new KafkaSpoutRetryExponentialBackoff(
					KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(initialDelay),
					KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(delayPeriod), Integer.MAX_VALUE,
					KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(maxDelay));

			logger.trace("Finished initializing kafkaSpoutRetryService");

			String tupleFieldTopic = checkForNullConfigAndLoad("top.spout.kafka.tupleFieldTopic", conf);
			String tupleFieldPartition = checkForNullConfigAndLoad("top.spout.kafka.tupleFieldPartition", conf);
			String tupleFieldOffset = checkForNullConfigAndLoad("top.spout.kafka.tupleFieldOffset", conf);
			String tupleFieldKey = checkForNullConfigAndLoad("top.spout.kafka.tupleFieldKey", conf);
			String tupleFieldValue = checkForNullConfigAndLoad("top.spout.kafka.tupleFieldValue", conf);

			logger.trace("Started initializing spoutConf");
			KafkaSpoutConfig<String, String> spoutConf = KafkaSpoutConfig.builder(bootStrapServers, topic)
					.setProp(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId)
					.setOffsetCommitPeriodMs(offsetCommitPeriodMs)
					.setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_LATEST)
					.setMaxUncommittedOffsets(maxUncommittedOffsets).setRetry(kafkaSpoutRetryService)
					.setRecordTranslator((r) -> new Values(r.topic(), r.partition(), r.offset(), r.key(), r.value()),
							new Fields(tupleFieldTopic, tupleFieldPartition, tupleFieldOffset, tupleFieldKey,
									tupleFieldValue))
					.build();
			logger.trace("Finished initializing spoutConf");

			builder.setSpout(spoutName, new KafkaSpout<String, String>(spoutConf), spoutParallelism);

		}

		String mapperBoltName = checkForNullConfigAndLoad("top.mapperbolt.name", conf);
		int mapperboltParallelism = Integer.parseInt(checkForNullConfigAndLoad("top.mapperbolt.parallelism", conf));

		logger.debug("Initializing " + mapperBoltName + " with parallelism " + mapperboltParallelism);
		builder.setBolt(mapperBoltName, new MapperBolt(), mapperboltParallelism).shuffleGrouping(spoutName);

		String graphBoltName = checkForNullConfigAndLoad("top.graphbolt.name", conf);
		int graphBoltParallelism = Integer.parseInt(checkForNullConfigAndLoad("top.graphbolt.parallelism", conf));

		logger.debug("Initializing " + graphBoltName + " with parallelism " + graphBoltParallelism);
		builder.setBolt(graphBoltName, new JanusBolt(), graphBoltParallelism).shuffleGrouping(mapperBoltName);

		boolean debugMode = Boolean.getBoolean(checkForNullConfigAndLoad("top.debug", conf));
		conf.setDebug(debugMode);

		int numWorkers = Integer.parseInt(checkForNullConfigAndLoad("top.numWorkers", conf));
		conf.setNumWorkers(numWorkers);

		boolean localDeploy = Boolean.parseBoolean(checkForNullConfigAndLoad("top.localDeploy", conf));

		String topologyName = checkForNullConfigAndLoad("top.name", conf);

		if (localDeploy) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(topologyName, conf, builder.createTopology());
		} else {
			StormSubmitter.submitTopologyWithProgressBar(topologyName, conf, builder.createTopology());
		}

	}

	public static Config readConfigFromFile(String filename, Logger log) throws IOException {
		Config conf = new Config();
		System.out.println("[METRON] Reading config file: " + filename);

		FileInputStream fstream = new FileInputStream(filename);
		BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

		String strLine;

		while ((strLine = br.readLine()) != null) {
			System.out.println(strLine);

			if (strLine.length() != 0 && !(strLine.charAt(0) == '#')) {
				String[] parts = strLine.split("=");
				conf.put(parts[0], parts[1]);
				log.debug("Setting property " + parts[0] + " to " + parts[1]);
			}
		}

		br.close();

		return conf;
	}

	public static String checkForNullConfigAndLoad(String configName, Config conf) throws IllegalArgumentException {
		if (!conf.containsKey(configName))
			throw new IllegalArgumentException(configName + " param cannot be null.");

		String value = conf.get(configName).toString();

		logger.debug("Value of " + configName + " is " + value);

		return value;
	}

}