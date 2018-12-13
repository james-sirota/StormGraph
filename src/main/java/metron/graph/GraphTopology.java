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
import java.util.Arrays;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.kafka.spout.KafkaSpoutRetryExponentialBackoff;
import org.apache.storm.kafka.spout.KafkaSpoutRetryService;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphTopology {

	private static final Logger logger = LoggerFactory.getLogger(GraphTopology.class);

	public static void main(String[] args) throws Exception {

		if (args[0] == null)
			System.out.println("Please specify the location of the file graphtopology_config.conf");


		Config conf = readConfigFromFile(args[0], logger);

		TopologyBuilder builder = new TopologyBuilder();

		String spoutName = ConfigHandler.checkForNullConfigAndLoad("top.spout.name", conf);
		int spoutParallelism = Integer.parseInt(ConfigHandler.checkForNullConfigAndLoad("top.spout.parallelism", conf));
		boolean generateData = Boolean
				.parseBoolean(ConfigHandler.checkForNullConfigAndLoad("top.generatorSpoutEnabled", conf));

		if (generateData) {

			logger.trace("Started initializing generator spout");
			logger.debug(
					"Setting up generator spout with name " + spoutName + " and parallelism of " + spoutParallelism);
			builder.setSpout(spoutName, new TelemetryLoaderSpout(), spoutParallelism);
			logger.trace("Finished initializing the generator spout");

		} else {

			String bootStrapServers = ConfigHandler.checkForNullConfigAndLoad("top.spout.kafka.bootStrapServers", conf);
			String topic = ConfigHandler.checkForNullConfigAndLoad("top.spout.kafka.topic", conf);
			String consumerGroupId = ConfigHandler.checkForNullConfigAndLoad("top.spout.kafka.consumerGroupId", conf);
			Long offsetCommitPeriodMs = Long
					.parseLong(ConfigHandler.checkForNullConfigAndLoad("top.spout.kafka.offsetCommitPeriodMs", conf));
			int initialDelay = Integer
					.parseInt(ConfigHandler.checkForNullConfigAndLoad("top.spout.kafka.retry.initialDelay", conf));
			int delayPeriod = Integer
					.parseInt(ConfigHandler.checkForNullConfigAndLoad("top.spout.kafka.retry.delayPeriod", conf));
			int maxDelay = Integer
					.parseInt(ConfigHandler.checkForNullConfigAndLoad("top.spout.kafka.retry.maxDelay", conf));
			int maxUncommittedOffsets = Integer
					.parseInt(ConfigHandler.checkForNullConfigAndLoad("top.spout.kafka.maxUncommittedOffsets", conf));

			logger.trace("Started initializing kafkaSpoutRetryService");
			logger.debug("Initializing kafkaSpoutRetryService " + " with initial delay " + initialDelay
					+ " delay period " + delayPeriod + " max delay " + maxDelay);

			KafkaSpoutRetryService kafkaSpoutRetryService = new KafkaSpoutRetryExponentialBackoff(
					KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(initialDelay),
					KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(delayPeriod), Integer.MAX_VALUE,
					KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(maxDelay));

			logger.trace("Finished initializing kafkaSpoutRetryService");

			String tupleFieldTopic = ConfigHandler.checkForNullConfigAndLoad("top.spout.kafka.tupleFieldTopic", conf);
			String tupleFieldPartition = ConfigHandler.checkForNullConfigAndLoad("top.spout.kafka.tupleFieldPartition",
					conf);
			String tupleFieldOffset = ConfigHandler.checkForNullConfigAndLoad("top.spout.kafka.tupleFieldOffset", conf);
			String tupleFieldKey = ConfigHandler.checkForNullConfigAndLoad("top.spout.kafka.tupleFieldKey", conf);
			String tupleFieldValue = ConfigHandler.checkForNullConfigAndLoad("top.spout.kafka.tupleFieldValue", conf);

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

		String mapperBoltName = ConfigHandler.checkForNullConfigAndLoad("top.mapperbolt.name", conf);
		int mapperboltParallelism = Integer
				.parseInt(ConfigHandler.checkForNullConfigAndLoad("top.mapperbolt.parallelism", conf));

		logger.debug("Initializing " + mapperBoltName + " with parallelism " + mapperboltParallelism);
		builder.setBolt(mapperBoltName, new MapperBolt(), mapperboltParallelism).shuffleGrouping(spoutName);

		String graphBoltName = ConfigHandler.checkForNullConfigAndLoad("top.graphbolt.name", conf);
		int graphBoltParallelism = Integer
				.parseInt(ConfigHandler.checkForNullConfigAndLoad("top.graphbolt.parallelism", conf));

		logger.debug("Initializing " + graphBoltName + " with parallelism " + graphBoltParallelism);
		builder.setBolt(graphBoltName, new JanusBolt(), graphBoltParallelism).shuffleGrouping(mapperBoltName);

		boolean debugMode = Boolean.getBoolean(ConfigHandler.checkForNullConfigAndLoad("top.debug", conf));
		conf.setDebug(debugMode);

		int numWorkers = Integer.parseInt(ConfigHandler.checkForNullConfigAndLoad("top.numWorkers", conf));
		conf.setNumWorkers(numWorkers);

		boolean localDeploy = Boolean.parseBoolean(ConfigHandler.checkForNullConfigAndLoad("top.localDeploy", conf));

		String topologyName = ConfigHandler.checkForNullConfigAndLoad("top.name", conf);

		if (localDeploy) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(topologyName, conf, builder.createTopology());
		} 
		else
		{
			System.out.println("[METRON]Submitting topology to remote cluster...");
			StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
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

}