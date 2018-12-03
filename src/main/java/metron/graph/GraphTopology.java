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

import com.esotericsoftware.minlog.Log;

public class GraphTopology {

	private static final Logger logger = LoggerFactory.getLogger(GraphTopology.class);

	public static void main(String[] args) throws Exception {

		Config conf = readConfigFromFile(args[0], logger);

		TopologyBuilder builder = new TopologyBuilder();

		String spoutName = conf.get("top.spout.name").toString();
		logger.debug("Set spoutName " + " to " + spoutName);

		int spoutParallelism = Integer.parseInt(conf.get("top.spout.parallelism").toString());
		logger.debug("Set spoutParallelism" + " to " + spoutParallelism);

		boolean generateData = Boolean.parseBoolean(conf.get("top.generatorSpoutEnabled").toString());
		logger.debug("Set  generateData" + " to " + generateData);

		if (generateData) {

			logger.trace("Started initializing generator spout");
			logger.debug(
					"Setting up generator spout with name " + spoutName + " and parallelism of " + spoutParallelism);
			builder.setSpout(spoutName, new TelemetryLoaderSpout(), spoutParallelism);
			logger.trace("Finished initializing the generator spout");

		} else {

			logger.trace("Started initializing kafka spout");

			String bootStrapServers = conf.get("top.spout.kafka.bootStrapServers").toString();
			logger.debug("Set bootStrapServers" + " to " + bootStrapServers);

			String topic = conf.get("top.spout.kafka.topic").toString();
			logger.debug("Set topic" + " to " + topic);

			String consumerGroupId = conf.get("top.spout.kafka.consumerGroupId").toString();
			logger.debug("Set consumerGroupId" + " to " + consumerGroupId);

			Long offsetCommitPeriodMs = Long.parseLong(conf.get("top.spout.kafka.consumerGroupId").toString());
			logger.debug("Set offsetCommitPeriodMs" + " to " + offsetCommitPeriodMs);

			int initialDelay = Integer.parseInt(conf.get("top.spout.kafka.retry.initialDelay").toString());
			logger.debug("Set initialDelay" + " to " + initialDelay);

			int delayPeriod = Integer.parseInt(conf.get("top.spout.kafka.retry.delayPeriod").toString());
			logger.debug("Set delayPeriod" + " to " + delayPeriod);

			int maxDelay = Integer.parseInt(conf.get("top.spout.kafka.retry.maxDelay").toString());
			logger.debug("Set maxDelay" + " to " + maxDelay);

			int maxUncommittedOffsets = Integer.parseInt(conf.get("top.spout.kafka.maxUncommittedOffsets").toString());
			logger.debug("Set maxUncommittedOffsets" + " to " + maxUncommittedOffsets);

			logger.trace("Started initializing kafkaSpoutRetryService");
			logger.debug("Initializing kafkaSpoutRetryService " + " with initial delay " + initialDelay
					+ " delay period " + delayPeriod + " max delay " + maxDelay);

			KafkaSpoutRetryService kafkaSpoutRetryService = new KafkaSpoutRetryExponentialBackoff(
					KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(initialDelay),
					KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(delayPeriod), Integer.MAX_VALUE,
					KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(maxDelay));

			logger.trace("Finished initializing kafkaSpoutRetryService");

			String tupleFieldTopic = conf.get("top.spout.kafka.tupleFieldTopic").toString();
			logger.debug("Set tupleFieldTopic" + " to " + tupleFieldTopic);

			String tupleFieldPartition = conf.get("top.spout.kafka.tupleFieldPartition").toString();
			logger.debug("Set tupleFieldPartition" + " to " + tupleFieldPartition);

			String tupleFieldOffset = conf.get("top.spout.kafka.tupleFieldOffset").toString();
			logger.debug("Set tupleFieldOffset" + " to " + tupleFieldOffset);

			String tupleFieldKey = conf.get("top.spout.kafka.tupleFieldKey").toString();
			logger.debug("Set tupleFieldKey" + " to " + tupleFieldKey);

			String tupleFieldValue = conf.get("top.spout.kafka.tupleFieldValue").toString();
			logger.debug("Set tupleFieldValue" + " to " + tupleFieldValue);

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

		String mapperBoltName = conf.get("top.mapperbolt.name").toString();
		logger.debug("Set mapperBoltName" + " to " + mapperBoltName);

		int spoutBoltParallelism = Integer.parseInt(conf.get("top.mapperbolt.parallelism").toString());
		logger.debug("Set spoutBoltParallelism" + " to " + spoutBoltParallelism);
		
		logger.debug("Initializing " + mapperBoltName + " with parallelism " + spoutBoltParallelism);
		builder.setBolt(mapperBoltName, new MapperBolt(), spoutBoltParallelism).shuffleGrouping(spoutName);
		
		String graphBoltName = conf.get("top.graphbolt.name").toString();
		logger.debug("Set graphBoltName" + " to " + graphBoltName);
		
		int graphBoltParallelism = Integer.parseInt(conf.get("top.graphbolt.parallelism").toString());
		logger.debug("Set graphBoltParallelism" + " to " + graphBoltParallelism);
		
		
		logger.debug("Initializing " + graphBoltName + " with parallelism " + graphBoltParallelism);
		builder.setBolt(graphBoltName, new JanusBolt(),graphBoltParallelism)
				.shuffleGrouping(conf.get("top.mapperbolt.name").toString());

		boolean debugMode = Boolean.getBoolean("top.debug");
		logger.debug("Set debugMode" + " to " + debugMode);
		
		logger.trace("Updating conf.setDebug with value " + debugMode);
		conf.setDebug(debugMode);
		
		int numWorkers = Integer.parseInt(conf.get("top.numWorkers").toString());
		logger.debug("Set numWorkers" + " to " + numWorkers);

		logger.trace("Updating conf.setNumWorkers with value " + numWorkers);
		conf.setNumWorkers(numWorkers);

		boolean localDeploy = Boolean.parseBoolean(conf.get("top.localDeploy").toString());
		logger.debug("Set localDeploy" + " to " + localDeploy);
		
		String topologyName = conf.get("top.name").toString();
		logger.debug("Set topologyName" + " to " + topologyName);

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
}