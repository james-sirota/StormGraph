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

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphTopology {

	private static final Logger logger = LoggerFactory.getLogger(GraphTopology.class);

	public static void main(String[] args) throws Exception {

		Config conf = readConfigFromFile(args[0]);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout(conf.get("top.spout.name").toString(), new TelemetryLoaderSpout(),
				Integer.parseInt(conf.get("top.spout.parallelism").toString()));

		builder.setBolt(conf.get("top.mapperbolt.name").toString(), new MapperBolt(),
				Integer.parseInt(conf.get("top.mapperbolt.parallelism").toString()))
				.shuffleGrouping(conf.get("top.spout.name").toString());

		builder.setBolt(conf.get("top.graphbolt.name").toString(), new JanusBolt(),
				Integer.parseInt(conf.get("top.graphbolt.parallelism").toString()))
				.shuffleGrouping(conf.get("top.mapperbolt.name").toString());

		conf.setDebug(Boolean.getBoolean("top.debug"));

		conf.setNumWorkers(Integer.parseInt(conf.get("top.numWorkers").toString()));

		boolean localDeploy = Boolean.parseBoolean(conf.get("top.localDeploy").toString());
		String topologyName = conf.get("top.name").toString();

		if (localDeploy) {
			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology(topologyName, conf, builder.createTopology());
		} else {
			StormSubmitter.submitTopologyWithProgressBar(topologyName, conf, builder.createTopology());
		}

	}

	public static Config readConfigFromFile(String filename) throws IOException {
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
			}
		}

		br.close();

		return conf;
	}
}