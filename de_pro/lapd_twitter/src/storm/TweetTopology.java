package storm;

import hbase.HBaseBolt;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import org.apache.log4j.Logger;

class TweetTopology {
	private static final Logger LOG = Logger.getLogger(TweetTopology.class);
	public static void main(String[] args) throws Exception {
		// prepare properties
		Properties hbaseConfig = new Properties();
		hbaseConfig.setProperty("hbase_site", "/etc/hbase/conf/hbase-site.xml");
		hbaseConfig.setProperty("hdfs_core_site", "/etc/hadoop/conf/core-site.xml");
		hbaseConfig.setProperty("hdfs_site", "/etc/hadoop/conf/hdfs-site.xml");
		hbaseConfig.setProperty("hbase_table", "lapd_twitter");
		hbaseConfig.setProperty("habse_cf", "testcf");
		hbaseConfig.setProperty("habse_num_colFamilies", "1");
		
		// create the topology
		TopologyBuilder builder = new TopologyBuilder();

		// now create the tweet spout with the credentials
		TweetSpout tweetSpout = new TweetSpout("7L23PjrlKZNPt3ydmCRoZBcsG",
				"6T5KHw79ARnfCLHS0xKNk0DaDVItRTPdQt3lRfPfQudfJgFx4d",
				"4814179994-bhDWirXU4zeNhcWsyFdTi9MJnOEa71Lnuc1W83l",
				"IfTTap8yXGaYjHkeZL21g6jqs7icCzmCqjwZMSMGBpXGI");

		// attach the tweet spout to the topology - parallelism of 1
		builder.setSpout("tweet-spout", tweetSpout, 1);
		
		// attach hbase bolt
		builder.setBolt("hbase-bolt", HBaseBolt.make(hbaseConfig), 1).shuffleGrouping("tweet-spout");
		
		// attach the report bolt using global grouping - parallelism of 1
//		builder.setBolt("report-bolt", new ReportBolt(), 1).globalGrouping("hbase-spout");

		// create the default config object
		Config conf = new Config();

		// set the config in debugging mode
		conf.setDebug(true);

		if (args != null && args.length > 0) {

			// run it in a live cluster

			// set the number of workers for running all spout and bolt tasks
			conf.setNumWorkers(3);

			// create the topology and submit with config
			StormSubmitter.submitTopology(args[0], conf,
					builder.createTopology());

		} else {

			// run it in a simulated local cluster

			// set the number of threads to run - similar to setting number of
			// workers in live cluster
			conf.setMaxTaskParallelism(3);

			// create the local cluster instance
			LocalCluster cluster = new LocalCluster();

			// submit the topology to the local cluster
			cluster.submitTopology("tweet-word-count", conf,
					builder.createTopology());

			// let the topology run for 300 seconds. note topologies never
			// terminate!
			Utils.sleep(300000);

			// now kill the topology
			cluster.killTopology("tweet-word-count");

			// we are done, so shutdown the local cluster
			cluster.shutdown();
		}
	}
}
