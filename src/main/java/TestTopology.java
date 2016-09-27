import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.generated.TopologyAssignException;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import com.alibaba.jstorm.utils.JStormUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Administrator
 * On 2016/8/25 0025.
 *
 * @description
 */


public class TestTopology {
    private static Logger LOG = LoggerFactory.getLogger(TestTopology.class);

    public final static String TOPOLOGY_SPOUT_PARALLELISM_HINT = "spout.parallel";
    public final static String TOPOLOGY_BOLT_PARALLELISM_HINT = "bolt.parallel";
    private static Map conf = new HashMap<Object, Object>();

    public static void SetBuilder(TopologyBuilder builder, Map conf) {

        int spout_Parallelism_hint = JStormUtils.parseInt(conf.get(TOPOLOGY_SPOUT_PARALLELISM_HINT),1);
        int bolt_Parallelism_hint = JStormUtils.parseInt(conf.get(TOPOLOGY_BOLT_PARALLELISM_HINT),1);

        builder.setSpout("word-reader",new WordSpout(),spout_Parallelism_hint);
        builder.setBolt("word-add",new WordAddBolt(),bolt_Parallelism_hint).shuffleGrouping("word-reader");
        builder.setBolt("word-add2",new WordAdd2Bolt(),bolt_Parallelism_hint).fieldsGrouping("word-add",new Fields("wordadd"));

        int workerNum = JStormUtils.parseInt(conf.get(Config.TOPOLOGY_WORKERS),
                5);
        conf.put(Config.TOPOLOGY_WORKERS, workerNum);
    }

    public static void SetLocalTopology() throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        conf.put(TOPOLOGY_BOLT_PARALLELISM_HINT, Integer.valueOf(1));

        SetBuilder(builder, conf);

        LOG.debug("test");
        LOG.info("Submit log");
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("SplitMerge", conf, builder.createTopology());

        Thread.sleep(60000*6);

        cluster.killTopology("SplitMerge");

        cluster.shutdown();
    }

    public static void SetRemoteTopology() throws AlreadyAliveException,
            InvalidTopologyException, TopologyAssignException {

        String streamName = (String) conf.get(Config.TOPOLOGY_NAME);
        if (streamName == null) {
            streamName = "SequenceTest";
        }

        TopologyBuilder builder = new TopologyBuilder();

        SetBuilder(builder, conf);

        conf.put(Config.STORM_CLUSTER_MODE, "distributed");

        StormSubmitter.submitTopology(streamName, conf,
                builder.createTopology());

    }


    public static void LoadProperty(String prop) {
        Properties properties = new Properties();

        try {
            InputStream stream = new FileInputStream(prop);
            properties.load(stream);
        } catch (FileNotFoundException e) {
            System.out.println("No such file " + prop);
        } catch (Exception e1) {
            e1.printStackTrace();

            return;
        }

        conf.putAll(properties);
    }

    public static void LoadYaml(String confPath) {

        Yaml yaml = new Yaml();

        try {
            InputStream stream = new FileInputStream(confPath);

            conf = (Map) yaml.load(stream);
            if (conf == null || conf.isEmpty() == true) {
                throw new RuntimeException("Failed to read config file");
            }

        } catch (FileNotFoundException e) {
            System.out.println("No such file " + confPath);
            throw new RuntimeException("No config file");
        } catch (Exception e1) {
            e1.printStackTrace();
            throw new RuntimeException("Failed to read config file");
        }

        return;
    }

    public static void LoadConf(String arg) {
        if (arg.endsWith("yaml")) {
            LoadYaml(arg);
        } else {
            LoadProperty(arg);
        }
    }

    public static boolean local_mode(Map conf) {
        String mode = (String) conf.get(Config.STORM_CLUSTER_MODE);
        if (mode != null) {
            if (mode.equals("local")) {
                return true;
            }
        }

        return false;

    }

    public static void main(String[] args) throws Exception {
//        if (args.length == 0) {
//            System.err.println("Please input configuration file");
//            System.exit(-1);
//        }
//
//        LoadConf(args[0]);
        LoadConf("H:\\ideaspace\\jstorm211\\conf\\conf-local.prop");

        if (local_mode(conf)) {
            SetLocalTopology();
        } else {
            SetRemoteTopology();
        }

    }
}

