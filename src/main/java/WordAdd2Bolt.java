import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator
 * On 2016/8/9 0009.
 *
 * @description
 */
public class WordAdd2Bolt extends BaseRichBolt {
    public static Logger logger = org.apache.log4j.Logger.getLogger(IRichBolt.class);
    Integer id;
    String name;
    private OutputCollector collector;
    Map<String,Integer> counters;
    private FileOutputStream out ;
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.counters = new HashMap<String, Integer>();
        this.collector = outputCollector;
        this.name = topologyContext.getThisComponentId();
        this.id = topologyContext.getThisTaskId();
        try {
            out = new FileOutputStream("mylog");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void execute(Tuple tuple) {
        String str = tuple.getString(0);

        if (!counters.containsKey(str))
            counters.put(str,1);
        else{
            Integer c = counters.get(str) +1;
            counters.put(str,c);
        }
        System.out.println ("addup2-------------"+str+"num"+counters.get(str));
        logger.info("addup2-------------"+str+"num"+counters.get(str));
        String newline = System.getProperty("line.separator");
        try {
            out.write(("addup2-------------"+str+"num"+counters.get(str)+newline).getBytes());
            out.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        collector.ack(tuple);
    }

    public void cleanup() {
        System.out.println("-- Word Counter [" + name + "-" + id + "] --");
        for (Map.Entry<String, Integer> entry : counters.entrySet()) {
            System.out.println(entry.getKey() + ": " + entry.getValue());
        }
        counters.clear();
        try {
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
