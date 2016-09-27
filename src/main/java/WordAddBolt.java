import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.Map;

/**
 * Created by Administrator
 * On 2016/8/9 0009.
 *
 * @description
 */
public class WordAddBolt extends BaseRichBolt {
    private OutputCollector collector;
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector=outputCollector;
    }

    public void execute(Tuple tuple) {
        String word = tuple.getString(0)+"!!!";
        System.out.println ("addup2-------------"+word);
        collector.emit(tuple,new Values(word));
        collector.ack(tuple);
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("wordadd"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
