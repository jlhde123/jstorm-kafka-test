import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import java.util.*;

/**
 * Created by Administrator
 * On 2016/8/9 0009.
 *
 * @description
 */
public class WordSpout extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private static Integer i=0;
    private String []words={"zero","one","two","three","four"};
    private ConsumerConnector consumer;
    private Random r= new Random(System.currentTimeMillis());
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector=spoutOutputCollector;
        Properties props = new Properties();
        //zookeeper 配置
        props.put("zookeeper.connect", "127.0.0.1:2181");

        //group 代表一个消费组
        props.put("group.id", "jd-group");

        //zk连接超时
        props.put("zookeeper.session.timeout.ms", "4000");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");
        //序列化类
        props.put("serializer.class", "kafka.serializer.StringEncoder");

        ConsumerConfig config = new ConsumerConfig(props);
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
    }

    public void close() {

    }

    public void activate() {

    }

    public void deactivate() {

    }

    public void nextTuple() {
//        Utils.sleep(500);
//        System.out.println ("-------------"+i++);
//        String word = words[r.nextInt(4)];

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put("TEST-TOPIC", new Integer(1));

        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        StringDecoder valueDecoder = new StringDecoder(new VerifiableProperties());

        Map<String, List<KafkaStream<String, String>>> consumerMap =
                consumer.createMessageStreams(topicCountMap,keyDecoder,valueDecoder);
        KafkaStream<String, String> stream = consumerMap.get("TEST-TOPIC").get(0);
        ConsumerIterator<String, String> it = stream.iterator();
        while (it.hasNext()){
            collector.emit(new Values(it.next().message()),it.next().message());
        }

    }

    public void ack(Object o) {
        System.out.println ("ack="+(String) o);
    }

    public void fail(Object o) {
        System.out.println ("fail="+o);
    }
}
