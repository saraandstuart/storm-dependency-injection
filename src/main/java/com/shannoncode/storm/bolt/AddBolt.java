package com.shannoncode.storm.bolt;

import com.shannoncode.storm.service.Add;
import com.shannoncode.storm.service.Calculation;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @author Stuart Shannon
 */
public class AddBolt extends BaseRichBolt
{
    private transient OutputCollector collector;

    private transient Calculation calculation;

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("result"));
    }

    public void prepare(Map stormConf,
                        TopologyContext context,
                        OutputCollector collector)
    {
        this.collector = collector;

        calculation = new Add();

    }

    public void execute(Tuple tuple)
    {
        int x = (Integer) tuple.getValueByField("x");
        int y = (Integer) tuple.getValueByField("y");

        int result = calculation.apply(x, y);

        collector.emit(new Values(result));
        collector.ack(tuple);

    }

}
