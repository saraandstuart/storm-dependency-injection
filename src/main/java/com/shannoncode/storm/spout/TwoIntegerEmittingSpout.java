package com.shannoncode.storm.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Map;

/**
 * @author Stuart Shannon
 */
public class TwoIntegerEmittingSpout extends BaseRichSpout
{
    private transient SpoutOutputCollector collector;

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("x", "y"));
    }

    public void open(Map stormConf,
                     TopologyContext context,
                     SpoutOutputCollector collector)
    {
        this.collector = collector;
    }

    public void nextTuple()
    {
        collector.emit(new Values(3, 4));
    }
}
