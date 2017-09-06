package com.shannoncode.storm.bolt;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.shannoncode.storm.service.Add;
import com.shannoncode.storm.service.Calculation;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Stuart Shannon
 */
public class CalculationBolt extends BaseRichBolt
{
    private static final Logger LOG = LoggerFactory.getLogger(CalculationBolt.class);

    private transient OutputCollector collector;

    private transient Calculation calculation;

    private Module module;

    public CalculationBolt(Module module)
    {
        this.module = module;
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields("result"));
    }

    public void prepare(Map stormConf,
                        TopologyContext context,
                        OutputCollector collector)
    {
        this.collector = collector;

        Injector injector = Guice.createInjector(module);
        calculation = injector.getInstance(Calculation.class);

    }

    public void execute(Tuple tuple)
    {
        int x = (Integer) tuple.getValueByField("x");
        int y = (Integer) tuple.getValueByField("y");

        int result = calculation.apply(x, y);

        LOG.info("CalculationBolt emitted: " + result);

        collector.emit(new Values(result));
        collector.ack(tuple);

    }

}
