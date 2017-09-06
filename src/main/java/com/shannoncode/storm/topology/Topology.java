package com.shannoncode.storm.topology;

import com.google.inject.AbstractModule;
import com.shannoncode.storm.bolt.CalculationBolt;
import com.shannoncode.storm.service.Add;
import com.shannoncode.storm.service.Calculation;
import com.shannoncode.storm.service.Multiply;
import com.shannoncode.storm.spout.TwoIntegerEmittingSpout;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;

import java.io.Serializable;

/**
 * @author Stuart Shannon
 */
public class Topology
{
    private static final String SPOUT = "SPOUT";
    private static final String BOLT_CALCULATION = "BOLT_CALCULATION";

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout(SPOUT, new TwoIntegerEmittingSpout());

        CalculationBolt calculationBolt = new CalculationBolt(new AddModule());

        builder.setBolt(BOLT_CALCULATION, calculationBolt)
                .noneGrouping(SPOUT);

        Config config = new Config();
        config.setNumWorkers(2);

        StormSubmitter.submitTopology("dependency-injection-poc", config, builder.createTopology());

    }

    private static class AddModule extends AbstractModule implements Serializable
    {
        protected void configure()
        {
            bind(Calculation.class).to(Add.class);
        }
    }

    private static class MultiplyModule extends AbstractModule implements Serializable
    {
        protected void configure()
        {
            bind(Calculation.class).to(Multiply.class);
        }
    }
}
