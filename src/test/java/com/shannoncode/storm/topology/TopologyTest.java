package com.shannoncode.storm.topology;

import com.google.inject.AbstractModule;
import com.shannoncode.storm.bolt.CalculationBolt;
import com.shannoncode.storm.service.Add;
import com.shannoncode.storm.service.Calculation;
import com.shannoncode.storm.service.Multiply;
import org.apache.storm.Config;
import org.apache.storm.ILocalCluster;
import org.apache.storm.Testing;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.testing.*;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertThat;

/**
 * @author Stuart Shannon
 */
public class TopologyTest
{
    private static final String SPOUT = "SPOUT";
    private static final String BOLT_CALCULATION = "BOLT_CALCULATION";

    private static final List<Values> EXPECTED_SPOUT_OUTPUT = new ArrayList<>();
    private static final List<Values> EXPECTED_BOLT_OUTPUT_CALCULATION = new ArrayList<>();

    @Test
    public void topologyFlow()
    {
        //given
        MkClusterParam mkClusterParam = new MkClusterParam();
        Config daemonConf = new Config();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
        mkClusterParam.setDaemonConf(daemonConf);

        //when
        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new CalculationTestJob());
    }

    private class CalculationTestJob implements TestJob
    {
        public void run(ILocalCluster cluster) throws Exception
        {
            TopologyBuilder builder = new TopologyBuilder();

            builder.setSpout(SPOUT, new FeederSpout(new Fields("x", "y")));

            CalculationBolt calculationBolt = new CalculationBolt(new AddModule());

            builder.setBolt(BOLT_CALCULATION, calculationBolt)
                    .noneGrouping(SPOUT);

            StormTopology topology = builder.createTopology();

            MockedSources mockedSources = new MockedSources();
            mockedSources.addMockData(SPOUT, new Values(3, 4));

            Config stormConf = new Config();
            stormConf.setNumWorkers(2);

            CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
            completeTopologyParam.setMockedSources(mockedSources);
            completeTopologyParam.setStormConf(stormConf);

            Map result = Testing.completeTopology(cluster, topology, completeTopologyParam);

            List<Values> actualSpoutTuples = Testing.readTuples(result, SPOUT);
            List<Values> actualCalculationBoltTuples = Testing.readTuples(result, BOLT_CALCULATION);

            assertThat(actualSpoutTuples, isEqualsIgnoresOrder(EXPECTED_SPOUT_OUTPUT));
            assertThat(actualCalculationBoltTuples, isEqualsIgnoresOrder(EXPECTED_BOLT_OUTPUT_CALCULATION));
        }
    }

    private static Matcher<List<Values>> isEqualsIgnoresOrder(final List<Values> expected)
    {
        return new TypeSafeMatcher<List<Values>>()
        {
            @Override
            protected boolean matchesSafely(List<Values> actual) {
                return Testing.multiseteq(expected, actual);
            }

            public void describeTo(Description description) {
                description.appendValue(expected);
            }
        };
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

    static
    {
        EXPECTED_SPOUT_OUTPUT.add(new Values(3, 4));
        EXPECTED_BOLT_OUTPUT_CALCULATION.add(new Values(7));
    }

}
