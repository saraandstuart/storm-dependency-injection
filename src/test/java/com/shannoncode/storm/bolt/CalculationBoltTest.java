package com.shannoncode.storm.bolt;

import com.google.inject.AbstractModule;
import com.shannoncode.storm.service.Add;
import com.shannoncode.storm.service.Calculation;
import com.shannoncode.storm.service.Multiply;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import java.io.Serializable;

import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/**
 * @author Stuart Shannon
 */
public class CalculationBoltTest
{
    @Rule
    public MockitoRule rule = MockitoJUnit.rule();

    @Mock
    private OutputCollector collector;

    @Test
    public void add()
    {
        //given
        CalculationBolt bolt = new CalculationBolt(new AddModule());
        Values expected = new Values(7);
        bolt.prepare(null, null, collector);

        //when
        bolt.execute(mockTuple(3, 4));

        //then
        verify(collector).emit(eq(new Values(7)));
    }

    @Test
    public void multiply()
    {
        //given
        CalculationBolt bolt = new CalculationBolt(new MultiplyModule());
        Values expected = new Values(7);
        bolt.prepare(null, null, collector);

        //when
        bolt.execute(mockTuple(3, 4));

        //then
        verify(collector).emit(eq(new Values(12)));
    }

    private Tuple mockTuple(int x, int y)
    {
        Tuple tuple = mock(Tuple.class);

        given(tuple.getValueByField("x")).willReturn(x);
        given(tuple.getValueByField("y")).willReturn(y);

        return tuple;
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
