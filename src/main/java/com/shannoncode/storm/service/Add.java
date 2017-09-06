package com.shannoncode.storm.service;

/**
 * @author Stuart Shannon
 */
public class Add implements Calculation
{
    public int apply(int x, int y)
    {
        return x + y;
    }
}
