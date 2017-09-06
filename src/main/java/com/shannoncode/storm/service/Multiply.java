package com.shannoncode.storm.service;

/**
 * @author Stuart Shannon
 */
public class Multiply implements Calculation
{
    public int apply(int x, int y)
    {
        return x * y;
    }
}
