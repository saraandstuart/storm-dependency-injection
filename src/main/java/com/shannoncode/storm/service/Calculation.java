package com.shannoncode.storm.service;

/**
 * @author Stuart Shannon
 */
@FunctionalInterface
public interface Calculation
{
    int apply(int x, int y);

}
