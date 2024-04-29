package com.facebook.presto.sql.planner;

public interface Constraint
{
    default boolean isLeaf()
    {
        return true;
    }
}
