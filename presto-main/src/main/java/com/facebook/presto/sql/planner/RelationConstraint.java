package com.facebook.presto.sql.planner;

public class RelationConstraint implements Constraint
{
    private final String name;

    public String getName()
    {
        return name;
    }

    @Override
    public boolean isLeaf()
    {
        return true;
    }

    public RelationConstraint(String name) {this.name = name;}
}
