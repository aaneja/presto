package com.facebook.presto.sql.planner;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.plan.JoinDistributionType;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.JoinNode;

import java.util.List;
import java.util.Optional;

public class JoinConstraintNode2
        implements Constraint
{
    private static final Logger LOG = Logger.get(JoinConstraintNode2.class);
    private final JoinType type;
    private final Optional<JoinDistributionType> distributionType;
    private final List<Constraint> children;

    public JoinConstraintNode2(JoinType type, Optional<JoinDistributionType> distributionType, List<Constraint> children)
    {
        this.type = type;
        this.distributionType = distributionType;
        this.children = children;
    }

    public static boolean matches(Constraint constraint, PlanNode toCompare)
    {
        if (constraint instanceof JoinConstraintNode2) {
            if (toCompare instanceof JoinNode) {
                JoinNode toMatch = (JoinNode) toCompare;
                JoinConstraintNode2 joinConstraint = (JoinConstraintNode2) constraint;
                if (toMatch.getType() != joinConstraint.getType() ||
                        (joinConstraint.getDistributionType().isPresent() && !toMatch.getDistributionType().isPresent()) ||
                        (joinConstraint.getDistributionType().isPresent() && joinConstraint.getDistributionType().get() != toMatch.getDistributionType().get())) {
                    return false;
                }
                // Current JoinNode matches, check the children
                return matches(joinConstraint.getChildren().get(0), toMatch.getLeft()) &&
                        matches(joinConstraint.getChildren().get(1), toMatch.getRight());
            }

            // The current node, could be a Projection introduced during join reordering
            if (toCompare instanceof ProjectNode) {
                return matches(constraint, ((ProjectNode) toCompare).getSource());
            }

            // The current node is a leaf node, so it does not match the Join constraint
            return false;
        }
        else if (constraint instanceof RelationConstraint) {
            if (toCompare instanceof JoinNode) {
                // Expected a terminating leaf node
                return false;
            }
            // TODO : We need a canonical property that can identify the 'named' relation at this point, for now we assume a match
            LOG.info("Found a node %s", toCompare.getId());
            // Assuming id is that property for now
            return ((RelationConstraint) constraint).getName().equalsIgnoreCase(toCompare.getId().toString());
        }
        return false;
    }

    @Override
    public boolean isLeaf()
    {
        return false;
    }

    public JoinType getType()
    {
        return type;
    }

    public Optional<JoinDistributionType> getDistributionType()
    {
        return distributionType;
    }

    public List<Constraint> getChildren()
    {
        return children;
    }
}
