/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner.planconstraints;

import com.facebook.airlift.log.Logger;
import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.spi.plan.JoinDistributionType;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.fasterxml.jackson.annotation.JsonCreator;

import java.util.List;
import java.util.Optional;

@ThriftStruct
public class JoinConstraint
        extends PlanConstraint
{
    private static final Logger LOG = Logger.get(JoinConstraint.class);
    private final JoinType joinType;
    private final Optional<JoinDistributionType> distributionType;
    private final List<PlanConstraint> children;

    @ThriftConstructor
    @JsonCreator
    public JoinConstraint(JoinType joinType, Optional<JoinDistributionType> distributionType, List<PlanConstraint> children)
    {
        this.joinType = joinType;
        this.distributionType = distributionType;
        this.children = children;
    }

    public static boolean matches(PlanConstraint constraint, PlanNode toCompare)
    {
        if (constraint instanceof JoinConstraint) {
            if (toCompare instanceof JoinNode) {
                JoinNode toMatch = (JoinNode) toCompare;
                JoinConstraint joinConstraint = (JoinConstraint) constraint;
                if (toMatch.getType() != joinConstraint.getJoinType() ||
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

    @ThriftField(1)
    public JoinType getJoinType()
    {
        return joinType;
    }

    @ThriftField(2)
    public Optional<JoinDistributionType> getDistributionType()
    {
        return distributionType;
    }

    @ThriftField(3)
    public List<PlanConstraint> getChildren()
    {
        return children;
    }
}
