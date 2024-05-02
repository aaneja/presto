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
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Lookup;
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

    public static boolean matches(Lookup lookup, PlanConstraint constraint, PlanNode toCompare)
    {
        if (toCompare instanceof GroupReference) {
            toCompare = lookup.resolve(toCompare);
        }

        if (constraint instanceof JoinConstraint) {
            JoinConstraint joinConstraint = (JoinConstraint) constraint;
            if (toCompare instanceof JoinNode) {
                JoinNode toMatch = (JoinNode) toCompare;
                if (toMatch.getType() != joinConstraint.getJoinType() ||
                        (joinConstraint.getDistributionType().isPresent() && !toMatch.getDistributionType().isPresent()) ||
                        (joinConstraint.getDistributionType().isPresent() && joinConstraint.getDistributionType().get() != toMatch.getDistributionType().get())) {
                    return false;
                }
                // Current JoinNode matches, check the children
                return matches(lookup, joinConstraint.getChildren().get(0), toMatch.getLeft()) &&
                        matches(lookup, joinConstraint.getChildren().get(1), toMatch.getRight());
            }

            // The current node, could be a Projection introduced during join reordering
            if (toCompare instanceof ProjectNode) {
                return matches(lookup, constraint, ((ProjectNode) toCompare).getSource());
            }

            // The current node does not match as-is, check the children for a sub-graph match
            return matches(lookup, joinConstraint.getChildren().get(0), toCompare) ||
                    matches(lookup, joinConstraint.getChildren().get(1), toCompare);
        }
        else if (constraint instanceof RelationConstraint) {
            RelationConstraint relationConstraint = (RelationConstraint) constraint;
            if (toCompare instanceof JoinNode) {
                // Expected a terminating leaf node
                return false;
            }
            // TODO : We need a canonical property that can identify the 'named' relation at this point, for now we assume a match

            if (toCompare instanceof TableScanNode) {
                TableScanNode tableScanNode = (TableScanNode) toCompare;
                String tableHandle = tableScanNode.getTable().getConnectorHandle().toString();
                LOG.info("Found a table with handle : %s", tableHandle);
                return tableHandle.contains(relationConstraint.getName());
            }
            else {
                // Assuming id is that property for now
                LOG.info("Found a node %s", toCompare.getId());
                return relationConstraint.getName().equalsIgnoreCase(toCompare.getId().toString());
            }
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
