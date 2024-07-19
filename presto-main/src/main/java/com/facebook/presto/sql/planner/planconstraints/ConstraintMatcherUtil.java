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

import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.JoinNode;

import java.util.List;
import java.util.NavigableMap;
import java.util.Optional;

import static com.facebook.presto.operator.scalar.MathFunctions.round;
import static com.facebook.presto.spi.statistics.SourceInfo.ConfidenceLevel.HIGH;

public class ConstraintMatcherUtil
{

    private ConstraintMatcherUtil() {}

    public static boolean matches(Lookup lookup, PlanConstraint constraint, PlanNode toCompare, NavigableMap<SourceLocation, String> sourceLocationAliasMap)
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
                return matches(lookup, joinConstraint.getChildren().get(0), toMatch.getLeft(), sourceLocationAliasMap) &&
                        matches(lookup, joinConstraint.getChildren().get(1), toMatch.getRight(), sourceLocationAliasMap);
            }

            // The current node, could be a Projection introduced during join reordering
            if (toCompare instanceof ProjectNode) {
                return matches(lookup, constraint, ((ProjectNode) toCompare).getSource(), sourceLocationAliasMap);
            }

            // The current node does not match as-is, check the children for a sub-graph match
            return matches(lookup, joinConstraint.getChildren().get(0), toCompare, sourceLocationAliasMap) ||
                    matches(lookup, joinConstraint.getChildren().get(1), toCompare, sourceLocationAliasMap);
        }
        else if (constraint instanceof CardinalityConstraint) {
            CardinalityConstraint cardinalityConstraint = (CardinalityConstraint) constraint;
            return matches(lookup, cardinalityConstraint.getNode(), toCompare, sourceLocationAliasMap);
        }
        else if (constraint instanceof RelationConstraint) {
            RelationConstraint relationConstraint = (RelationConstraint) constraint;

            if (toCompare.getSourceLocation().isPresent()) {
                SourceLocation nodeLocation = toCompare.getSourceLocation().get();
                String alias = findAlias(nodeLocation, sourceLocationAliasMap);
                return relationConstraint.getName().equalsIgnoreCase(alias);
            }
            else {
                return false;
            }
        }
        return false;
    }

    public static Optional<PlanNodeStatsEstimate> getStatsEstimate(List<CardinalityConstraint> cardinalityConstraints,
            NavigableMap<SourceLocation, String> sourceLocationAliasMap,
            PlanNode planNode,
            PlanNodeStatsEstimate statsEstimate,
            Lookup lookup)
    {
        Optional<PlanNodeStatsEstimate> newStatsEstimate = Optional.empty();

        Optional<Long> cardinality = getCardinality(cardinalityConstraints, sourceLocationAliasMap, planNode, lookup);
        if (cardinality.isPresent()) {
            double cardinalityAsDouble = cardinality.get().doubleValue();
            double factor = cardinalityAsDouble / statsEstimate.getOutputRowCount();
            double totalSize = round(factor * statsEstimate.getOutputSizeInBytes(), 0);
            PlanNodeStatsEstimate.Builder builder = PlanNodeStatsEstimate.buildFrom(statsEstimate);
            builder.setOutputRowCount(cardinalityAsDouble);
            builder.setTotalSize(totalSize);
            builder.setConfidence(HIGH);
            newStatsEstimate = Optional.of(builder.build());
        }

        return newStatsEstimate;
    }

    private static String findAlias(SourceLocation nodeLocation, NavigableMap<SourceLocation, String> sourceLocationAliasMap)
    {
        // Find the first entry that is greater than or equal to the node location
        SourceLocation aliasStartLocation = sourceLocationAliasMap.floorKey(nodeLocation);
        if (aliasStartLocation == null) {
            return null;
        }
        return sourceLocationAliasMap.get(aliasStartLocation);
    }

    private static Optional<Long> getCardinality(List<CardinalityConstraint> cardinalityConstraints,
            NavigableMap<SourceLocation, String> sourceLocationAliasMap,
            PlanNode planNode,
            Lookup lookup)
    {
        for (CardinalityConstraint constraint : cardinalityConstraints) {
            // TODO : Fix me, We need to plumb the source location to alias map
            if (matches(lookup, constraint, planNode, sourceLocationAliasMap)) {
                return Optional.of(constraint.getCardinalityEstimate().getCardinality());
            }
        }
        return Optional.empty();
    }
}
