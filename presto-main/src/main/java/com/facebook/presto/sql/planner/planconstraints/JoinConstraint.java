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
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.JoinDistributionType;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@ThriftStruct
public class JoinConstraint
        extends PlanConstraint
{
    public static final Pattern TEST_SETUP_TABLE_NAME_PATTERN = Pattern.compile("(\\w+):sf\\d+", Pattern.MULTILINE);
    public static final Pattern TPCH_TABLE_NAME_PATTERN = Pattern.compile("connectorId='tpch', connectorHandle='(.*?):.*?'", Pattern.MULTILINE);
    public static final Pattern TPCDS_TABLE_NAME_PATTERN = Pattern.compile("connectorHandle='tpcds:(.*?):.*?'", Pattern.MULTILINE);
    public static final Pattern HIVE_TABLE_NAME_PATTERN = Pattern.compile("tableName=(.*?),", Pattern.MULTILINE);
    public static final Pattern ICEBERG_TABLE_NAME_PATTERN = Pattern.compile("connectorHandle='(.*?)\\$data@Optional\\[[0-9]+\\]',", Pattern.MULTILINE);
    private static final Logger LOG = Logger.get(JoinConstraint.class);
    private final JoinType joinType;
    private final Optional<JoinDistributionType> distributionType;
    private final List<PlanConstraint> children;

    @ThriftConstructor
    @JsonCreator
    public JoinConstraint(@JsonProperty("joinType") JoinType joinType,
            @JsonProperty("distributionType") Optional<JoinDistributionType> distributionType,
            @JsonProperty("children") List<PlanConstraint> children)
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
        else if (constraint instanceof CardinalityConstraint) {
            CardinalityConstraint cardinalityConstraint = (CardinalityConstraint) constraint;
            return matches(lookup, cardinalityConstraint.getNode(), toCompare);
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
                String tableName = extractTableName(tableHandle);
                LOG.info("Extracted [%s] from table handle : %s", tableName, tableHandle);
                return relationConstraint.getName().equalsIgnoreCase(tableName);
            }
            if (toCompare instanceof FilterNode) {
                return matches(lookup, constraint, ((FilterNode) toCompare).getSource());
            }
            else {
                // Assuming id is that property for now
                LOG.info("Found a node %s", toCompare.getId());
                return relationConstraint.getName().equalsIgnoreCase(toCompare.getId().toString());
            }
        }
        return false;
    }

    // TODO : Enhance this for other connectors as well, this is a hack at the moment
    private static String extractTableName(String field)
    {
        List<Pattern> matchers = ImmutableList.of(TEST_SETUP_TABLE_NAME_PATTERN, TPCH_TABLE_NAME_PATTERN, TPCDS_TABLE_NAME_PATTERN, HIVE_TABLE_NAME_PATTERN, ICEBERG_TABLE_NAME_PATTERN);
        for (Pattern pattern : matchers) {
            Matcher matcher = pattern.matcher(field);
            if (matcher.find()) {
                return matcher.group(1);
            }
        }
        throw new RuntimeException("Unable to extract table name from " + field);
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

    @Override
    public int hashCode()
    {
        return Objects.hash(joinType, distributionType, children);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }

        JoinConstraint o = (JoinConstraint) obj;
        return Objects.equals(joinType, o.joinType) &&
                Objects.equals(distributionType, o.distributionType) &&
                Objects.equals(children, o.children);
    }
}