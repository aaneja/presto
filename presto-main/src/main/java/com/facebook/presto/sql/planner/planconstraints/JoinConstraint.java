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

import com.facebook.drift.annotations.ThriftConstructor;
import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.spi.plan.JoinDistributionType;
import com.facebook.presto.spi.plan.JoinType;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

@ThriftStruct
public class JoinConstraint
        extends PlanConstraint
{
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
