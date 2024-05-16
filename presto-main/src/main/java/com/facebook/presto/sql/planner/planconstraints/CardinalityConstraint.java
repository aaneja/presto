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
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

@ThriftStruct
public class CardinalityConstraint
        extends PlanConstraint
{
    private final PlanConstraint node;
    private final CardinalityEstimate cardinalityEstimate;

    @ThriftConstructor
    @JsonCreator
    public CardinalityConstraint(@JsonProperty("node") PlanConstraint node,
            @JsonProperty("cardinalityEstimate") CardinalityEstimate cardinalityEstimate)
    {
        this.node = node;
        this.cardinalityEstimate = cardinalityEstimate;
    }

    @ThriftField(1)
    public PlanConstraint getNode()
    {
        return node;
    }

    @ThriftField(2)
    public CardinalityEstimate getCardinalityEstimate()
    {
        return cardinalityEstimate;
    }

    @Override
    public boolean isLeaf()
    {
        return true;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(node, cardinalityEstimate);
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

        CardinalityConstraint o = (CardinalityConstraint) obj;
        return Objects.equals(node, o.node) &&
                Objects.equals(cardinalityEstimate, o.cardinalityEstimate);
    }
}
