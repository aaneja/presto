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

import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class PlanConstraintsHolder
{
    public static final PlanConstraintsHolder EMPTY_PLAN_CONSTRAINTS_HOLDER = new PlanConstraintsHolder(ImmutableList.of(), HashMultimap.create());
    private final List<PlanConstraint> planConstraints;
    private final Multimap<String, String> aliases;
    public PlanConstraintsHolder(List<PlanConstraint> planConstraints, Multimap<String, String> aliases)
    {
        this.planConstraints = requireNonNull(planConstraints, "planconstraints is null");
        this.aliases = requireNonNull(aliases, "aliases is null");
    }

    public List<PlanConstraint> getPlanConstraints()
    {
        return planConstraints;
    }

    public Multimap<String, String> getAliases()
    {
        return aliases;
    }
}
