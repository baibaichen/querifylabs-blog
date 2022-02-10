/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
package evolution.org.apache.kylin.calcite.cost;

import evolution.org.apache.kylin.calcite.KylinPlannerContext;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;

/**
 * Refer to <code>HiveVolcanoPlanner</code>, now only implement {@link #createPlanner}
 */
public class KylinVolcanoPlanner {

    private KylinVolcanoPlanner(){}

    private static final boolean ENABLE_COLLATION_TRAIT = true;
    private static final boolean ENABLE_DISTRIBUTION_TRAIT = false;

    public static RelOptPlanner createPlanner(KylinPlannerContext conf) {
        RelOptPlanner planner = new VolcanoPlanner(conf);
        planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
        if (ENABLE_COLLATION_TRAIT) {
            planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
        }
        if (ENABLE_DISTRIBUTION_TRAIT) {
            planner.addRelTraitDef(RelDistributionTraitDef.INSTANCE);
        }
        return planner;
    }
}
