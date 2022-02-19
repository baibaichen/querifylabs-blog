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
package org.apache.kylin.test;

import org.apache.calcite.util.graph.OuterJoinGraph;
import org.assertj.core.api.AbstractAssert;

import java.util.function.Function;

public class OuterJoinGraphAssert extends AbstractAssert<OuterJoinGraphAssert, OuterJoinGraph> {
    protected OuterJoinGraphAssert(OuterJoinGraph outerJoinGraph, Class<?> selfType) {
        super(outerJoinGraph, selfType);
    }

    public static OuterJoinGraphAssert assertThat(OuterJoinGraph actual) {
        return new OuterJoinGraphAssert(actual, OuterJoinGraphAssert.class);
    }

    public static OuterJoinGraphAssert assertThat(Function<String, OuterJoinGraph> factory, String query) {
        return new OuterJoinGraphAssert(factory.apply(query), OuterJoinGraphAssert.class);
    }

    public OuterJoinGraphAssert exactMatch(OuterJoinGraph pattern) {
        // check that actual TolkienCharacter we want to make assertions on is not null.
        isNotNull();
        // check assertion logic
        if (!OuterJoinGraph.exactMatch(actual, pattern)) {
            failWithMessage("Query Join Graph :\n%s doesn't exactly match:\n%s ", actual, pattern);
        }
        // return this to allow chaining other assertion methods
        return this;
    }
}
