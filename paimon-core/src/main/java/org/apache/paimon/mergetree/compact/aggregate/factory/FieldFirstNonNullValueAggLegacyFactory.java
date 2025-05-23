/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.mergetree.compact.aggregate.factory;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.mergetree.compact.aggregate.FieldAggregator;
import org.apache.paimon.mergetree.compact.aggregate.FieldFirstNonNullValueAgg;
import org.apache.paimon.types.DataType;

/** Factory for legacy name of #{@link FieldFirstNonNullValueAgg}. */
public class FieldFirstNonNullValueAggLegacyFactory implements FieldAggregatorFactory {

    public static final String LEGACY_NAME = "first_not_null_value";

    @Override
    public FieldAggregator create(DataType fieldType, CoreOptions options, String field) {
        return new FieldFirstNonNullValueAgg(identifier(), fieldType);
    }

    @Override
    public String identifier() {
        return LEGACY_NAME;
    }
}
