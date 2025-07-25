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

package org.apache.paimon.flink.sink.listener;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.partition.actions.PartitionMarkDoneAction;
import org.apache.paimon.table.FileStoreTable;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/** The class is only applicable for {@link CustomPartitionMarkDoneActionTest}. */
public class MockCustomPartitionMarkDoneAction implements PartitionMarkDoneAction {

    private static final Set<String> markedDonePartitions = new HashSet<>();

    private String tableName;

    @Override
    public void open(FileStoreTable fileStoreTable, CoreOptions options) {
        this.tableName = fileStoreTable.fullName();
    }

    @Override
    public void markDone(String partition) {
        MockCustomPartitionMarkDoneAction.markedDonePartitions.add(
                String.format("table=%s,partition=%s", tableName, partition));
    }

    public static Set<String> getMarkedDonePartitions() {
        return MockCustomPartitionMarkDoneAction.markedDonePartitions;
    }

    @Override
    public void close() throws IOException {}
}
