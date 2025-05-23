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

package org.apache.paimon.spark.procedure;

import org.apache.paimon.CoreOptions;
import org.apache.paimon.options.ExpireConfig;
import org.apache.paimon.table.ExpireSnapshots;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.utils.ProcedureUtils;

import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.HashMap;

import static org.apache.spark.sql.types.DataTypes.IntegerType;
import static org.apache.spark.sql.types.DataTypes.StringType;

/** A procedure to expire snapshots. */
public class ExpireSnapshotsProcedure extends BaseProcedure {

    private static final ProcedureParameter[] PARAMETERS =
            new ProcedureParameter[] {
                ProcedureParameter.required("table", StringType),
                ProcedureParameter.optional("retain_max", IntegerType),
                ProcedureParameter.optional("retain_min", IntegerType),
                ProcedureParameter.optional("older_than", StringType),
                ProcedureParameter.optional("max_deletes", IntegerType),
                ProcedureParameter.optional("options", StringType)
            };

    private static final StructType OUTPUT_TYPE =
            new StructType(
                    new StructField[] {
                        new StructField(
                                "deleted_snapshots_count", IntegerType, false, Metadata.empty())
                    });

    protected ExpireSnapshotsProcedure(TableCatalog tableCatalog) {
        super(tableCatalog);
    }

    @Override
    public ProcedureParameter[] parameters() {
        return PARAMETERS;
    }

    @Override
    public StructType outputType() {
        return OUTPUT_TYPE;
    }

    @Override
    public InternalRow[] call(InternalRow args) {
        Identifier tableIdent = toIdentifier(args.getString(0), PARAMETERS[0].name());
        Integer retainMax = args.isNullAt(1) ? null : args.getInt(1);
        Integer retainMin = args.isNullAt(2) ? null : args.getInt(2);
        String olderThanStr = args.isNullAt(3) ? null : args.getString(3);
        Integer maxDeletes = args.isNullAt(4) ? null : args.getInt(4);
        String options = args.isNullAt(5) ? null : args.getString(5);

        return modifyPaimonTable(
                tableIdent,
                table -> {
                    HashMap<String, String> dynamicOptions = new HashMap<>();
                    ProcedureUtils.putAllOptions(dynamicOptions, options);
                    table = table.copy(dynamicOptions);
                    ExpireSnapshots expireSnapshots = table.newExpireSnapshots();

                    CoreOptions tableOptions = ((FileStoreTable) table).store().options();
                    ExpireConfig.Builder builder =
                            ProcedureUtils.fillInSnapshotOptions(
                                    tableOptions, retainMax, retainMin, olderThanStr, maxDeletes);
                    int deleted = expireSnapshots.config(builder.build()).expire();
                    return new InternalRow[] {newInternalRow(deleted)};
                });
    }

    public static ProcedureBuilder builder() {
        return new BaseProcedure.Builder<ExpireSnapshotsProcedure>() {
            @Override
            public ExpireSnapshotsProcedure doBuild() {
                return new ExpireSnapshotsProcedure(tableCatalog());
            }
        };
    }

    @Override
    public String description() {
        return "ExpireSnapshotsProcedure";
    }
}
