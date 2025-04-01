package org.apache.paimon.catalog;
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

import org.apache.paimon.Snapshot;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTCatalog;
import org.apache.paimon.rest.RESTCatalogOptions;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataTypes;

import org.apache.paimon.shade.guava30.com.google.common.collect.Lists;
import org.apache.paimon.shade.guava30.com.google.common.collect.Maps;

import org.apache.hadoop.conf.Configuration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;

import static org.apache.paimon.rest.RESTCatalogOptions.*;
import static org.apache.paimon.rest.RESTTokenFileIO.DATA_TOKEN_ENABLED;
import static org.assertj.core.api.Assertions.assertThat;

public class BennettCatalogTest extends CatalogTestBase {
    @BeforeEach
    @Override
    public void setUp() throws Exception {
        Options options = new Options();
        options.set(RESTCatalogOptions.URI, "http://localhost:8080");
        options.set(CatalogOptions.WAREHOUSE, "test_catalog");
        options.set(TOKEN_PROVIDER, "dlf");
        options.set(DLF_ACCESS_KEY_ID, "userAccessKeyId");
        options.set(DLF_ACCESS_KEY_SECRET, "userAccessKeySecretEncrypted");
        options.set(DLF_REGION, "cn-hangzhou");
        options.set(DATA_TOKEN_ENABLED, false);
        Configuration configuration = new Configuration();
        CatalogContext catalogContext = CatalogContext.create(options, configuration);
        this.catalog = new RESTCatalog(catalogContext, true);
    }

    @Test
    @Override
    public void testGetTable() throws Exception {
        catalog.createDatabase("test_db", false);

        // Get system and data table when the table exists
        Identifier identifier = Identifier.create("test_db", "test_table1");
        catalog.createTable(identifier, DEFAULT_TABLE_SCHEMA, false);
        Identifier identifier2 = Identifier.create("test_db", "test_table2");
        catalog.createTable(identifier2, DEFAULT_TABLE_SCHEMA, false);

        catalog.getTable(identifier);
        catalog.getTable(identifier2);
    }


    private void createTable(
        Identifier identifier, Map<String, String> options, List<String> partitionKeys)
        throws Exception {
        catalog.createDatabase(identifier.getDatabaseName(), true);
        catalog.createTable(
            identifier,
            new Schema(
                Lists.newArrayList(new DataField(0, "col1", DataTypes.INT())),
                partitionKeys,
                Collections.emptyList(),
                options,
                ""),
            true);
    }
}
