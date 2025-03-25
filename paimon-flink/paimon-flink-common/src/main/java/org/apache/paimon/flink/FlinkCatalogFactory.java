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

package org.apache.paimon.flink;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.Options;
import org.apache.paimon.rest.RESTCatalogFactory;
import org.apache.paimon.rest.RESTCatalogOptions;
import org.apache.paimon.rest.auth.AuthProviderEnum;
import org.apache.paimon.utils.StringUtils;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;

import java.util.Base64;
import java.util.Collections;
import java.util.Set;

import static org.apache.paimon.flink.FlinkCatalogOptions.DEFAULT_DATABASE;

/** Factory for {@link FlinkCatalog}. */
public class FlinkCatalogFactory implements org.apache.flink.table.factories.CatalogFactory {

    public static final String IDENTIFIER = "qingwei-debug";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<org.apache.flink.configuration.ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<org.apache.flink.configuration.ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }

    public static final ConfigOption<String> DLF_META_CREDENTIAL_PROVIDER_PATH =
            ConfigOptions.key("dlf.meta.credential.provider.path")
                    .stringType()
                    .defaultValue("/secret/DLF/meta/")
                    .withDescription(
                            "The local path that will be used as a secret url for \"dlf.tokenCache.meta.credential.provider.url\" config for DLF.");

    private static final org.apache.flink.configuration.ConfigOption<String> DLF_REST_TOKEN_PATH =
            org.apache.flink.configuration.ConfigOptions.key(
                            RESTCatalogOptions.DLF_TOKEN_PATH.key())
                    .stringType()
                    .noDefaultValue();

    @Override
    public FlinkCatalog createCatalog(Context context) {
        forwardDlfOptions(context.getConfiguration(), Options.fromMap(context.getOptions()));
        return createCatalog(
                context.getName(),
                CatalogContext.create(
                        Options.fromMap(context.getOptions()), new FlinkFileIOLoader()),
                context.getClassLoader());
    }

    public static FlinkCatalog createCatalog(
            String catalogName, CatalogContext context, ClassLoader classLoader) {
        return new FlinkCatalog(
                CatalogFactory.createCatalog(context, classLoader),
                catalogName,
                context.options().get(DEFAULT_DATABASE),
                classLoader,
                context.options());
    }

    public static FlinkCatalog createCatalog(String catalogName, Catalog catalog, Options options) {

        return new FlinkCatalog(
                catalog,
                catalogName,
                Catalog.DEFAULT_DATABASE,
                FlinkCatalogFactory.class.getClassLoader(),
                options);
    }

    public static Catalog createPaimonCatalog(Options catalogOptions) {
        return CatalogFactory.createCatalog(
                CatalogContext.create(catalogOptions, new FlinkFileIOLoader()));
    }

    public static void forwardDlfOptions(ReadableConfig flinkConfig, Options catalogOptions) {
        //        LOG.info("Adding option for dlf-paimon catalog");
        //        LOG.info("forwardDlfOptions " + flinkConfig);
        if (RESTCatalogFactory.IDENTIFIER.equals(catalogOptions.get(CatalogOptions.METASTORE.key()))
                && catalogOptions
                        .getOptional(RESTCatalogOptions.TOKEN_PROVIDER)
                        .filter(value -> AuthProviderEnum.DLF.identifier().equals(value))
                        .isPresent()) {
            // get the credential provider configuration from flink config.
            String tokenPath = flinkConfig.get(DLF_REST_TOKEN_PATH);
            String userName =
                    flinkConfig.get(
                            ConfigOptions.key("pipeline.dlf.role-session-name")
                                    .stringType()
                                    .noDefaultValue()
                                    .withDescription(
                                            "The role session name of the user who submitted the jobs."));
            if (StringUtils.isNullOrWhitespaceOnly(userName)
                    && StringUtils.isNullOrWhitespaceOnly(tokenPath)) {
                throw new IllegalArgumentException(
                        "role-session-name or dlf.token-path must not be null or empty.");
            }
            if (StringUtils.isNullOrWhitespaceOnly(tokenPath)) {
                String tokenDir = flinkConfig.get(DLF_META_CREDENTIAL_PROVIDER_PATH);
                if (tokenDir.endsWith("/")) {
                    tokenPath = tokenDir + convertRoleSessionName2TokenFileName(userName);
                } else {
                    tokenPath = tokenDir + "/" + convertRoleSessionName2TokenFileName(userName);
                }
            }
            //            LOG.info("Use rest catalog and tokenPath is [{}]", tokenPath);
            catalogOptions.set(RESTCatalogOptions.DLF_TOKEN_PATH.key(), tokenPath);
        }
    }

    public static String convertRoleSessionName2TokenFileName(String s) {
        if (s == null) {
            return null;
        }
        Base64.Encoder encoder = Base64.getUrlEncoder().withoutPadding();
        try {
            String encodeResult = encoder.encodeToString(s.getBytes());
            return encodeResult.replace("\n", "");
        } catch (Exception e) {
            throw new RuntimeException("Error encoding base64 string ", e);
        }
    }
}
