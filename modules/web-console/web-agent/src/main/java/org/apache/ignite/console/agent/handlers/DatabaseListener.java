/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.console.agent.handlers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import io.vertx.core.eventbus.Message;
import org.apache.ignite.console.agent.AgentConfiguration;
import org.apache.ignite.console.agent.db.DbMetadataReader;
import org.apache.ignite.console.agent.db.DbSchema;
import org.apache.ignite.console.agent.db.DbTable;
import org.apache.ignite.console.demo.AgentMetadataDemo;
import org.apache.log4j.Logger;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.console.agent.AgentUtils.resolvePath;

/**
 * API to extract database metadata.
 */
public class DatabaseListener extends AbstractHandler {
    /** */
    private static final String EVENT_SCHEMA_IMPORT_DRIVERS = "schemaImport:drivers";

    /** */
    private static final String EVENT_SCHEMA_IMPORT_SCHEMAS = "schemaImport:schemas";

    /** */
    private static final String EVENT_SCHEMA_IMPORT_METADATA = "schemaImport:metadata";


    /** */
    private static final Logger log = Logger.getLogger(DatabaseListener.class.getName());

    /** */
    private final File driversFolder;

    /** */
    private final DbMetadataReader dbMetaReader;

    /**
     * @param cfg Config.
     */
    public DatabaseListener(AgentConfiguration cfg) {
        driversFolder = resolvePath(cfg.driversFolder() == null ? "jdbc-drivers" : cfg.driversFolder());
        dbMetaReader = new DbMetadataReader();
    }

    /** {@inheritDoc} */
    @Override public void start() {
        vertx.eventBus().consumer(EVENT_SCHEMA_IMPORT_DRIVERS, this::driversHandler);
        vertx.eventBus().consumer(EVENT_SCHEMA_IMPORT_SCHEMAS, this::schemasHandler);
        vertx.eventBus().consumer(EVENT_SCHEMA_IMPORT_METADATA, this::metadataHandler);
    }

    /**
     * @param msg Message.
     */
    private void driversHandler(Message<Void> msg) {
        if (driversFolder == null) {
            log.info("JDBC drivers folder not specified, returning empty list");

            msg.reply(Collections.emptyList());

            return;
        }

        if (log.isDebugEnabled())
            log.debug("Collecting JDBC drivers in folder: " + driversFolder.getPath());

        File[] list = driversFolder.listFiles(new FilenameFilter() {
            @Override public boolean accept(File dir, String name) {
                return name.endsWith(".jar");
            }
        });

        if (list == null) {
            log.info("JDBC drivers folder has no files, returning empty list");

            msg.reply(Collections.emptyList());
        }

        List<JdbcDriver> drivers = new ArrayList<>();

        for (File file : list) {
            try {
                boolean win = System.getProperty("os.name").contains("win");

                URL url = new URL("jar", null,
                    "file:" + (win ? "/" : "") + file.getPath() + "!/META-INF/services/java.sql.Driver");

                try (BufferedReader reader = new BufferedReader(new InputStreamReader(url.openStream(), UTF_8))) {
                    String jdbcDriverCls = reader.readLine();

                    drivers.add(new JdbcDriver(file.getName(), jdbcDriverCls));

                    if (log.isDebugEnabled())
                        log.debug("Found: [driver=" + file + ", class=" + jdbcDriverCls + "]");
                }
            }
            catch (IOException e) {
                drivers.add(new JdbcDriver(file.getName(), null));

                log.info("Found: [driver=" + file + "]");
                log.info("Failed to detect driver class: " + e.getMessage());
            }
        }

        msg.reply(drivers);
    }

    /**
     * @param msg Message.
     */
    private void schemasHandler(Message<Map<String, Object>> msg) {
        try {
            String jdbcDriverJarPath = null;

            Map<String, Object> args = msg.body();

            if (args.containsKey("jdbcDriverJar"))
                jdbcDriverJarPath = args.get("jdbcDriverJar").toString();

            if (!args.containsKey("jdbcDriverClass"))
                throw new IllegalArgumentException("Missing driverClass in arguments: " + args);

            String jdbcDriverCls = args.get("jdbcDriverClass").toString();

            if (!args.containsKey("jdbcUrl"))
                throw new IllegalArgumentException("Missing url in arguments: " + args);

            String jdbcUrl = args.get("jdbcUrl").toString();

            if (!args.containsKey("info"))
                throw new IllegalArgumentException("Missing info in arguments: " + args);

            Properties jdbcInfo = new Properties();

            jdbcInfo.putAll((Map)args.get("info"));

            if (log.isDebugEnabled())
                log.debug("Start collecting database schemas [drvJar=" + jdbcDriverJarPath +
                    ", drvCls=" + jdbcDriverCls + ", jdbcUrl=" + jdbcUrl + "]");

            try (Connection conn = connect(jdbcDriverJarPath, jdbcDriverCls, jdbcUrl, jdbcInfo)) {
                String catalog = conn.getCatalog();

                if (catalog == null) {
                    String[] parts = jdbcUrl.split("[/:=]");

                    catalog = parts.length > 0 ? parts[parts.length - 1] : "NONE";
                }

                Collection<String> schemas = dbMetaReader.schemas(conn);

                if (log.isDebugEnabled())
                    log.debug("Finished collection of schemas [jdbcUrl=" + jdbcUrl + ", catalog=" + catalog +
                        ", count=" + schemas.size() + "]");

                msg.reply(new DbSchema(catalog, schemas));
            }
        }
        catch (Throwable e) {
            msg.fail(FAILED, "Failed to collect DB schemas");
        }
    }

    /**
     * @param msg Message.
     */
    private void metadataHandler(Message<Map<String, Object>> msg) {
        try {
            String jdbcDriverJarPath = null;

            Map<String, Object> args = msg.body();

            if (args.containsKey("jdbcDriverJar"))
                jdbcDriverJarPath = args.get("jdbcDriverJar").toString();

            if (!args.containsKey("jdbcDriverClass"))
                throw new IllegalArgumentException("Missing driverClass in arguments: " + args);

            String jdbcDriverCls = args.get("jdbcDriverClass").toString();

            if (!args.containsKey("jdbcUrl"))
                throw new IllegalArgumentException("Missing url in arguments: " + args);

            String jdbcUrl = args.get("jdbcUrl").toString();

            if (!args.containsKey("info"))
                throw new IllegalArgumentException("Missing info in arguments: " + args);

            Properties jdbcInfo = new Properties();

            jdbcInfo.putAll((Map)args.get("info"));

            if (!args.containsKey("schemas"))
                throw new IllegalArgumentException("Missing schemas in arguments: " + args);

            List<String> schemas = (List<String>)args.get("schemas");

            if (!args.containsKey("tablesOnly"))
                throw new IllegalArgumentException("Missing tablesOnly in arguments: " + args);

            boolean tblsOnly = (boolean)args.get("tablesOnly");

            if (log.isDebugEnabled())
                log.debug("Start collecting database metadata [drvJar=" + jdbcDriverJarPath +
                    ", drvCls=" + jdbcDriverCls + ", jdbcUrl=" + jdbcUrl + "]");

            try (Connection conn = connect(jdbcDriverJarPath, jdbcDriverCls, jdbcUrl, jdbcInfo)) {
                Collection<DbTable> metadata = dbMetaReader.metadata(conn, schemas, tblsOnly);

                if (log.isDebugEnabled())
                    log.debug("Finished collection of metadata [jdbcUrl=" + jdbcUrl + ", count=" + metadata.size() + "]");

                msg.reply(metadata);
            }
        }
        catch (Throwable e) {
            msg.fail(FAILED, "Failed to collect DB metadata");
        }
    }

    /**
     * @param jdbcDriverJarPath JDBC driver JAR path.
     * @param jdbcDriverCls JDBC driver class.
     * @param jdbcUrl JDBC URL.
     * @param jdbcInfo Properties to connect to database.
     * @return Connection to database.
     * @throws SQLException If failed to connect.
     */
    private Connection connect(String jdbcDriverJarPath, String jdbcDriverCls, String jdbcUrl,
        Properties jdbcInfo) throws SQLException {
        if (AgentMetadataDemo.isTestDriveUrl(jdbcUrl))
            return AgentMetadataDemo.testDrive();

        if (!new File(jdbcDriverJarPath).isAbsolute() && driversFolder != null)
            jdbcDriverJarPath = new File(driversFolder, jdbcDriverJarPath).getPath();

        return dbMetaReader.connect(jdbcDriverJarPath, jdbcDriverCls, jdbcUrl, jdbcInfo);
    }

    /**
     * Wrapper class for later to be transformed to JSON and send to Web Console.
     */
    private static class JdbcDriver {
        /** */
        public final String jdbcDriverJar;
        /** */
        public final String jdbcDriverCls;

        /**
         * @param jdbcDriverJar File name of driver jar file.
         * @param jdbcDriverCls Optional JDBC driver class.
         */
        JdbcDriver(String jdbcDriverJar, String jdbcDriverCls) {
            this.jdbcDriverJar = jdbcDriverJar;
            this.jdbcDriverCls = jdbcDriverCls;
        }
    }
}
