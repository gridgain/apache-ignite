/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.compatibility.sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.compatibility.sql.model.City;
import org.apache.ignite.compatibility.sql.model.Company;
import org.apache.ignite.compatibility.sql.model.Country;
import org.apache.ignite.compatibility.sql.model.Department;
import org.apache.ignite.compatibility.sql.model.ModelFactory;
import org.apache.ignite.compatibility.sql.model.Person;
import org.apache.ignite.compatibility.testframework.junits.Dependency;
import org.apache.ignite.compatibility.testframework.junits.IgniteCompatibilityAbstractTest;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.ClientConnectorConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.jdbc.thin.JdbcThinConnection;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.junits.multijvm.IgniteProcessProxy;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

/**
 * Test for SQL queries regressions detection.
 * It happens in the next way:
 * 
 * 1. Test starts two different Ignite versions: current version and the old one.
 * 2. Then framework executes (randomly chosen/generated) equivalent queries in both versions.
 * 3. Execution time for both version is measured and if it exceeds some threshold, the query marked as suspected.
 * 4. All suspected queries are submitted to both Ignite versions one more time to get rid of outliers.
 * 5. If a poor execution time is reproducible for suspected query,
 *    this query is reported as a problematic and test fails because of it.
 */
@SuppressWarnings("TypeMayBeWeakened")
public class SqlQueryRegressionsTest extends IgniteCompatibilityAbstractTest {
    /** Ignite version. */
    private static final String IGNITE_VERSION = "2.5.0";

    /** */
    private static final int OLD_JDBC_PORT = 10800;

    /** */
    private static final int NEW_JDBC_PORT = 10802;

    /** Query workers count. */
    private static final int WORKERS_CNT = 4;

    /** */
    private static final long TEST_TIMEOUT = 10_000;

    /** */
    private static final long WARM_UP_TIMEOUT = 5_000;

    /** */
    private static final String JDBC_URL = "jdbc:ignite:thin://127.0.0.1:";

    /** */
    public static final TcpDiscoveryIpFinder OLD_VER_FINDER = new TcpDiscoveryVmIpFinder(true) {{
        setAddresses(Collections.singleton("127.0.0.1:47500..47509"));
    }};

    /**  */
    public static final TcpDiscoveryVmIpFinder NEW_VER_FINDER = new TcpDiscoveryVmIpFinder(true) {{
        setAddresses(Collections.singleton("127.0.0.1:47510..47519"));
    }};


    /** {@inheritDoc} */
    @Override protected @NotNull Collection<Dependency> getDependencies(String igniteVer) {
        Collection<Dependency> dependencies = super.getDependencies(igniteVer);

        dependencies.add(new Dependency("indexing", "ignite-indexing", false));

        // TODO add and exclude proper versions of h2
        dependencies.add(new Dependency("h2", "com.h2database", "h2", "1.4.195", false));

        return dependencies;
    }

    /** {@inheritDoc} */
    @Override protected Set<String> getExcluded(String ver, Collection<Dependency> dependencies) {
        Set<String> excluded = super.getExcluded(ver, dependencies);

        excluded.add("h2");

        return excluded;
    }

    /** {@inheritDoc} */
    @Override protected long getTestTimeout() {
        return  2 * TEST_TIMEOUT + WARM_UP_TIMEOUT + super.getTestTimeout();
    }

    /**
     * Test for SQL performance regression detection.
     *
     * @throws Exception If failed.
     */
    @Test
    public void testSqlPerformanceRegressions() throws Exception {
        try {
            int seed = ThreadLocalRandom.current().nextInt();

            if (log.isInfoEnabled())
                log.info("Chosen random seed=" + seed);

            startOldAndNewClusters(seed);

            createTablesAndPopulateData(grid(0), seed);

            Supplier<String> qrysSupplier = new PredefinedQueriesSupplier(Arrays.asList(
                //"SELECT * FROM person p1, person p2",
                "SELECT * FROM person p1 WHERE id > 0",
                "SELECT * FROM department d1 WHERE id > 0",
                "SELECT * FROM country c1",
                "SELECT * FROM city ci1",
                "SELECT * FROM company co1"
            ));

            try (SimpleConnectionPool oldConnPool = new SimpleConnectionPool(JDBC_URL, OLD_JDBC_PORT, WORKERS_CNT);
                 SimpleConnectionPool newConnPool = new SimpleConnectionPool(JDBC_URL, NEW_JDBC_PORT, WORKERS_CNT)) {
                // 0. Warm-up.
                runBenchmark(WARM_UP_TIMEOUT, oldConnPool, newConnPool, qrysSupplier, 0, 1);

                // 1. Initial run.
                Collection<QueryDuelResult> suspiciousQrys =
                    runBenchmark(WARM_UP_TIMEOUT, oldConnPool, newConnPool, qrysSupplier, 1, 1);

                if (suspiciousQrys.isEmpty())
                    return; // No suspicious queries - no problem.

                Set<String> suspiciousQrysSet = suspiciousQrys.stream()
                    .map(QueryDuelResult::query)
                    .collect(Collectors.toSet());

                if (log.isInfoEnabled())
                    log.info("Problematic queries number: " + suspiciousQrysSet.size());

                Supplier<String> problematicQrysSupplier = new PredefinedQueriesSupplier(suspiciousQrysSet);

                // 2. Rerun problematic queries to ensure they are not outliers.
                Collection<QueryDuelResult> failedQueries =
                    runBenchmark(WARM_UP_TIMEOUT, oldConnPool, newConnPool, problematicQrysSupplier, 3, 5);

                assertTrue("Found SQL performance regression for queries: " + formatPretty(failedQueries),
                    failedQueries.isEmpty());
            }
        }
        finally {
            stopClusters();
        }
    }

    /**
     * Starts task that compare query execution time in the new and old Ignite versions.
     *
     * @param timeout Test duration.
     * @param oldConnPool Pool of JDBC connections to the old Ignite version.
     * @param newConnPool Pool of JDBC connections to the new Ignite version.
     * @param qrySupplier Sql queries generator.
     * @return Suspicious queries collection.
     * @throws InterruptedException If interrupted.
     */
    public Collection<QueryDuelResult> runBenchmark(
        final long timeout,
        SimpleConnectionPool oldConnPool,
        SimpleConnectionPool newConnPool,
        Supplier<String> qrySupplier,
        int successCnt,
        int attemptsCnt
    ) throws InterruptedException {
        Collection<QueryDuelResult> suspiciousQrys = Collections.newSetFromMap(new ConcurrentHashMap<>());

        final long end = System.currentTimeMillis() + timeout;

        BlockingExecutor exec = new BlockingExecutor(WORKERS_CNT);

        while (System.currentTimeMillis() < end) {
            QueryDuelRunner runner =
                new QueryDuelRunner(oldConnPool, newConnPool, qrySupplier , suspiciousQrys, successCnt, attemptsCnt);

            exec.execute(runner);
        }

        exec.stopAndWaitForTermination();

        return suspiciousQrys;
    }

    /**
     * Starts old and new Ignite clusters.
     * @param seed Random seed.
     */
    public void startOldAndNewClusters(int seed) throws Exception {
        // Old cluster.
        startGrid(2, IGNITE_VERSION, new NodeConfigurationClosure(), new PostStartupClosure(true, seed));
        startGrid(3, IGNITE_VERSION, new NodeConfigurationClosure(), new PostStartupClosure(false, seed));

        // New cluster
        IgnitionEx.start(prepareNodeConfig(
            getConfiguration(getTestIgniteInstanceName(0)), NEW_VER_FINDER, NEW_JDBC_PORT));
        IgnitionEx.start(prepareNodeConfig(
            getConfiguration(getTestIgniteInstanceName(1)), NEW_VER_FINDER, NEW_JDBC_PORT));
    }

    /**
     * Stops both new and old clusters.
     */
    public void stopClusters() {
        // Old cluster.
        IgniteProcessProxy.killAll();

        // New cluster.
        for (Ignite ignite : G.allGrids())
            U.close(ignite, log);
    }

    /**
     * @param qrys Queries duels result.
     * @return Pretty formatted result of duels.
     */
    private static String formatPretty(Collection<QueryDuelResult> qrys) {
        StringBuilder sb = new StringBuilder().append("\n");

        for (QueryDuelResult res : qrys) {
            sb.append(res)
                .append('\n');
        }

        return sb.toString();
    }

    /**
     * @param ignite Ignite node.
     * @param seed Random seed.
     */
    private static void createTablesAndPopulateData(Ignite ignite, int seed) {
        createAndPopulateTable(ignite, new Person.Factory(seed));
        createAndPopulateTable(ignite, new Department.Factory(seed));
        createAndPopulateTable(ignite, new Country.Factory(seed));
        createAndPopulateTable(ignite, new City.Factory(seed));
        createAndPopulateTable(ignite, new Company.Factory(seed));
    }

    /** */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static void createAndPopulateTable(Ignite ignite, ModelFactory factory) {
        QueryEntity qryEntity = factory.queryEntity();
        CacheConfiguration cacheCfg = new CacheConfiguration<>(factory.tableName())
            .setQueryEntities(Collections.singleton(qryEntity))
            .setSqlSchema("PUBLIC");

        IgniteCache personCache = ignite.createCache(cacheCfg);

        for (long i = 0; i < factory.count(); i++)
            personCache.put(i, factory.createRandom());
    }

    /**
     * Prepares ignite nodes configuration.
     */
    private static IgniteConfiguration prepareNodeConfig(IgniteConfiguration cfg, TcpDiscoveryIpFinder ipFinder,
        int jdbcPort) {
        cfg.setLocalHost("127.0.0.1");
        cfg.setPeerClassLoadingEnabled(false);

        TcpDiscoverySpi disco = new TcpDiscoverySpi();
        disco.setIpFinder(ipFinder);
        cfg.setDiscoverySpi(disco);

        ClientConnectorConfiguration clientCfg = new ClientConnectorConfiguration();
        clientCfg.setPort(jdbcPort);
        return cfg;
    }

    /**
     * Configuration closure.
     */
    private static class NodeConfigurationClosure implements IgniteInClosure<IgniteConfiguration> {
        /** {@inheritDoc} */
        @Override public void apply(IgniteConfiguration cfg) {
            prepareNodeConfig(cfg, OLD_VER_FINDER, OLD_JDBC_PORT);
        }
    }

    /**
     * Closure that executed for old Ingite version after start up.
     */
    private static class PostStartupClosure implements IgniteInClosure<Ignite> {
        /** */
        private final boolean createTbl;

        /** Random seed. */
        private final int seed;

        /**
         * @param createTbl {@code true} In case table should be created
         * @param seed
         */
        PostStartupClosure(boolean createTbl, int seed) {
            this.createTbl = createTbl;
            this.seed = seed;
        }

        /** {@inheritDoc} */
        @Override public void apply(Ignite ignite) {
            if (createTbl) {
                createTablesAndPopulateData(ignite, seed);
            }
        }
    }

    /**
     * Class that runs queries in different Ignite version and compares their execution times.
     */
    private static class QueryDuelRunner implements Runnable {
        /** JDBC connection pool of the old Ignite version. */
        private final SimpleConnectionPool oldConnPool;

        /** JDBC connection pool of the new Ignite version. */
        private final SimpleConnectionPool newConnPool;

        /** Query producer. */
        private final Supplier<String> qrySupplier;

        /** Collection of suspicious queries. */
        private final Collection<QueryDuelResult> suspiciousQrs;

        /** Number of success runs where oldExecTime <= newExecTime to pass the duel duel successfully. */
        private int successCnt;

        /**
         * Number of duel attempts. Duel is successful when it
         * made {@link #successCnt} successful runs with {@link #attemptsCnt} attempts.
         */
        private int attemptsCnt;

        /** */
        private QueryDuelRunner(
            SimpleConnectionPool oldConnPool,
            SimpleConnectionPool newConnPool,
            Supplier<String> qrySupplier,
            Collection<QueryDuelResult> suspiciousQrs,
            int successCnt,
            int attemptsCnt
        ) {
            assert successCnt <= attemptsCnt : "successCnt=" + successCnt + ", attemptsCnt=" + attemptsCnt;

            this.oldConnPool = oldConnPool;
            this.newConnPool = newConnPool;
            this.qrySupplier = qrySupplier;
            this.suspiciousQrs = suspiciousQrs;
            this.successCnt = successCnt;
            this.attemptsCnt = attemptsCnt;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            String qry = qrySupplier.get();
            List<Long> oldQryExecTimes = new ArrayList<>(attemptsCnt);
            List<Long> newQryExecTimes = new ArrayList<>(attemptsCnt);
            List<Exception> exceptions = new ArrayList<>(attemptsCnt);

            while (attemptsCnt-- > 0) {
                try {
                    QueryExecutionTimer oldVerRun = new QueryExecutionTimer(qry, oldConnPool);
                    QueryExecutionTimer newVerRun = new QueryExecutionTimer(qry, newConnPool);

                    CompletableFuture<Long> oldVerFut = CompletableFuture.supplyAsync(oldVerRun);
                    CompletableFuture<Long> newVerFut = CompletableFuture.supplyAsync(newVerRun);

                    CompletableFuture.allOf(oldVerFut, newVerFut).get();

                    Long oldRes = oldVerFut.get();
                    Long newRes = newVerFut.get();

                    oldQryExecTimes.add(oldRes);
                    newQryExecTimes.add(newRes);

                    if (log.isDebugEnabled()) {
                        log.debug("Query running time: newVer" + newRes + ", oldVer=" + oldRes +
                            ", diff=" + (newRes - oldRes));
                    }

                    if (isSuccessfulRun(oldRes, newRes))
                        successCnt--;

                    if (successCnt == 0)
                        break;
                }
                catch (Exception e) {
                    e.printStackTrace();

                    exceptions.add(e);

                    break;
                }
            }

            if (successCnt > 0)
                suspiciousQrs.add(new QueryDuelResult(qry, oldQryExecTimes, newQryExecTimes, exceptions));
        }

        /**
         * @param oldRes Query execution time in the old engine.
         * @param newRes Query execution time int current engine.
         * @return {@code True} if a query execution time in the new engine is not much longer than in the old one.
         */
        public boolean isSuccessfulRun(Long oldRes, Long newRes) {
            // TODO move magic numbers to constants.
            final double epsilon = 10.0; // Let's say 10 ms is about statistical error.

            if (oldRes < newRes && (oldRes > epsilon || newRes > epsilon)) {
                double newD = Math.max(newRes, epsilon);
                double oldD = Math.max(oldRes, epsilon);

                if (newD / oldD > 2)
                    return false;
            }

            return true;
        }
    }

    /**
     * The result of query duel.
     */
    private static class QueryDuelResult {
        /** */
        private final String qry;

        /** */
        private final List<Long> oldQryExecTimes;

        /** */
        private final List<Long> newQryExecTimes;

        /** */
        private final List<Exception> errors;

        /** */
        QueryDuelResult(String qry, List<Long> oldQryExecTimes, List<Long> newQryExecTimes, List<Exception> errors) {
            this.qry = qry;
            this.oldQryExecTimes = oldQryExecTimes;
            this.newQryExecTimes = newQryExecTimes;
            this.errors = errors;
        }

        /** */
        public String query() {
            return qry;
        }

        /** */
        public List<Long> oldExecutionTime() {
            return oldQryExecTimes;
        }

        /** */
        public List<Long> newExecutionTime() {
            return newQryExecTimes;
        }

        /** */
        public List<Exception> error() {
            return errors;
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            QueryDuelResult res = (QueryDuelResult)o;
            return Objects.equals(qry, res.qry);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            return Objects.hash(qry);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "QueryDuelResult{" +
                "qry='" + qry + '\'' +
                ", oldQryExecTimes=" + oldQryExecTimes +
                ", newQryExecTimes=" + newQryExecTimes +
                ", err=" + errors +
                '}';
        }
    }

    /**
     * Query runner. Runs query and checks it's execution time.
     */
    private static class QueryExecutionTimer implements Supplier<Long> {
        /** */
        private final String qry;

        /** */
        private final SimpleConnectionPool connPool; // TODO check query result.

        /** */
        private QueryExecutionTimer(String qry, SimpleConnectionPool connPool) {
            this.qry = qry;
            this.connPool = connPool;
        }

        /** {@inheritDoc} */
        @Override public Long get() {
            long start = System.currentTimeMillis();

            Connection conn = connPool.getConnection();

            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery(qry)) {
                    int cnt = 0;
                    while (rs.next()) { // TODO check for empty result
                        cnt++;
                    }

                    ResultSetMetaData md = rs.getMetaData();
                    StringBuilder sb = new StringBuilder();
                    for (int i = 1; i <= md.getColumnCount(); i++) {
                        if (i > 0)
                            sb.append(", ");

                        sb.append(md.getColumnName(i));
                    }
                    System.out.println("Rs size=" + cnt + ", tblName=" + md.getTableName(1) + ", fields=" + sb +
                        ", conn=" + ((JdbcThinConnection)conn).url());
                }
            }
            catch (SQLException e) {
                throw new RuntimeException(e);
            }
            finally {
                connPool.releaseConnection(conn);
            }

            return System.currentTimeMillis() - start;
        }
    }

    /**
     * Simple JDBC connection pool for testing.
     */
    private static class SimpleConnectionPool implements AutoCloseable {
        /** */
        private final List<Connection> connPool;

        /** */
        private final List<Connection> usedConnections;

        /** */
        private SimpleConnectionPool(String url, int port, int size) throws SQLException {
            connPool = new ArrayList<>(size);
            usedConnections = new ArrayList<>(size); // "jdbc:ignite:thin://127.0.0.1:"

            for (int i = 0; i < size; i++) {
                Connection conn = DriverManager.getConnection(url + port);

                conn.setSchema("PUBLIC");

                connPool.add(conn);
            }
        }

        /** */
        public synchronized Connection getConnection() {
            Connection conn = connPool.remove(connPool.size() - 1);

            usedConnections.add(conn);

            return conn;
        }

        /** */
        public synchronized boolean releaseConnection(Connection conn) {
            connPool.add(conn);

            return usedConnections.remove(conn);
        }

        /** {@inheritDoc} */
        @Override public synchronized void close() {
            connPool.forEach(U::closeQuiet);
            usedConnections.forEach(U::closeQuiet);
        }
    }

    /**
     * Executor that restricts the number of submitted tasks.
     */
    private static class BlockingExecutor {
        /** */
        private final Semaphore semaphore;

        /** */
        private final ExecutorService executor;

        /** */
        BlockingExecutor(final int taskLimit) {
            semaphore = new Semaphore(taskLimit);
            executor = new ThreadPoolExecutor(
                taskLimit,
                taskLimit,
                10,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(taskLimit));
        }

        /** */
        public void execute(final Runnable r) {
            try {
                semaphore.acquire();
            }
            catch (InterruptedException e) {
                e.printStackTrace();
                return;
            }

            final Runnable semaphoredRunnable = () -> {
                try {
                    r.run();
                }
                finally {
                    semaphore.release();
                }
            };

            executor.execute(semaphoredRunnable);
        }

        /** */
        public void stopAndWaitForTermination() throws InterruptedException {
            executor.shutdown();
            executor.awaitTermination(TEST_TIMEOUT, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Simple factory of SQL queries. Just returns preconfigured queries one by one.
     */
    private static class PredefinedQueriesSupplier implements Supplier<String> {
        /** */
        private final Collection<String> qrys;

        /** */
        private Iterator<String> it;

        /** */
        private PredefinedQueriesSupplier(Collection<String> qrys) {
            assert !qrys.isEmpty();
            this.qrys = qrys;
            it = qrys.iterator();
        }

        /** {@inheritDoc} */
        @Override public synchronized String get() {
            if (!it.hasNext())
                it = qrys.iterator();

            return it.next();
        }
    }
}
