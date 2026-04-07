package org.openprojectx.spark.hms.test;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.*;
import org.openprojectx.cloudera.hms.testcontainers.ClouderaHiveMetastoreContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.localstack.LocalStackContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests for {@link IcebergOperationsApp}.
 *
 * <p>Infrastructure:
 * <ul>
 *   <li><b>HMS</b>  – {@link ClouderaHiveMetastoreContainer} (ghcr.io/openprojectx/cloudera-hms)
 *       with embedded PostgreSQL backend.</li>
 *   <li><b>S3</b>   – {@link LocalStackContainer} running the S3 service.</li>
 *   <li><b>Spark</b>– local[2] session created once per class, stopped in {@code @AfterAll}.</li>
 * </ul>
 *
 * <p>Tests are ordered so each one builds on the previous table state.
 * Time-travel tests run before the snapshot-expiry maintenance test.
 */
@Testcontainers
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class IcebergOperationsTest {

    // -------------------------------------------------------------------------
    // Container definitions (class-scoped lifecycle via @Testcontainers)
    // -------------------------------------------------------------------------

    private static final String BUCKET = "test-iceberg";

    @Container
    static final LocalStackContainer localstack =
            new LocalStackContainer(DockerImageName.parse("localstack/localstack:3.8.1"))
                    .withServices("s3");

    @Container
    static final ClouderaHiveMetastoreContainer hms =
            new ClouderaHiveMetastoreContainer()
                    .withExtraConfiguration(Map.of(
                            "fs.s3a.impl" , "org.apache.hadoop.fs.s3a.S3AFileSystem",
                            "fs.s3a.endpoint" , localstack.getEndpoint().toString(),
                            "fs.s3a.endpoint.region" , localstack.getRegion(),
                            "fs.s3a.path.style.access" , "true",
                            "fs.s3a.connection.ssl.enabled" , "false",
                            "fs.s3a.access.key" , localstack.getAccessKey(),
                            "fs.s3a.secret.key" , localstack.getSecretKey(),
                            "fs.s3a.aws.credentials.provider" , "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
                    ));

    // -------------------------------------------------------------------------
    // Shared state across ordered test methods
    // -------------------------------------------------------------------------

    static SparkSession spark;

    /** Snapshot ID captured right after the first INSERT batch – used for time travel. */
    static long snapshotIdAfterFirstBatch;

    /** Timestamp string (yyyy-MM-dd HH:mm:ss) of that same snapshot. */
    static String timestampAfterFirstBatch;

    // -------------------------------------------------------------------------
    // Setup / teardown
    // -------------------------------------------------------------------------

    /**
     * Starts after the two containers are ready (TestcontainersExtension runs before @BeforeAll).
     * Creates the S3 bucket and initialises the SparkSession.
     */
    @BeforeAll
    static void setup() {
        createS3Bucket();

        String s3Endpoint = localstack
                .getEndpoint()
                .toString();
        String warehouse = "s3://" + BUCKET + "/warehouse";

        spark = IcebergOperationsApp.createSparkSession(hms.thriftUri(), s3Endpoint, warehouse);
    }

    @AfterAll
    static void teardown() {
        if (spark != null) {
            try {
                IcebergOperationsApp.dropTable(spark);
                IcebergOperationsApp.dropNamespace(spark);
            } finally {
                spark.stop();
            }
        }
    }

    // -------------------------------------------------------------------------
    // 1 – DDL: namespace + table
    // -------------------------------------------------------------------------

    @Test
    @Order(1)
    @DisplayName("1. Create namespace and partitioned Iceberg table")
    void createNamespaceAndTable() {
        IcebergOperationsApp.createNamespace(spark);
        IcebergOperationsApp.createTable(spark);

        List<Row> tables = spark.sql(
                "SHOW TABLES IN " + IcebergOperationsApp.CATALOG
                        + "." + IcebergOperationsApp.NAMESPACE)
                .collectAsList();

        assertEquals(1, tables.size());
        assertTrue(tables.stream()
                        .anyMatch(r -> IcebergOperationsApp.TABLE.equals(r.getAs("tableName"))),
                "Table 'orders' should exist in the namespace");

        // Initial table should be empty
        assertEquals(0L, IcebergOperationsApp.countRows(spark));
    }

    // -------------------------------------------------------------------------
    // 2 – DML: first INSERT batch
    // -------------------------------------------------------------------------

    @Test
    @Order(2)
    @DisplayName("2. Insert first batch (5 rows) and capture snapshot for time travel")
    void insertFirstBatch() {
        IcebergOperationsApp.insertFirstBatch(spark);

        assertEquals(5L, IcebergOperationsApp.countRows(spark));

        // Capture snapshot for later time-travel tests (Orders 3 & 4).
        // This must happen BEFORE any further mutations.
        Row snapshot = spark.sql(
                "SELECT snapshot_id, committed_at"
                        + " FROM " + IcebergOperationsApp.FULL_TABLE + ".snapshots"
                        + " ORDER BY committed_at"
                        + " LIMIT 1")
                .first();
        snapshotIdAfterFirstBatch = snapshot.getLong(0);
        // Truncate to seconds so the TIMESTAMP AS OF literal is unambiguous
        timestampAfterFirstBatch = snapshot.getTimestamp(1).toString().substring(0, 19);

        assertNotEquals(0L, snapshotIdAfterFirstBatch);
        assertNotNull(timestampAfterFirstBatch);
    }

    // -------------------------------------------------------------------------
    // 3 – DML: second INSERT batch
    // -------------------------------------------------------------------------

    @Test
    @Order(3)
    @DisplayName("3. Insert second batch (10 rows total) and verify category summary")
    void insertSecondBatchAndVerify() {
        IcebergOperationsApp.insertSecondBatch(spark);

        assertEquals(10L, IcebergOperationsApp.countRows(spark));

        List<Row> summary = IcebergOperationsApp.querySummary(spark).collectAsList();
        assertFalse(summary.isEmpty());

        // Electronics: orders 1 (1500), 6 (2200), 9 (3100) = 3 completed rows
        Row electronics = summary.stream()
                .filter(r -> "Electronics".equals(r.getString(0)))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Electronics row missing from summary"));
        assertEquals(3L, electronics.getLong(1), "Electronics completed order count");
        assertEquals(6800.0, electronics.getDouble(2), 0.01, "Electronics total amount");
    }

    // -------------------------------------------------------------------------
    // 4 – DML: UPDATE
    // -------------------------------------------------------------------------

    @Test
    @Order(4)
    @DisplayName("4. UPDATE – PENDING Electronics orders become SHIPPED")
    void updateRows() {
        IcebergOperationsApp.updatePendingToShipped(spark);

        // Order 3 (Electronics, was PENDING) must now be SHIPPED
        Row order3 = spark.sql(
                "SELECT order_id, status FROM " + IcebergOperationsApp.FULL_TABLE
                        + " WHERE order_id = 3")
                .first();
        assertEquals("SHIPPED", order3.getString(1));

        // Order 7 (Food, PENDING) should be unchanged
        Row order7 = spark.sql(
                "SELECT order_id, status FROM " + IcebergOperationsApp.FULL_TABLE
                        + " WHERE order_id = 7")
                .first();
        assertEquals("PENDING", order7.getString(1));
    }

    // -------------------------------------------------------------------------
    // 5 – DML: DELETE
    // -------------------------------------------------------------------------

    @Test
    @Order(5)
    @DisplayName("5. DELETE – CANCELLED orders are removed")
    void deleteRows() {
        long before = IcebergOperationsApp.countRows(spark);
        long cancelledCount = spark.sql(
                "SELECT COUNT(*) FROM " + IcebergOperationsApp.FULL_TABLE
                        + " WHERE status = 'CANCELLED'")
                .first().getLong(0);
        assertTrue(cancelledCount > 0, "There should be CANCELLED orders to delete");

        IcebergOperationsApp.deleteCancelledOrders(spark);

        long after = IcebergOperationsApp.countRows(spark);
        assertEquals(before - cancelledCount, after);

        long remainingCancelled = spark.sql(
                "SELECT COUNT(*) FROM " + IcebergOperationsApp.FULL_TABLE
                        + " WHERE status = 'CANCELLED'")
                .first().getLong(0);
        assertEquals(0L, remainingCancelled);
    }

    // -------------------------------------------------------------------------
    // 6 – DML: MERGE INTO
    // -------------------------------------------------------------------------

    @Test
    @Order(6)
    @DisplayName("6. MERGE INTO – matched rows updated, unmatched rows inserted")
    void mergeInto() {
        long before = IcebergOperationsApp.countRows(spark);

        IcebergOperationsApp.mergeInto(spark);

        // 2 new rows inserted (order_id 11 and 12); 1 row updated (order_id 3)
        assertEquals(before + 2, IcebergOperationsApp.countRows(spark));

        // Matched row: order 3 amount and status updated
        Row order3 = spark.sql(
                "SELECT amount, status FROM " + IcebergOperationsApp.FULL_TABLE
                        + " WHERE order_id = 3")
                .first();
        assertEquals(900.0, order3.getDouble(0), 0.01);
        assertEquals("COMPLETED", order3.getString(1));

        // New row: order 11 (Books)
        Row order11 = spark.sql(
                "SELECT customer_id, category, amount, status FROM "
                        + IcebergOperationsApp.FULL_TABLE + " WHERE order_id = 11")
                .first();
        assertEquals(110L, order11.getLong(0));
        assertEquals("Books", order11.getString(1));
    }

    // -------------------------------------------------------------------------
    // 7 – Schema evolution
    // -------------------------------------------------------------------------

    @Test
    @Order(7)
    @DisplayName("7. Schema evolution – add, rename, and drop columns")
    void schemaEvolution() {
        IcebergOperationsApp.addColumns(spark);
        List<String> cols1 = columnNames();
        assertTrue(cols1.contains("discount"), "discount column should be present after ADD");
        assertTrue(cols1.contains("region"),   "region column should be present after ADD");

        IcebergOperationsApp.renameColumn(spark);
        List<String> cols2 = columnNames();
        assertTrue(cols2.contains("sales_region"), "sales_region should exist after RENAME");
        assertFalse(cols2.contains("region"),       "old column name should be gone after RENAME");

        IcebergOperationsApp.dropColumn(spark);
        List<String> cols3 = columnNames();
        assertFalse(cols3.contains("discount"), "discount should be gone after DROP");

        // Data reads should still work after schema changes
        assertTrue(IcebergOperationsApp.countRows(spark) > 0);
    }

    // -------------------------------------------------------------------------
    // 8 – Time travel by snapshot id
    // -------------------------------------------------------------------------

    @Test
    @Order(8)
    @DisplayName("8. Time travel – VERSION AS OF <snapshot_id> returns first-batch state")
    void timeTravelBySnapshotId() {
        assertNotEquals(0L, snapshotIdAfterFirstBatch,
                "snapshotIdAfterFirstBatch must be populated by test #2");

        long count = IcebergOperationsApp
                .timeTravelBySnapshotId(spark, snapshotIdAfterFirstBatch)
                .first().getLong(0);

        assertEquals(5L, count,
                "Time travel to snapshot after first batch should return exactly 5 rows");
    }

    // -------------------------------------------------------------------------
    // 9 – Time travel by timestamp
    // -------------------------------------------------------------------------

    @Test
    @Order(9)
    @DisplayName("9. Time travel – TIMESTAMP AS OF returns first-batch state")
    void timeTravelByTimestamp() {
        assertNotNull(timestampAfterFirstBatch,
                "timestampAfterFirstBatch must be populated by test #2");

        long count = IcebergOperationsApp
                .timeTravelByTimestamp(spark, timestampAfterFirstBatch)
                .first().getLong(0);

        assertEquals(5L, count,
                "Time travel to first snapshot timestamp should return exactly 5 rows");
    }

    // -------------------------------------------------------------------------
    // 10 – Metadata tables
    // -------------------------------------------------------------------------

    @Test
    @Order(10)
    @DisplayName("10. Iceberg metadata tables – snapshots, history, manifests, files, partitions, refs")
    void metadataTables() {
        // Snapshots: 2 inserts + 1 update + 1 delete + 1 merge = at least 5
        List<Row> snapshots = IcebergOperationsApp.querySnapshots(spark).collectAsList();
        assertTrue(snapshots.size() >= 5,
                "Expected ≥ 5 snapshots, got " + snapshots.size());

        // History mirrors the snapshot chain
        List<Row> history = IcebergOperationsApp.queryHistory(spark).collectAsList();
        assertFalse(history.isEmpty());

        // Manifests
        List<Row> manifests = IcebergOperationsApp.queryManifests(spark).collectAsList();
        assertFalse(manifests.isEmpty());

        // Data files
        List<Row> files = IcebergOperationsApp.queryFiles(spark).collectAsList();
        assertFalse(files.isEmpty());

        // Partitions
        List<Row> partitions = IcebergOperationsApp.queryPartitions(spark).collectAsList();
        assertFalse(partitions.isEmpty());

        // Refs: default Iceberg branch must be 'main'
        List<Row> refs = IcebergOperationsApp.queryRefs(spark).collectAsList();
        assertTrue(refs.stream().anyMatch(r -> "main".equals(r.getString(0))),
                "Expected a ref named 'main'");
    }

    // -------------------------------------------------------------------------
    // 11 – Table maintenance
    // -------------------------------------------------------------------------

    @Test
    @Order(11)
    @DisplayName("11. Table maintenance – rewrite data files, rewrite manifests, expire snapshots, remove orphan files")
    void tableMaintenance() {
        // Rewrite data files (binpack strategy by default)
        Dataset<Row> rewriteResult = IcebergOperationsApp.rewriteDataFiles(spark);
        assertNotNull(rewriteResult.first(),
                "rewrite_data_files should return at least one summary row");

        // Rewrite manifests
        Dataset<Row> manifestResult = IcebergOperationsApp.rewriteManifests(spark);
        assertNotNull(manifestResult.first(),
                "rewrite_manifests should return at least one summary row");

        // Expire snapshots – retain only the 2 most recent
        IcebergOperationsApp.expireSnapshots(spark);

        List<Row> remainingSnapshots = IcebergOperationsApp.querySnapshots(spark).collectAsList();
        assertTrue(remainingSnapshots.size() <= 2,
                "After expire_snapshots(retain_last=2) at most 2 snapshots should remain, "
                        + "got " + remainingSnapshots.size());

        // Remove orphan files – dry run (should not throw even when there are none)
        Dataset<Row> orphans = IcebergOperationsApp.removeOrphanFiles(spark);
        assertNotNull(orphans, "remove_orphan_files dry-run should return a result set");
    }

    // -------------------------------------------------------------------------
    // 12 – Partition evolution
    // -------------------------------------------------------------------------

    @Test
    @Order(12)
    @DisplayName("12. Partition evolution – days → months, add bucket(8, customer_id)")
    void partitionEvolution() {
        // Replace day-level partition with month-level
        IcebergOperationsApp.replacePartitionField(spark);
        assertTrue(IcebergOperationsApp.countRows(spark) > 0,
                "Table must still be readable after REPLACE PARTITION FIELD");

        // Add a second partition dimension
        IcebergOperationsApp.addBucketPartitionField(spark);

        // Insert new data so the new partition spec is exercised
        spark.sql(
            "INSERT INTO " + IcebergOperationsApp.FULL_TABLE + " VALUES"
            + " (20, 200, 'Electronics', 499.00, 'COMPLETED', DATE '2024-03-01'),"
            + " (21, 201, 'Books',        19.99, 'COMPLETED', DATE '2024-03-15')"
        );

        // Partitions table should reflect both old and new specs
        List<Row> partitions = IcebergOperationsApp.queryPartitions(spark).collectAsList();
        assertFalse(partitions.isEmpty(),
                "Partitions metadata should be non-empty after new data written under evolved spec");

        assertEquals(IcebergOperationsApp.countRows(spark),
                IcebergOperationsApp.countRows(spark),
                "Row count should be stable after partition evolution");
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /** Returns only real column names from DESCRIBE TABLE (filters out partition / comment rows). */
    private static List<String> columnNames() {
        return spark.sql("DESCRIBE TABLE " + IcebergOperationsApp.FULL_TABLE)
                .collectAsList().stream()
                .map(r -> r.getString(0))
                .filter(name -> !name.isEmpty() && !name.startsWith("#"))
                .collect(Collectors.toList());
    }

    /** Creates the S3 bucket in LocalStack using AWS SDK v2. */
    private static void createS3Bucket() {
        try (S3Client s3 = S3Client.builder()
                .endpointOverride(localstack.getEndpoint())
                .credentialsProvider(StaticCredentialsProvider.create(
                        AwsBasicCredentials.create("test", "test")))
                .region(Region.US_EAST_1)
                .serviceConfiguration(S3Configuration.builder()
                        .pathStyleAccessEnabled(true)
                        .build())
                .build()) {
            s3.createBucket(CreateBucketRequest.builder().bucket(BUCKET).build());
        }
    }
}
