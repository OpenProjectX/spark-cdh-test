package org.openprojectx.spark.hms.test;

import org.apache.iceberg.Table;
import org.apache.iceberg.UpdateSchema;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Demonstrates comprehensive Apache Iceberg operations using a Hive Metastore (HMS) catalog
 * backed by S3-compatible storage.
 *
 * <p>Operations covered:
 * <ul>
 *   <li>DDL  – namespace/table create, drop</li>
 *   <li>DML  – INSERT, UPDATE, DELETE, MERGE INTO</li>
 *   <li>Schema evolution – ADD/RENAME/DROP COLUMN</li>
 *   <li>Partition evolution – REPLACE PARTITION FIELD, ADD PARTITION FIELD</li>
 *   <li>Time travel – VERSION AS OF (snapshot id), TIMESTAMP AS OF</li>
 *   <li>Metadata tables – snapshots, history, manifests, files, partitions, refs</li>
 *   <li>Table maintenance – expire_snapshots, rewrite_data_files, rewrite_manifests,
 *       remove_orphan_files</li>
 * </ul>
 *
 * <p>CLI usage:
 * <pre>
 *   java -cp ... IcebergOperationsApp &lt;hms-thrift-uri&gt; &lt;s3-endpoint&gt; &lt;warehouse-s3-uri&gt;
 *   # e.g.  thrift://localhost:9083  http://localhost:4566  s3a://iceberg-warehouse/warehouse
 * </pre>
 */
public class IcebergOperationsApp {

    public static final String CATALOG   = "hive_prod";
    public static final String NAMESPACE = "sales";
    public static final String TABLE     = "orders";
    /** Fully-qualified three-part Iceberg table name used in SQL. */
    public static final String FULL_TABLE = CATALOG + "." + NAMESPACE + "." + TABLE;

    // -------------------------------------------------------------------------
    // SparkSession factory
    // -------------------------------------------------------------------------

    /**
     * Builds a local SparkSession wired to an external HMS and an S3-compatible endpoint.
     *
     * <p>Iceberg's own {@code S3FileIO} is used for all data access so that {@code hadoop-aws} /
     * AWS SDK v1 is not required for S3 connectivity.
     *
     * @param hmsUri      Thrift URI of the Hive Metastore, e.g. {@code thrift://localhost:9083}
     * @param s3Endpoint  HTTP endpoint of the S3-compatible service, e.g. {@code http://localhost:4566}
     * @param warehouse   S3 URI used as the Iceberg warehouse root, e.g. {@code s3://bucket/warehouse}
     */
    public static SparkSession createSparkSession(String hmsUri, String s3Endpoint, String warehouse) {
        return SparkSession.builder()
                .appName("IcebergOperations")
                .master("local[2]")
                .config("spark.ui.enabled", "false")
                .config("spark.driver.host", "localhost")
                .config("spark.driver.bindAddress", "127.0.0.1")
                .config("spark.sql.shuffle.partitions", "4")
                // Iceberg SQL extensions (time travel, MERGE, CALL procedures, …)
                .config("spark.sql.extensions",
                        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                // Use an in-memory spark_catalog so Derby is never initialised
                .config("spark.sql.catalogImplementation", "in-memory")
                // Named Iceberg catalog backed by external HMS
                .config("spark.sql.catalog." + CATALOG,
                        "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog." + CATALOG + ".type", "hive")
                .config("spark.sql.catalog." + CATALOG + ".uri", hmsUri)
                .config("spark.sql.catalog." + CATALOG + ".warehouse", warehouse)
                // Iceberg S3FileIO – bypasses hadoop-aws, uses AWS SDK v2 directly
                .config("spark.sql.catalog." + CATALOG + ".io-impl",
                        "org.apache.iceberg.aws.s3.S3FileIO")
                .config("spark.sql.catalog." + CATALOG + ".s3.endpoint", s3Endpoint)
                .config("spark.sql.catalog." + CATALOG + ".s3.access-key-id", "test")
                .config("spark.sql.catalog." + CATALOG + ".s3.secret-access-key", "test")
                .config("spark.sql.catalog." + CATALOG + ".s3.path-style-access", "true")
                .config("spark.sql.catalog." + CATALOG + ".client.region", "us-east-1")
                // Hadoop S3A settings are still needed by some Spark/Iceberg maintenance actions
                // such as remove_orphan_files, which enumerate warehouse paths via FileSystem APIs.
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.endpoint", s3Endpoint)
                .config("spark.hadoop.fs.s3a.access.key", "test")
                .config("spark.hadoop.fs.s3a.secret.key", "test")
                .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
                .getOrCreate();
    }

    // -------------------------------------------------------------------------
    // DDL
    // -------------------------------------------------------------------------

    public static void createNamespace(SparkSession spark) {
        spark.sql("CREATE NAMESPACE IF NOT EXISTS " + CATALOG + "." + NAMESPACE
                + " COMMENT 'Sales data namespace'");
    }

    /**
     * Creates the orders Iceberg table partitioned by {@code category} and {@code days(order_date)}.
     */
    public static void createTable(SparkSession spark) {
        spark.sql(
                "CREATE TABLE IF NOT EXISTS " + FULL_TABLE + " ("
                        + "  order_id    BIGINT  COMMENT 'Unique order identifier',"
                        + "  customer_id BIGINT  COMMENT 'Customer identifier',"
                        + "  category    STRING  COMMENT 'Product category',"
                        + "  amount      DOUBLE  COMMENT 'Order total amount',"
                        + "  status      STRING  COMMENT 'Order status',"
                        + "  order_date  DATE    COMMENT 'Date the order was placed'"
                        + ") USING iceberg"
                        + " PARTITIONED BY (category, days(order_date))"
                        + " TBLPROPERTIES ("
                        + "  'write.format.default'                    = 'parquet',"
                        + "  'write.parquet.compression-codec'         = 'snappy',"
                        + "  'write.metadata.delete-after-commit.enabled' = 'true',"
                        + "  'write.metadata.previous-versions-max'   = '10'"
                        + ")"
        );
    }

    public static void dropTable(SparkSession spark) {
        spark.sql("DROP TABLE IF EXISTS " + FULL_TABLE + " PURGE");
    }

    public static void dropNamespace(SparkSession spark) {
        spark.sql("DROP NAMESPACE IF EXISTS " + CATALOG + "." + NAMESPACE);
    }

    // -------------------------------------------------------------------------
    // DML – inserts
    // -------------------------------------------------------------------------

    /** Inserts the first batch of 5 orders (creates snapshot 1 – append). */
    public static void insertFirstBatch(SparkSession spark) {
        spark.sql(
                "INSERT INTO " + FULL_TABLE + " VALUES"
                        + " (1,  100, 'Electronics', 1500.00, 'COMPLETED', DATE '2024-01-15'),"
                        + " (2,  101, 'Clothing',     200.50, 'COMPLETED', DATE '2024-01-16'),"
                        + " (3,  102, 'Electronics',  850.00, 'PENDING',   DATE '2024-01-17'),"
                        + " (4,  103, 'Food',          45.99, 'COMPLETED', DATE '2024-01-15'),"
                        + " (5,  104, 'Clothing',     320.00, 'CANCELLED', DATE '2024-01-18')"
        );
    }

    /** Inserts the second batch of 5 orders (creates snapshot 2 – append). */
    public static void insertSecondBatch(SparkSession spark) {
        spark.sql(
                "INSERT INTO " + FULL_TABLE + " VALUES"
                        + " (6,  105, 'Electronics', 2200.00, 'COMPLETED', DATE '2024-02-01'),"
                        + " (7,  106, 'Food',          89.99, 'PENDING',   DATE '2024-02-02'),"
                        + " (8,  107, 'Clothing',     150.00, 'COMPLETED', DATE '2024-02-03'),"
                        + " (9,  108, 'Electronics', 3100.00, 'COMPLETED', DATE '2024-02-04'),"
                        + " (10, 109, 'Food',         120.50, 'COMPLETED', DATE '2024-02-05')"
        );
    }

    // -------------------------------------------------------------------------
    // DML – queries
    // -------------------------------------------------------------------------

    /** Returns the total row count of the table. */
    public static long countRows(SparkSession spark) {
        return spark.sql("SELECT COUNT(*) AS cnt FROM " + FULL_TABLE).first().getLong(0);
    }

    /**
     * Aggregated summary of completed orders grouped by category,
     * sorted descending by total amount.
     */
    public static Dataset<Row> querySummary(SparkSession spark) {
        return spark.sql(
                "SELECT category,"
                        + "       COUNT(*)           AS order_count,"
                        + "       ROUND(SUM(amount), 2) AS total_amount,"
                        + "       ROUND(AVG(amount), 2) AS avg_amount"
                        + " FROM " + FULL_TABLE
                        + " WHERE status = 'COMPLETED'"
                        + " GROUP BY category"
                        + " ORDER BY total_amount DESC"
        );
    }

    // -------------------------------------------------------------------------
    // DML – row-level writes
    // -------------------------------------------------------------------------

    /** Changes PENDING → SHIPPED for Electronics orders (creates a new overwrite snapshot). */
    public static void updatePendingToShipped(SparkSession spark) {
        spark.sql(
                "UPDATE " + FULL_TABLE
                        + " SET status = 'SHIPPED'"
                        + " WHERE status = 'PENDING' AND category = 'Electronics'"
        );
    }

    /** Deletes all CANCELLED orders. */
    public static void deleteCancelledOrders(SparkSession spark) {
        spark.sql("DELETE FROM " + FULL_TABLE + " WHERE status = 'CANCELLED'");
    }

    /**
     * Upserts from a temporary view:
     * <ul>
     *   <li>Matches on {@code order_id} → updates {@code status} and {@code amount}.</li>
     *   <li>No match → inserts new rows.</li>
     * </ul>
     */
    public static void mergeInto(SparkSession spark) {
        spark.sql(
                "CREATE OR REPLACE TEMP VIEW order_updates AS"
                        + " SELECT * FROM (VALUES"
                        + "   (3,  102, 'Electronics', 900.00, 'COMPLETED', DATE '2024-01-17'),"
                        + "   (11, 110, 'Books',        75.00, 'COMPLETED', DATE '2024-02-10'),"
                        + "   (12, 111, 'Books',       120.00, 'PENDING',   DATE '2024-02-11')"
                        + " ) AS t(order_id, customer_id, category, amount, status, order_date)"
        );
        spark.sql(
                "MERGE INTO " + FULL_TABLE + " target"
                        + " USING order_updates source"
                        + " ON target.order_id = source.order_id"
                        + " WHEN MATCHED THEN"
                        + "   UPDATE SET target.status = source.status, target.amount = source.amount"
                        + " WHEN NOT MATCHED THEN"
                        + "   INSERT *"
        );
    }

    // -------------------------------------------------------------------------
    // Schema evolution
    // -------------------------------------------------------------------------

    private static Table icebergTable(SparkSession spark) {
        try {
            return Spark3Util.loadIcebergTable(spark, FULL_TABLE);
        } catch (Exception e) {
            throw new IllegalStateException("Failed to load Iceberg table " + FULL_TABLE, e);
        }
    }

    /** Adds two new optional columns (schema-only, no new snapshot). */
    public static void addColumns(SparkSession spark) {
        UpdateSchema updateSchema = icebergTable(spark).updateSchema();
        updateSchema.addColumn("discount", Types.DoubleType.get());
        updateSchema.addColumn("region", Types.StringType.get(), "Sales region");
        updateSchema.commit();
    }

    /** Renames the region column to sales_region. */
    public static void renameColumn(SparkSession spark) {
        icebergTable(spark).updateSchema()
                .renameColumn("region", "sales_region")
                .commit();
    }

    /** Drops the discount column. */
    public static void dropColumn(SparkSession spark) {
        icebergTable(spark).updateSchema()
                .deleteColumn("discount")
                .commit();
    }

    // -------------------------------------------------------------------------
    // Partition evolution
    // -------------------------------------------------------------------------

    /**
     * Replaces the day-level date partition with month-level.
     * Existing data files keep the old spec; new writes use the updated spec.
     */
    public static void replacePartitionField(SparkSession spark) {
        spark.sql(
                "ALTER TABLE " + FULL_TABLE
                        + " REPLACE PARTITION FIELD days(order_date) WITH months(order_date)"
        );
    }

    /** Adds a bucket partition on customer_id (multi-spec evolution). */
    public static void addBucketPartitionField(SparkSession spark) {
        spark.sql("ALTER TABLE " + FULL_TABLE + " ADD PARTITION FIELD bucket(8, customer_id)");
    }

    // -------------------------------------------------------------------------
    // Time travel
    // -------------------------------------------------------------------------

    /** Reads the table as of a specific Iceberg snapshot id. */
    public static Dataset<Row> timeTravelBySnapshotId(SparkSession spark, long snapshotId) {
        return spark.sql(
                "SELECT COUNT(*) AS row_count"
                        + " FROM " + FULL_TABLE + " VERSION AS OF " + snapshotId
        );
    }

    /**
     * Reads the table as of a specific timestamp string
     * (format {@code yyyy-MM-dd HH:mm:ss}).
     */
    public static Dataset<Row> timeTravelByTimestamp(SparkSession spark, String timestamp) {
        return spark.sql(
                "SELECT COUNT(*) AS row_count"
                        + " FROM " + FULL_TABLE + " TIMESTAMP AS OF '" + timestamp + "'"
        );
    }

    // -------------------------------------------------------------------------
    // Iceberg metadata tables
    // -------------------------------------------------------------------------

    /** All snapshots ordered chronologically. */
    public static Dataset<Row> querySnapshots(SparkSession spark) {
        return spark.sql(
                "SELECT snapshot_id, parent_id, operation, committed_at"
                        + " FROM " + FULL_TABLE + ".snapshots"
                        + " ORDER BY committed_at"
        );
    }

    /** Full snapshot lineage history. */
    public static Dataset<Row> queryHistory(SparkSession spark) {
        return spark.sql(
                "SELECT made_current_at, snapshot_id, parent_id, is_current_ancestor"
                        + " FROM " + FULL_TABLE + ".history"
        );
    }

    /** Manifest files and their row-level statistics. */
    public static Dataset<Row> queryManifests(SparkSession spark) {
        return spark.sql(
                "SELECT path, length,"
                        + "       partition_spec_id, added_snapshot_id,"
                        + "       added_data_files_count, existing_data_files_count, deleted_data_files_count"
                        + " FROM " + FULL_TABLE + ".manifests"
        );
    }

    /** Individual data files tracked by the current snapshot. */
    public static Dataset<Row> queryFiles(SparkSession spark) {
        return spark.sql(
                "SELECT file_path, file_format, record_count, file_size_in_bytes"
                        + " FROM " + FULL_TABLE + ".files"
        );
    }

    /** Partition statistics for the current snapshot. */
    public static Dataset<Row> queryPartitions(SparkSession spark) {
        return spark.sql(
                "SELECT partition, spec_id, record_count, file_count"
                        + " FROM " + FULL_TABLE + ".partitions"
                        + " ORDER BY record_count DESC"
        );
    }

    /** Named references (branches and tags). */
    public static Dataset<Row> queryRefs(SparkSession spark) {
        return spark.sql("SELECT name, type, snapshot_id FROM " + FULL_TABLE + ".refs");
    }

    // -------------------------------------------------------------------------
    // Table maintenance stored procedures
    // -------------------------------------------------------------------------

    /**
     * Expires old snapshots, retaining the two most recent.
     * Returns rows describing deleted file counts.
     */
    public static Dataset<Row> expireSnapshots(SparkSession spark) {
        return spark.sql(
                "CALL " + CATALOG + ".system.expire_snapshots("
                        + "  table       => '" + FULL_TABLE + "',"
                        + "  older_than  => TIMESTAMP '2099-01-01 00:00:00',"
                        + "  retain_last => 2"
                        + ")"
        );
    }

    /**
     * Compacts data files using the default binpack strategy.
     * Returns before/after byte and file counts.
     */
    public static Dataset<Row> rewriteDataFiles(SparkSession spark) {
        return spark.sql(
                "CALL " + CATALOG + ".system.rewrite_data_files("
                        + "  table => '" + FULL_TABLE + "'"
                        + ")"
        );
    }

    /**
     * Rewrites manifest files to merge many small manifests.
     * Returns rewritten/added manifest counts.
     */
    public static Dataset<Row> rewriteManifests(SparkSession spark) {
        return spark.sql(
                "CALL " + CATALOG + ".system.rewrite_manifests("
                        + "  table => '" + FULL_TABLE + "'"
                        + ")"
        );
    }

    /**
     * Performs a dry-run orphan file scan (returns file paths, does not delete).
     * Change {@code dry_run => false} to actually remove orphaned files.
     */
    public static Dataset<Row> removeOrphanFiles(SparkSession spark) {
        return spark.sql(
                "CALL " + CATALOG + ".system.remove_orphan_files("
                        + "  table   => '" + FULL_TABLE + "',"
                        + "  dry_run => true"
                        + ")"
        );
    }

    // -------------------------------------------------------------------------
    // Main entry point
    // -------------------------------------------------------------------------

    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println(
//                    thrift://localhost:9083  http://localhost:4566  s3://my-bucket/warehouse
                    "Usage: IcebergOperationsApp <hms-thrift-uri> <s3-endpoint> <warehouse-s3-uri>");
            System.exit(1);
        }

        SparkSession spark = createSparkSession(args[0], args[1], args[2]);

        try {
            section("0. Reset existing demo table");
            dropTable(spark);

            section("1. Create namespace");
            createNamespace(spark);

            section("2. Create partitioned Iceberg table");
            createTable(spark);

            section("3. Insert data (two batches → two snapshots)");
            insertFirstBatch(spark);
            insertSecondBatch(spark);
            System.out.println("Row count: " + countRows(spark));

            section("4. Aggregated query – completed orders by category");
            querySummary(spark).show(false);

            section("5. UPDATE – pending Electronics → SHIPPED");
            updatePendingToShipped(spark);

            section("6. DELETE – remove CANCELLED orders");
            deleteCancelledOrders(spark);
            System.out.println("Row count after delete: " + countRows(spark));

            section("7. MERGE INTO – upsert from view");
            mergeInto(spark);
            System.out.println("Row count after merge: " + countRows(spark));

            section("8. Schema evolution – add, rename, drop columns");
            addColumns(spark);
            renameColumn(spark);
            dropColumn(spark);

            section("9. Partition evolution – days → months");
            replacePartitionField(spark);

            section("10. Metadata: snapshots");
            querySnapshots(spark).show(false);

            section("11. Metadata: history");
            queryHistory(spark).show(false);

            section("12. Metadata: manifest files");
            queryManifests(spark).show(false);

            section("13. Metadata: data files");
            queryFiles(spark).show(false);

            section("14. Metadata: partitions");
            queryPartitions(spark).show(false);

            section("15. Metadata: refs (branches / tags)");
            queryRefs(spark).show(false);

            section("16. Time travel – snapshot id");
            long firstSnapshotId = querySnapshots(spark).first().getLong(0);
            timeTravelBySnapshotId(spark, firstSnapshotId).show(false);

            section("17. Maintenance – rewrite data files");
            rewriteDataFiles(spark).show(false);

            section("18. Maintenance – rewrite manifests");
            rewriteManifests(spark).show(false);

            section("19. Maintenance – expire snapshots (retain last 2)");
            expireSnapshots(spark).show(false);

            section("20. Maintenance – remove orphan files (dry run)");
            removeOrphanFiles(spark).show(false);

        } finally {
            spark.stop();
        }
    }

    private static void section(String title) {
        System.out.println("\n=== " + title + " ===");
    }
}
