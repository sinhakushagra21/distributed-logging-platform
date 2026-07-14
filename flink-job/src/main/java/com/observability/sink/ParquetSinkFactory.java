package com.observability.sink;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;

/**
 * Builds the S3/MinIO archival sink: enriched logs written as columnar Parquet,
 * bucketed into hourly folders (yyyy-MM-dd--HH).
 *
 * <p>Bulk (Parquet) formats in Flink roll files on checkpoint, which is what
 * ties the archive to the exactly-once story: a Parquet part file only becomes
 * "committed" when a checkpoint completes. The tradeoff is many smaller files at
 * short checkpoint intervals; a production setup would add a compaction job.
 *
 * <p>The path is an {@code s3a://} URI when targeting MinIO; the Flink runtime
 * resolves it via the flink-s3-fs-hadoop plugin (configured in the image).
 */
public final class ParquetSinkFactory {

    private ParquetSinkFactory() {
    }

    public static FileSink<GenericRecord> build(String basePath) {
        Schema schema = new Schema.Parser().parse(AvroSchemas.ENRICHED_LOG_JSON);
        return FileSink
                .forBulkFormat(new Path(basePath), AvroParquetWriters.forGenericRecord(schema))
                .withBucketAssigner(new DateTimeBucketAssigner<>("yyyy-MM-dd--HH"))
                .build();
    }
}
