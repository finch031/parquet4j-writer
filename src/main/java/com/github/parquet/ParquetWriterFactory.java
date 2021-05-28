package com.github.parquet;

import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;
import java.io.IOException;

/**
 * a factory that creates a Parquet {@link BulkWriter}.The factory takes a user-supplied
 * builder to assemble Parquet's writer and then turns it into a {@code BulkWriter}.
 * @param <T> The type of record to write.
 */
public class ParquetWriterFactory<T> implements BulkWriter.Factory<T> {
    private static final long serialVersionUID = 1L;

    /** the builder to construct the ParquetWriter. */
    private final ParquetBuilder<T> writerBuilder;

    /**
     * creates a new ParquetWriterFactory using the given builder
     * to assemble the ParquetWriter.
     * @param writerBuilder The builder to construct the ParquetWriter.
     */
    public ParquetWriterFactory(ParquetBuilder<T> writerBuilder) {
        this.writerBuilder = writerBuilder;
    }

    @Override
    public BulkWriter<T> create(FSDataOutputStream stream) throws IOException {
        final OutputFile out = new StreamOutputFile(stream);
        final ParquetWriter<T> writer = writerBuilder.createWriter(out);
        return new ParquetBulkWriter<>(writer);
    }
}
