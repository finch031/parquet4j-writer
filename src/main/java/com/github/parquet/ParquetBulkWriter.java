package com.github.parquet;

import org.apache.parquet.hadoop.ParquetWriter;
import java.io.IOException;

/**
 * A simple {@link BulkWriter} implementation that wraps a {@link ParquetWriter}.
 *
 * @param <T> The type of records written.
 */
public class ParquetBulkWriter<T> implements BulkWriter<T> {
    /** The ParquetWriter to write to. */
    private final ParquetWriter<T> parquetWriter;

    /**
     * Creates a new ParquetBulkWriter wrapping the given ParquetWriter.
     *
     * @param parquetWriter The ParquetWriter to write to.
     */
    public ParquetBulkWriter(ParquetWriter<T> parquetWriter) {
        this.parquetWriter = Utils.checkNotNull(parquetWriter, "parquetWriter");
    }

    @Override
    public void addElement(T datum) throws IOException {
        parquetWriter.write(datum);
    }

    @Override
    public void flush() {
        // nothing we can do here
    }

    @Override
    public void finish() throws IOException {
        parquetWriter.close();
    }
}
