package com.github.parquet;

import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.io.OutputFile;

import java.io.IOException;
import java.io.Serializable;

/**
 * A builder to create a {@link ParquetWriter} from a Parquet {@link OutputFile}.
 * @param <T> The type of elements written by the writer.
 */
@FunctionalInterface
public interface ParquetBuilder<T> extends Serializable {
    /**
     * Creates and configures a parquet writer to the given output file.
     */
    ParquetWriter<T> createWriter(OutputFile out) throws IOException;
}
