package com.github.parquet;

import org.apache.parquet.io.OutputFile;
import org.apache.parquet.io.PositionOutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * an implementation of Parquet's {@link OutputFile} interface
 * because the implementation goes against an open stream, rather than open its
 * own streams against a file, instances can create one stream only.
 */
public class StreamOutputFile implements OutputFile {
    private static final long DEFAULT_BLOCK_SIZE = 64L * 1024L * 1024L;
    private final FSDataOutputStream stream;
    private final AtomicBoolean used;

    /**
     * creates a new StreamOutputFile. The first call to {@link #create(long)}
     * or {@link #createOrOverwrite(long)} returns a stream that writes to the given stream.
     * @param stream The stream to write to.
     */
    public StreamOutputFile(FSDataOutputStream stream) {
        this.stream = Utils.checkNotNull(stream,"stream");
        this.used = new AtomicBoolean(false);
    }

    @Override
    public PositionOutputStream create(long blockSizeHint) {
        if (used.compareAndSet(false, true)) {
            return new PositionOutputStreamAdapter(stream);
        } else {
            throw new IllegalStateException("A stream against this file was already created.");
        }
    }

    @Override
    public PositionOutputStream createOrOverwrite(long blockSizeHint) {
        return create(blockSizeHint);
    }

    @Override
    public boolean supportsBlockSize() {
        return false;
    }

    @Override
    public long defaultBlockSize() {
        return DEFAULT_BLOCK_SIZE;
    }
}
