package com.github.parquet;

import java.io.IOException;
import org.apache.parquet.io.PositionOutputStream;
import static org.apache.parquet.Preconditions.checkNotNull;

public class PositionOutputStreamAdapter extends PositionOutputStream {
    private final FSDataOutputStream out;

    /**
     * Create a new PositionOutputStreamAdapter.
     * @param out the stream written to.
     */
    public PositionOutputStreamAdapter(FSDataOutputStream out) {
        this.out = checkNotNull(out, "out");
    }

    @Override
    public long getPos() throws IOException {
        return out.getPos();
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);
    }

    @Override
    public void write(byte[] buffer, int off, int len) throws IOException {
        out.write(buffer, off, len);
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void close() {
        // we do not actually close the internal stream here, to prevent that the finishing
        // of the Parquet Writer closes the target output stream
    }
}
