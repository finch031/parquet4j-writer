package com.github.parquet;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import javax.annotation.Nullable;
import java.io.*;

/**
 * @author yusheng
 * @version 1.0.0
 * @datetime 2021-05-26 15:17
 * @description
 */
public final class Utils {
    private static final Logger LOG = LogManager.getLogger(Utils.class);

    private Utils(){
        // no instance.
    }

    /**
     * @param o the param to check
     * @param name the name of the param for the error message
     * @param <T> the type of the object
     * @return the validated o
     * @throws NullPointerException if o is null
     */
    public static <T> T checkNotNull(T o, String name) throws NullPointerException {
        if (o == null) {
            throw new NullPointerException(name + " should not be null");
        }
        return o;
    }

    /**
     * Get the stack trace from an exception as a string
     */
    public static String stackTrace(Throwable e) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        e.printStackTrace(pw);
        return sw.toString();
    }

    /**
     * Closes a {@link Closeable}, with control over whether an
     * {@code IOException} may be thrown. This is primarily useful in a
     * finally block, where a thrown exception needs to be logged but not
     * propagated (otherwise the original exception will be lost).
     *
     * <p>If {@code swallowIOException} is true then we never throw
     * {@code IOException} but merely log it.
     *
     * <p>Example:
     *
     * <p><pre>public void useStreamNicely() throws IOException {
     * SomeStream stream = new SomeStream("foo");
     * boolean threw = true;
     * try {
     *   // Some code which does something with the Stream. May throw a
     *   // Throwable.
     *   threw = false; // No throwable thrown.
     * } finally {
     *   // Close the stream.
     *   // If an exception occurs, only rethrow it if (threw==false).
     *   Closeables.close(stream, threw);
     * }
     * </pre>
     *
     * @param closeable the {@code Closeable} object to be closed, or null,
     *     in which case this method does nothing
     * @param swallowIOException if true, don't propagate IO exceptions
     *     thrown by the {@code close} methods
     * @throws IOException if {@code swallowIOException} is false and
     *     {@code close} throws an {@code IOException}.
     */
    public static void close(@Nullable Closeable closeable,
                             boolean swallowIOException) throws IOException {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (IOException e) {
            if (swallowIOException) {
                LOG.warn("IOException thrown while closing Closeable.", e);
            } else {
                throw e;
            }
        }
    }

    /**
     *  create Avro Schema from file.
     * */
    public static Schema fromAvro(InputStream in) throws IOException {
        GenericDatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
        DataFileStream<GenericRecord> stream = null;
        boolean threw = true;

        try {
            stream = new DataFileStream<>(in, datumReader);
            Schema schema = stream.getSchema();
            threw = false;
            return schema;
        } finally {
            close(stream, threw);
        }
    }

    /**
     *  create Avro Schema from file.
     * */
    public static Schema fromAvsc(InputStream in) throws IOException {
        // the parser has state, so use a new one each time
        return new Schema.Parser().parse(in);
    }

    /**
     * Given a time expressed in milliseconds, append the time formatted as
     * "hh[:mm[:ss]]".
     *
     * @param buf    Buffer to append to
     * @param millis Milliseconds
     */
    public static void appendPosixTime(StringBuilder buf, int millis) {
        if (millis < 0) {
            buf.append('-');
            millis = -millis;
        }
        int hours = millis / 3600000;
        buf.append(hours);
        millis -= hours * 3600000;
        if (millis == 0) {
            return;
        }
        buf.append(':');
        int minutes = millis / 60000;
        if (minutes < 10) {
            buf.append('0');
        }
        buf.append(minutes);
        millis -= minutes * 60000;
        if (millis == 0) {
            return;
        }
        buf.append(':');
        int seconds = millis / 1000;
        if (seconds < 10) {
            buf.append('0');
        }
        buf.append(seconds);
    }

}
