package ru.pdbp;
import java.io.*;
import java.nio.file.*;
import java.util.*;
/** Ordinary file writer, independent of Spark. Does not overwrite existing files. */
public final class PdbpWriter implements AutoCloseable {
    private final BufferedWriter writer;
    private final Codec.Schema schema;
    public PdbpWriter(Path path, Codec.Schema schema) throws IOException {
        this.schema = Objects.requireNonNull(schema);
        writer = Files.newBufferedWriter(path, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE);
        try { writer.write("PDBP/1\n" + Codec.header(schema) + "\n"); }
        catch (IOException e) { try { writer.close(); } catch (IOException close) { e.addSuppressed(close); } throw e; }
    }
    public void write(List<?> values) throws IOException {
        String encoded = Codec.encode(schema, values);
        writer.write(encoded); writer.write('\n');
    }
    public void close() throws IOException { writer.close(); }
}
