package ru.pdbp;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.regex.Pattern;

/** Shared grammar for the standalone writer and the Spark reader. */
public final class Codec {
    private Codec() {}
    public enum Type { LONG, STRING, DOUBLE }
    public record Column(String name, Type type) implements Serializable {
        public Column {
            if (name == null || !name.matches("[A-Za-z_][A-Za-z0-9_]*") || type == null)
                throw new IllegalArgumentException("Invalid column: " + name);
        }
    }
    public record Schema(List<Column> columns) implements Serializable {
        public Schema {
            columns = List.copyOf(columns);
            if (columns.isEmpty() || columns.stream().map(Column::name).distinct().count() != columns.size())
                throw new IllegalArgumentException("Schema must have unique columns and be nonempty");
        }
    }
    public static final Schema DEMO = new Schema(List.of(new Column("id", Type.LONG),
            new Column("name", Type.STRING), new Column("amount", Type.DOUBLE)));
    public static String header(Schema schema) {
        return String.join("|", schema.columns().stream().map(c -> c.name() + ":" + c.type()).toList());
    }
    public static Schema readHeader(BufferedReader reader) throws IOException {
        if (!"PDBP/1".equals(reader.readLine())) throw new IOException("Expected PDBP/1 signature");
        String line = reader.readLine();
        if (line == null) throw new IOException("Missing schema");
        try {
            var cols = new ArrayList<Column>();
            for (String field : line.split("\\|", -1)) {
                String[] pair = field.split(":", -1);
                if (pair.length != 2) throw new IllegalArgumentException("Expected name:TYPE");
                cols.add(new Column(pair[0], Type.valueOf(pair[1])));
            }
            return new Schema(cols);
        } catch (IllegalArgumentException e) { throw new IOException("Invalid schema: " + line, e); }
    }
    public static BufferedReader open(Path path) throws IOException { return Files.newBufferedReader(path); }
    public static Schema schema(Path path) throws IOException {
        try (var r = open(path)) { return readHeader(r); }
    }
    private static String escape(String s) {
        return s.replace("\\", "\\\\").replace("|", "\\|").replace("\n", "\\n")
                .replace("\r", "\\r").replace("\t", "\\t");
    }
    private static List<String> fields(String line) throws IOException {
        var out = new ArrayList<String>(); var field = new StringBuilder();
        boolean escaped = false;
        for (char c : line.toCharArray()) {
            if (escaped) {
                field.append(switch (c) {
                    case '\\' -> '\\'; case '|' -> '|'; case 'n' -> '\n';
                    case 'r' -> '\r'; case 't' -> '\t';
                    default -> throw new IOException("Unknown escape: \\" + c);
                }); escaped = false;
            } else if (c == '\\') escaped = true;
            else if (c == '|') { out.add(field.toString()); field.setLength(0); }
            else if (c == '\t') throw new IOException("Raw tab must be escaped");
            else field.append(c);
        }
        if (escaped) throw new IOException("Trailing backslash");
        out.add(field.toString()); return out;
    }
    public static Object[] decode(Schema schema, String line) throws IOException {
        var values = fields(line);
        if (values.size() != schema.columns().size()) throw new IOException("Wrong column count");
        Object[] row = new Object[values.size()];
        for (int i=0; i<row.length; i++) {
            String s = values.get(i);
            try {
                row[i] = switch (schema.columns().get(i).type()) {
                    case STRING -> s;
                    case LONG -> {
                        if (!s.matches("-?[0-9]+")) throw new NumberFormatException("Invalid LONG");
                        yield Long.valueOf(s);
                    }
                    case DOUBLE -> {
                        if (!s.matches("-?(?:[0-9]+(?:\\.[0-9]*)?|\\.[0-9]+)(?:[eE][+-]?[0-9]+)?"))
                            throw new NumberFormatException("Invalid DOUBLE");
                        double d = Double.parseDouble(s);
                        if (!Double.isFinite(d)) throw new NumberFormatException("Non-finite DOUBLE");
                        yield d;
                    }
                };
            } catch (NumberFormatException e) { throw new IOException("Invalid value for " + schema.columns().get(i).name() + ": " + s, e); }
        }
        return row;
    }
    public static String encode(Schema schema, List<?> values) {
        if (values.size() != schema.columns().size()) throw new IllegalArgumentException("Wrong column count");
        var result = new ArrayList<String>();
        for (int i=0; i<values.size(); i++) {
            Object v = values.get(i);
            boolean valid = switch (schema.columns().get(i).type()) {
                case LONG -> v instanceof Long;
                case STRING -> v instanceof String;
                case DOUBLE -> v instanceof Double d && Double.isFinite(d);
            };
            if (!valid) throw new IllegalArgumentException("Wrong type at column " + i);
            result.add(escape(v.toString()));
        }
        return String.join("|", result);
    }
}
