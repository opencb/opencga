package org.opencb.opencga.app.cli.main.io;

import org.apache.commons.lang3.StringUtils;
import org.fusesource.jansi.Ansi;
import org.fusesource.jansi.AnsiConsole;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.fusesource.jansi.Ansi.Erase;
import static org.fusesource.jansi.Ansi.ansi;

public class Table<T> {
    public static final String DEFAULT_EMPTY_VALUE = "-";
    private final List<TableColumnSchema<T>> schema = new ArrayList<>();
    private List<TableColumn<T>> columns = new ArrayList<>();
    private final TablePrinter tablePrinter;
    private boolean multiLine = true;

    public enum PrinterType {
        ASCII, JANSI, TSV,
    }

    public Table() {
        this(new JAnsiTablePrinter());
    }

    public Table(PrinterType type) {
        switch (type) {
            case ASCII:
                this.tablePrinter = new AsciiTablePrinter();
                break;
            case JANSI:
                this.tablePrinter = new JAnsiTablePrinter();
                break;
            case TSV:
                this.tablePrinter = new TsvTablePrinter();
                break;
            default:
                throw new IllegalArgumentException("Unknown type : " + type);
        }
    }

    public Table(TablePrinter tablePrinter) {
        this.tablePrinter = tablePrinter;
    }

    public Table<T> addColumn(String name, Function<T, String> get) {
        return addColumn(new TableColumnSchema<>(name, get));
    }

    public Table<T> addColumnNumber(String name, Function<T, ? extends Number> get) {
        return addColumnNumber(name, get, DEFAULT_EMPTY_VALUE);
    }

    public Table<T> addColumnNumber(String name, Function<T, ? extends Number> get, String defaultValue) {
        return addColumn(new TableColumnSchema<>(name, TableColumnSchema.wrap(get, defaultValue)));
    }

    public Table<T> addColumnEnum(String name, Function<T, ? extends Enum<?>> get) {
        return addColumnEnum(name, get, DEFAULT_EMPTY_VALUE);
    }

    public Table<T> addColumnEnum(String name, Function<T, ? extends Enum<?>> get, String defaultValue) {
        return addColumn(new TableColumnSchema<>(name, TableColumnSchema.wrap(get, defaultValue)));
    }

    public Table<T> addColumn(String name, Function<T, String> get, String defaultValue) {
        return addColumn(new TableColumnSchema<>(name, TableColumnSchema.wrap(get, defaultValue)));
    }

    public Table<T> addColumn(String name, Function<T, String> get, int maxWidth, String defaultValue) {
        return addColumn(new TableColumnSchema<>(name, TableColumnSchema.wrap(get, defaultValue), maxWidth));
    }

    public Table<T> addColumn(String name, Function<T, String> get, int maxWidth) {
        return addColumn(new TableColumnSchema<>(name, get, maxWidth));
    }

    public Table<T> addColumn(TableColumnSchema<T> e) {
        schema.add(e);
        return this;
    }

    public Table<T> addColumns(Collection<TableColumnSchema<T>> e) {
        schema.addAll(e);
        return this;
    }

    public Table<T> setMultiLine(boolean multiLine) {
        this.multiLine = multiLine;
        return this;
    }

    public void print(String content) {
        tablePrinter.print(content);
    }

    public void println() {
        tablePrinter.println(null);
    }

    public void println(String content) {
        tablePrinter.println(content);
    }

    public void printTable(List<T> rows) {
        updateTable(rows);
        printTable();
    }

    public void printTable() {
        tablePrinter.printFullLine(columns);
        tablePrinter.printHeader(columns);
        tablePrinter.printLine(columns);
        for (int i = 0; i < columns.get(0).values.size(); i++) {
            if (multiLine) {
                tablePrinter.printRowMultiLine(columns, i);
            } else {
                tablePrinter.printRow(columns, i);
            }
        }
        tablePrinter.printFooter(columns);
        tablePrinter.printFullLine(columns);
    }

    public void restoreCursorPosition() {
        tablePrinter.restorePosition();
    }

    public void printFullLine() {
        tablePrinter.printFullLine(columns);
    }

    public void updateTable(List<T> rows) {
        columns = schema.stream().map(column -> new TableColumn<>(column, rows)).collect(Collectors.toList());
    }


    public static class TableColumn<T> {
        private final TableColumnSchema<T> tableColumnSchema;
        private ArrayList<String> values = new ArrayList<>();
        private int width;

        public TableColumn(TableColumnSchema<T> tableColumnSchema, List<T> rows) {
            this.tableColumnSchema = tableColumnSchema;

            values.ensureCapacity(rows.size());
            for (T row : rows) {
                String value = tableColumnSchema.getGet().apply(row);
                values.add(value);
            }

            width = values.stream().mapToInt(String::length).max().orElse(5);
            width = Math.max(width, tableColumnSchema.getName().length());
            if (width > tableColumnSchema.getMaxWidth()) {
                width = tableColumnSchema.getMaxWidth();
            }
            if (width < tableColumnSchema.getMinWidth()) {
                width = tableColumnSchema.getMinWidth();
            }
        }

        public String getName() {
            return tableColumnSchema.getName();
        }

        public String getPrintName() {
            return StringUtils.rightPad(getPrintValue(tableColumnSchema.getName()), width);
        }

        public String getValue(int idx) {
            return values.get(idx);
        }

        public List<String> getMultiLineValue(int idx) {
            return getMultiLineValue(idx, "  ");
        }

        public List<String> getMultiLineValue(int idx, String indent) {
            String value = getValue(idx);
            List<String> lines = new ArrayList<>();
            while (value.length() > getMaxWidth()) {
                int splitPosition = getMultiLineSplitPosition(value, indent.length(), '\n', '\t', ' ', ',', ':', '/', '.', '_', '-');
                lines.add(StringUtils.rightPad(value.substring(0, splitPosition), width));
                value = indent + value.substring(splitPosition);
            }
            lines.add(StringUtils.rightPad(value, width));
            return lines;
        }

        private int getMultiLineSplitPosition(String value, int indentLength, char... splitPoints) {
            for (char splitPoint : splitPoints) {
                int splitPosition = Math.min(1 + value.lastIndexOf(splitPoint, getMaxWidth() - 1), value.length());
                if (splitPosition <= indentLength) {
                    splitPosition = Math.min(1 + value.indexOf(splitPoint, getMaxWidth() - 1), value.length());
                }
                if (splitPosition > 0 && splitPosition <= getMaxWidth()) {
                    return splitPosition;
                }
            }
            return width - indentLength;
        }

        public String getPrintValue(int idx) {
            return getPrintValue(values.get(idx));
        }

        public String getPrintValue(String value) {
            if (value.length() > width) {
                return value.substring(0, width - 3) + "...";
            } else {
                return StringUtils.rightPad(value, width);
            }
        }

        public Function<T, String> getGet() {
            return tableColumnSchema.getGet();
        }

        public int getMaxWidth() {
            return tableColumnSchema.getMaxWidth();
        }

        public int getWidth() {
            return width;
        }

        public String getLine() {
            return getLine(0);
        }

        public String getLine(int pad) {
            return StringUtils.repeat("-", width + pad);
        }
    }

    public static class TableColumnSchema<T> {
        private interface SecureGet<T> extends Function<T, String> {
        }

        public static final int DEFAULT_MAX_WIDTH = 30;
        public static final int DEFAULT_MIN_WIDTH = 5;
        private final String name;
        private final SecureGet<T> get;
        private final int minWidth;
        private final int maxWidth;

        public TableColumnSchema(String name, Function<T, String> get) {
            this(name, get, DEFAULT_MAX_WIDTH, DEFAULT_MIN_WIDTH);
        }

        public TableColumnSchema(String name, Function<T, String> get, int maxWidth) {
            this(name, get, maxWidth, DEFAULT_MIN_WIDTH);
        }

        public TableColumnSchema(String name, Function<T, String> get, int maxWidth, int minWidth) {
            this(name, get, maxWidth, minWidth, DEFAULT_EMPTY_VALUE);
        }

        public TableColumnSchema(String name, Function<T, String> get, int maxWidth, int minWidth, String defaultEmptyValue) {
            this.name = name;
            this.get = wrap(get, defaultEmptyValue);
            this.maxWidth = maxWidth;
            this.minWidth = minWidth;
        }

        private static <T> SecureGet<T> wrap(Function<T, ?> get, String defaultValue) {
            if (get instanceof TableColumnSchema.SecureGet) {
                return ((SecureGet<T>) get);
            } else {
                return t -> {
                    try {
                        Object o = get.apply(t);
                        if (o == null) {
                            return defaultValue;
                        } else {
                            String str = o.toString();
                            if (str == null || str.isEmpty()) {
                                return defaultValue;
                            } else {
                                return str;
                            }
                        }
                    } catch (RuntimeException e) {
                        return defaultValue;
                    }
                };
            }
        }

        public String getName() {
            return name;
        }

        public Function<T, String> getGet() {
            return get;
        }

        public int getMaxWidth() {
            return maxWidth;
        }

        public int getMinWidth() {
            return minWidth;
        }
    }

    public interface TablePrinter {

        // Restores initial position so content is overwritten
        void restorePosition();

        void println(String content);

        void print(String content);

        <T> void printHeader(List<Table.TableColumn<T>> columns);

        default <T> void printFooter(List<TableColumn<T>> columns) {
            printFullLine(columns);
        }

        <T> void printLine(List<Table.TableColumn<T>> columns);

        <T> void printFullLine(List<Table.TableColumn<T>> columns);

        <T> void printRow(List<TableColumn<T>> columns, int i);

        default <T> void printRowMultiLine(List<TableColumn<T>> columns, int i) {
            printRow(columns, i);
        }
    }

    public static class JAnsiTablePrinter implements TablePrinter {

        private final PrintStream out;
        private String sep = " ";
        private String pad = " ";
        private int numLines = 0;
        private boolean colour;

        public JAnsiTablePrinter() {
            this(AnsiConsole.out);
        }

        public JAnsiTablePrinter(PrintStream out) {
            AnsiConsole.systemInstall();
            this.out = out == null ? System.out : out;
            colour = true;
        }

        @Override
        public void restorePosition() {
            if (numLines > 0) {
                Ansi ansi = Ansi.ansi();
//                out.print(ansi.cursorUpLine(numLines).eraseScreen(Erase.BACKWARD).reset());
                out.print(ansi.cursorUpLine(numLines).reset());
                numLines = 0;
            }
        }

        @Override
        public <T> void printHeader(List<TableColumn<T>> columns) {
//            ansi.restoreCursorPosition();
//            ansi.saveCursorPosition();
            Ansi ansi = ansi();
            ansi.bold().fg(Ansi.Color.BLACK).bgBright(Ansi.Color.WHITE).a(sep);
            for (TableColumn<T> column : columns) {
                ansi.a(pad).a(column.getPrintName()).a(pad + sep);
            }
            ansi.reset().eraseLine(Erase.FORWARD);
            out.println(ansi);
            numLines++;
        }

        @Override
        public <T> void printFooter(List<TableColumn<T>> tableColumns) {
            Ansi ansi = ansi();
            out.print(ansi.eraseScreen(Erase.FORWARD));
        }

        @Override
        public void println(String content) {
            if (content == null) {
                out.println(ansi().eraseLine(Erase.FORWARD));
                numLines++;
            } else {
                for (String s : content.split("\n")) {
                    numLines++;
                    out.println(ansi().a(s).eraseLine(Erase.FORWARD));
                }
            }
        }

        @Override
        public void print(String content) {
            if (content == null) {
                return;
            }
            if (content.contains("\n")) {
                int idx = content.lastIndexOf("\n");
                println(content.substring(0, idx));
                content = content.substring(idx + 1);
            }
            out.print(content);
        }

        @Override
        public <T> void printLine(List<TableColumn<T>> columns) {
//            Ansi ansi = new Ansi();
//            ansi.a(sep);
//            for (TableColumn<T> column : columns) {
//                ansi.a(column.getLine(pad.length() * 2)).a(sep);
//            }
//            ansi.reset();
//            out.println(ansi);
//            numLines++;
        }

        @Override
        public <T> void printFullLine(List<TableColumn<T>> columns) {
//            for (TableColumn<T> column : columns) {
//                out.print(column.getLine());
//                out.print(StringUtils.repeat("-", pad.length() * 2 + sep.length()));
//            }
//            out.println("-");
//            numLines++;
        }

        @Override
        public <T> void printRow(List<TableColumn<T>> columns, int i) {
            Ansi ansi = ansi();
            ansi.a(sep);
            for (TableColumn<T> column : columns) {
                if (colour) {
                    if (i % 2 == 0) {
                        ansi.bg(Ansi.Color.BLACK);
                    } else {
                        ansi.bg(Ansi.Color.DEFAULT);
                    }
                }
                String printValue = column.getPrintValue(i);
                Ansi.Color fg = getColor(printValue);
                ansi.a(pad).fg(fg).a(printValue).a(pad + sep);
            }
            ansi.reset().eraseLine(Erase.FORWARD);
            out.println(ansi);
            numLines++;
        }

        private Ansi.Color getColor(String printValue) {
            Ansi.Color fg = Ansi.Color.DEFAULT;
            if (printValue.startsWith("RUNNING")) {
                fg = Ansi.Color.GREEN;
            } else if (printValue.startsWith("DONE")) {
                fg = Ansi.Color.BLUE;
            } else if (printValue.startsWith("ERROR")) {
                fg = Ansi.Color.RED;
            } else if (printValue.startsWith("ABORTED")) {
                fg = Ansi.Color.YELLOW;
            }
            return fg;
        }

        @Override
        public <T> void printRowMultiLine(List<TableColumn<T>> columns, int i) {
            List<List<String>> columnLines = columns.stream().map(c -> c.getMultiLineValue(i)).collect(Collectors.toList());
            int lines = columnLines.stream().mapToInt(Collection::size).max().orElse(1);

            for (int lineIdx = 0; lineIdx < lines; lineIdx++) {
                Ansi ansi = ansi();
                ansi.a(sep);
                int columnIdx = -1;
                for (TableColumn<T> column : columns) {
                    columnIdx++;
                    if (colour) {
                        if (i % 2 == 0) {
                            ansi.bg(Ansi.Color.BLACK);
                        } else {
                            ansi.bg(Ansi.Color.DEFAULT);
                        }
                    }
                    List<String> multiLines = columnLines.get(columnIdx);
                    Ansi.Color fg = getColor(multiLines.get(0));
                    ansi.a(pad).fg(fg);

                    if (multiLines.size() > lineIdx) {
                        ansi.a(multiLines.get(lineIdx));
                    } else {
                        ansi.a(StringUtils.repeat(' ', column.width));
                    }
                    ansi.a(pad + sep);
                }
                ansi.reset().eraseLine(Erase.FORWARD);
                out.println(ansi);
                numLines++;
            }
        }
    }

    public static class AsciiTablePrinter implements TablePrinter {
        private String sep = "|";
        private String pad = " ";
        private PrintStream out;

        public AsciiTablePrinter() {
            out = System.out;
        }

        @Override
        public void restorePosition() {
            // no op
            return;
        }

        @Override
        public void println(String content) {
            out.println(content);
        }

        @Override
        public void print(String content) {
            out.print(content);
        }

        @Override
        public <T> void printHeader(List<TableColumn<T>> columns) {
            out.print(sep);
            for (TableColumn<T> column : columns) {
                out.print(pad);
                out.print(column.getPrintName());
                out.print(pad + sep);
            }
            out.println();
        }

        @Override
        public <T> void printLine(List<TableColumn<T>> columns) {
            out.print(sep);
            for (TableColumn<T> column : columns) {
                out.print(column.getLine(pad.length() * 2));
                out.print(sep);
            }
            out.println();
        }

        @Override
        public <T> void printFullLine(List<TableColumn<T>> columns) {
            for (TableColumn<T> column : columns) {
                out.print(column.getLine());
                out.print(StringUtils.repeat("-", pad.length() * 2 + sep.length()));
            }
            out.println("-");
        }

        @Override
        public <T> void printRow(List<TableColumn<T>> columns, int i) {
            out.print(sep);
            for (TableColumn<T> column : columns) {
                out.print(pad);
                out.print(column.getPrintValue(i));
                out.print(pad + sep);
            }
            out.println();
        }

        @Override
        public <T> void printRowMultiLine(List<TableColumn<T>> columns, int i) {
            List<List<String>> columnLines = columns.stream().map(c -> c.getMultiLineValue(i)).collect(Collectors.toList());
            int lines = columnLines.stream().mapToInt(Collection::size).max().orElse(1);

            for (int lineIdx = 0; lineIdx < lines; lineIdx++) {
                out.print(sep);
                int columnIdx = -1;
                for (TableColumn<T> column : columns) {
                    columnIdx++;
                    out.print(pad);
                    List<String> multiLines = columnLines.get(columnIdx);
                    if (multiLines.size() > lineIdx) {
                        out.print(multiLines.get(lineIdx));
                    } else {
                        out.print(StringUtils.repeat(' ', column.width));
                    }
                    out.print(pad + sep);
                }
                out.println();
            }
        }
    }

    public static class TsvTablePrinter implements TablePrinter {
        private String sep = "\t";
        private PrintStream out;

        public TsvTablePrinter() {
            this(System.out);
        }

        public TsvTablePrinter(PrintStream out) {
            this.out = out;
        }

        @Override
        public void restorePosition() {
            return;
        }

        @Override
        public void println(String content) {
            out.println(content);
        }

        @Override
        public void print(String content) {
            out.print(content);
        }

        @Override
        public <T> void printHeader(List<TableColumn<T>> columns) {
            Iterator<TableColumn<T>> iterator = columns.iterator();
            out.print("#");
            while (iterator.hasNext()) {
                TableColumn<T> column = iterator.next();
                String name = column.getName().replace(" ", "_").toUpperCase();
                out.print(name);
                if (iterator.hasNext()) {
                    out.print(sep);
                }
            }
            out.println();
        }

        @Override
        public <T> void printLine(List<TableColumn<T>> columns) {
        }

        @Override
        public <T> void printFullLine(List<TableColumn<T>> columns) {
        }

        @Override
        public <T> void printRow(List<TableColumn<T>> columns, int i) {
            Iterator<TableColumn<T>> iterator = columns.iterator();
            while (iterator.hasNext()) {
                TableColumn<T> column = iterator.next();
                out.print(column.getValue(i));
                if (iterator.hasNext()) {
                    out.print(sep);
                }
            }
            out.println();
        }
    }
}

