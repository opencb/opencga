package org.opencb.opencga.storage.hadoop.app;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.TimeUtils;
import org.opencb.opencga.core.tools.ToolParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Stream;

public abstract class AbstractMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMain.class);

    protected static ObjectMapper objectMapper = new ObjectMapper()
            .configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);

    protected static <T> T readFile(String file, Class<T> type) throws java.io.IOException {
        return objectMapper.readValue(Paths.get(file).toFile(), type);
    }

    protected abstract void run(String[] args) throws Exception;

    protected static RuntimeException error(String msg) {
        printError(msg);
        return new IllegalArgumentException(msg);
    }

    protected static RuntimeException error(Exception e) {
        printError(e.getMessage());
        return new IllegalArgumentException(e);
    }

    protected static void printError(String msg) {
        print(new ObjectMap("error", msg));
    }

    protected static void print(final Object obj) {
        try {
            ObjectWriter objectWriter = objectMapper.writerWithDefaultPrettyPrinter();
            if (obj instanceof Iterable) {
                for (Object o : (Iterable<?>) obj) {
                    print(o);
                }
            } else if (obj instanceof Iterator) {
                Iterator<?> it = (Iterator<?>) obj;
                while (it.hasNext()) {
                    print(it.next());
                }
            } else if (obj instanceof Stream) {
                Stream<?> stream = (Stream<?>) obj;
                print(stream.iterator());
            } else {
                println(objectWriter.writeValueAsString(obj));
            }
            if (obj instanceof AutoCloseable) {
                ((AutoCloseable) obj).close();
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected static void println(String msg) {
        System.out.println(msg);
    }

    protected static String getArg(String[] args, int i, String def) {
        return args.length > i ? args[i] : def;
    }
    protected static String getArg(String[] args, int i) throws Exception {
        if (args.length > i) {
            return args[i];
        } else {
            String msg = "Missing arg " + i;
            printError(msg);
            throw new IllegalArgumentException(msg);
        }
    }

    protected static <T extends ToolParams> T getArgsMap(String[] args, int firstIdx, T params) {
        ObjectMap argsMap = getArgsMap(args, firstIdx, params.fields().keySet().toArray(new String[]{}));
        params.updateParams(argsMap);
        return params;
    }

    protected static ObjectMap getArgsMap(String[] args, int firstIdx, String... keys) {
        Set<String> acceptedKeys = new HashSet<>(Arrays.asList(keys));
        ObjectMap argsMap;
        argsMap = new ObjectMap();
        int i = firstIdx;
        while (i < args.length) {
            String key = args[i];
            while (key.startsWith("-")) {
                key = key.substring(1);
            }
            if (!acceptedKeys.isEmpty()) {
                if (!acceptedKeys.contains(key)) {
                    throw new IllegalArgumentException("Unknown argument '" + args[i] + "'");
                }
            }
            String value = safeArg(args, i + 1);
            if (value == null || value.startsWith("-")) {
                argsMap.put(key, true);
                i += 1;
            } else {
                argsMap.put(key, value);
                i += 2;
            }
        }
        return argsMap;
    }

    protected static String safeArg(String[] args, int idx) {
        return safeArg(args, idx, null);
    }

    protected static String safeArg(String[] args, int idx, String def) {
        return args.length < idx + 1 ? def : args[idx];
    }



    protected interface CallableNoReturn {
        void call() throws IOException;
    }

    protected interface MyCallable<T> {
        T call() throws IOException;
    }

    protected static <T> T runWithTime(String name, CallableNoReturn c) throws IOException {
        return runWithTime(name, () -> {
            c.call();
            return null;
        });
    }

    protected static <T> T runWithTime(String name, MyCallable<T> c) throws IOException {
        StopWatch stopWatch = StopWatch.createStarted();

        T r = c.call();

        LOGGER.info(name + " - " + TimeUtils.durationToString(stopWatch));

        return r;

    }

    public interface CommandExecutor {
        void exec(String[] args) throws Exception;
    }

    public static class NestedCommandExecutor implements CommandExecutor {
        protected final Map<String, String> subCommandAlias = new LinkedHashMap<>();
        protected final Map<String, String> subCommandDescription = new LinkedHashMap<>();
        protected final Map<String, CommandExecutor> commandExecutor = new LinkedHashMap<>();
        protected String argsContext;
        protected CommandExecutor defaultExecutor;

        public NestedCommandExecutor() {
            this("");
        }

        public NestedCommandExecutor(String argsContext) {
            this.argsContext = argsContext;
            this.defaultExecutor = args -> {
//                String command = getArg(args, 0, "help");
                String subCommand = getArg(args, 0, "help");
                println("Unknown subcommand '" + subCommand + "'");
                help(argsContext);
            };
        }

        protected final NestedCommandExecutor addSubCommand(String subCommand, String description, CommandExecutor executor) {
            return addSubCommand(Collections.singletonList(subCommand), description, executor);
        }

        protected final NestedCommandExecutor addSubCommand(List<String> subCommands, String description, CommandExecutor executor) {
            String subCommand = subCommands.get(0);
            for (String alias : subCommands) {
                subCommandAlias.put(alias, subCommand);
            }
            subCommandDescription.put(subCommand, description);
            commandExecutor.put(subCommand, executor);
            return this;
        }

        protected void setup(String command, String[] args) throws Exception {
        }

        protected void cleanup(String command, String[] args) throws Exception {
        }

        public final void exec(String[] args) throws Exception {
            String command = getArg(args, 0, "help");

            if (args.length <= 1) {
                args = new String[]{};
            } else {
                args = Arrays.copyOfRange(args, 1, args.length);
            }
            if (StringUtils.isEmpty(command) || "help".equals(command)) {
                help(argsContext);
            } else {
                String commandResolved = subCommandAlias.getOrDefault(command, command);
                CommandExecutor executor = commandExecutor.getOrDefault(commandResolved, defaultExecutor);
                if (executor instanceof NestedCommandExecutor) {
                    if (argsContext == null || argsContext.isEmpty()) {
                        ((NestedCommandExecutor) executor).argsContext = command;
                    } else {
                        ((NestedCommandExecutor) executor).argsContext = argsContext + " " + command;
                    }
                }
                setup(command, args);
                try {
                    executor.exec(args);
                } finally {
                    cleanup(command, args);
                }
            }
        }

        protected void help(String command) {
            println("Commands:");
            int maxLength = subCommandDescription.keySet().stream().mapToInt(String::length).max().orElse(10) + 2;
            subCommandDescription.forEach((subcommand, description) -> {
                println("  " + command + " " + subcommand + StringUtils.repeat(' ', maxLength - subcommand.length()) + " " + description);
            });
        }
    }
}
