package org.opencb.opencga.storage.hadoop.app;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.commons.lang3.time.StopWatch;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.core.common.TimeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.stream.Stream;

public abstract class AbstractMain {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractMain.class);

    protected static ObjectMapper objectMapper = new ObjectMapper()
            .configure(MapperFeature.REQUIRE_SETTERS_FOR_GETTERS, true);

    protected static <T> T read(String file, Class<T> type) throws java.io.IOException {
        return objectMapper.readValue(Paths.get(file).toFile(), type);
    }

    protected abstract void run(String[] args) throws Exception;

    protected RuntimeException error(String msg) {
        printError(msg);
        return new IllegalArgumentException(msg);
    }

    protected RuntimeException error(Exception e) {
        printError(e.getMessage());
        return new IllegalArgumentException(e);
    }

    protected void printError(String msg) {
        print(new ObjectMap("error", msg));
    }

    protected void print(final Object obj) {
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
                System.out.println(objectWriter.writeValueAsString(obj));
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

    protected String getArg(String[] args, int i, String def) {
        return args.length > i ? args[i] : def;
    }
    protected String getArg(String[] args, int i) throws Exception {
        if (args.length > i) {
            return args[i];
        } else {
            String msg = "Missing arg " + i;
            printError(msg);
            throw new IllegalArgumentException(msg);
        }
    }

    protected ObjectMap getArgsMap(String[] args, int firstIdx, String... keys) {
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
}
