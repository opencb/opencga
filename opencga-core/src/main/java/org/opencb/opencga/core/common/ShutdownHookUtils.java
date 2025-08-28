package org.opencb.opencga.core.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShutdownHookUtils {

    private static Logger logger = LoggerFactory.getLogger(ShutdownHookUtils.class);

    @FunctionalInterface
    public interface CallableWithException<T, E extends Exception> {
        T call() throws E;
    }

    @FunctionalInterface
    public interface RunnableWithException<E extends Exception> {
        void run() throws E;
    }

    public static <E extends Exception> void run(RunnableWithException<E> runnable, Runnable onError) throws E {
        run(() -> {
            runnable.run();
            return null;
        }, onError::run);
    }

    public static <T, E extends Exception> T run(CallableWithException<T, E> callable, RunnableWithException<E> onError)
            throws E {
        Thread hook = new Thread(() -> {
            try {
                logger.info("Shutdown hook triggered");
                onError.run();
            } catch (Exception e) {
                logger.error("Error executing shutdown hook", e);
                throw new RuntimeException(e);
            }
        });
        try {
            Runtime.getRuntime().addShutdownHook(hook);
            return callable.call();
        } catch (Exception e) {
            try {
                logger.warn("Exception caught, executing onError callback : " + e);
                onError.run();
            } catch (Exception ex) {
                logger.error("Error executing onError callback", ex);
                e.addSuppressed(ex);
            }
            throw e;
        } finally {
            Runtime.getRuntime().removeShutdownHook(hook);
        }
    }

}
