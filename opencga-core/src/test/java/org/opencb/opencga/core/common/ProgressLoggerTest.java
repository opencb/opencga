package org.opencb.opencga.core.common;

import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created on 07/07/16
 *
 * @author Jacobo Coll &lt;jacobo167@gmail.com&gt;
 */
public class ProgressLoggerTest {

    @Test
    public void test() {
        int totalCount = 21111;
        final AtomicInteger prints = new AtomicInteger(0);
        int numLinesLog = 13;
        ProgressLogger progressLogger = new ProgressLogger("Message", totalCount, numLinesLog) {
            @Override
            protected void print(String m) {
                super.print(m);
                prints.incrementAndGet();
            }
        };

        int increment = 1;
        for (int i = 0; i < totalCount; i+= increment) {
            progressLogger.increment(increment);
        }

        Assert.assertEquals(numLinesLog, prints.get());
    }

}