/*
 * Copyright 2015-2016 OpenCB
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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