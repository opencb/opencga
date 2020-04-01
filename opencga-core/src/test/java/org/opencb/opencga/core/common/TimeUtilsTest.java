/*
 * Copyright 2015-2020 OpenCB
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

import junit.framework.TestCase;
import org.junit.Test;

import java.util.Date;

public class TimeUtilsTest extends TestCase {

    @Test
    public void test() {

        Date date = new Date();
        String time = TimeUtils.getTime(date);
        Date date2 = TimeUtils.toDate(time);
        String time2 = TimeUtils.getTime(date2);

        assertEquals(date.getTime() / 1000 * 1000, date2.getTime() );
        assertEquals(time, time2);

    }

    @Test
    public void testMillis() {

        Date date = new Date();
        String time = TimeUtils.getTimeMillis(date);
        Date date2 = TimeUtils.toDate(time);
        String time2 = TimeUtils.getTimeMillis(date2);

        assertEquals(date.getTime(), date2.getTime());
        assertEquals(time, time2);

    }

}