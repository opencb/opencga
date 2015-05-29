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