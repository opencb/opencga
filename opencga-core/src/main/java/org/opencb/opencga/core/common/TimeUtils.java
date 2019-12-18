/*
 * Copyright 2015-2017 OpenCB
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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.crypto.Data;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.concurrent.TimeUnit;

public class TimeUtils {

    private static final String yyyyMMdd = "yyyyMMdd";
    private static final String yyyyMMddHHmmss = "yyyyMMddHHmmss";
    private static final String yyyyMMddHHmmssSSS = "yyyyMMddHHmmssSSS";

    private static final Logger logger = LoggerFactory.getLogger(TimeUtils.class);

    public static String getTime() {
        return getTime(new Date());
    }

    public static Date getDate() {
        return new Date();
    }

    public static String getTime(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat(yyyyMMddHHmmss);
        return sdf.format(date);
    }

    public static String getTimeMillis() {
        return getTimeMillis(new Date());
    }

    public static String getTimeMillis(Date date) {
        SimpleDateFormat sdfMillis = new SimpleDateFormat(yyyyMMddHHmmssSSS);
        return sdfMillis.format(date);
    }

    public static String getDay() {
        return getDay(new Date());
    }

    public static String getDay(Date date) {
        SimpleDateFormat sdf = new SimpleDateFormat(yyyyMMdd);
        return sdf.format(date);
    }

    public static String durationToString(StopWatch stopWatch) {
        return durationToString(stopWatch.getTime());
    }

    public static String durationToString(long duration, TimeUnit timeUnit) {
        return durationToString(timeUnit.toMillis(duration));
    }

    /**
     * Prints a duration in millis as:
     *
     *  1234.5s [ 00:20:34 ]
     *
     * @param durationInMillis Duration in millis
     * @return
     */
    public static String durationToString(long durationInMillis) {
        long durationInSeconds = Math.round(durationInMillis / 1000.0);
        long h = durationInSeconds / 3600;
        long m = (durationInSeconds % 3600) / 60;
        long s = durationInSeconds % 60;
        return (durationInMillis / 1000.0) + "s [ "+ StringUtils.leftPad(String.valueOf(h), 2, '0') + ':'
                + StringUtils.leftPad(String.valueOf(m), 2, '0') + ':'
                + StringUtils.leftPad(String.valueOf(s), 2, '0') + " ]";
    }

    public static Date add24HtoDate(Date date) {
        Calendar cal = new GregorianCalendar();
        cal.setTime(date);
        cal.setTimeInMillis(date.getTime());// sumamos 24h a la fecha del login
        cal.add(Calendar.DATE, 1);
        return new Date(cal.getTimeInMillis());
    }

    public static Date toDate(String dateStr) {
        Date date = null;
        try {
            if (dateStr.length() == yyyyMMddHHmmss.length()) {
                SimpleDateFormat sdf = new SimpleDateFormat(yyyyMMddHHmmss);
                date = sdf.parse(dateStr);
            } else {
                SimpleDateFormat sdfMillis = new SimpleDateFormat(yyyyMMddHHmmssSSS);
                date = sdfMillis.parse(dateStr);
            }
        } catch (ParseException e) {
            logger.warn(e.getMessage());
        }
        return date;
    }

    public static boolean isValidFormat(String format, String value) {
        Date date = null;
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(format);
            date = sdf.parse(value);
            if (!value.equals(sdf.format(date))) {
                date = null;
            }
        } catch (ParseException e) {
            logger.warn(e.getMessage());
        }
        return date != null;
    }
}
