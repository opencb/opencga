package org.opencb.opencga.lib.common;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;

public class TimeUtils {
    public static String getTime() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
        return sdf.format(new Date());
    }

    public static String getTimeMillis() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmssSSS");
        return sdf.format(new Date());
    }

    public static Date add24HtoDate(Date date) {
        Calendar cal = new GregorianCalendar();
        cal.setTime(date);
        cal.setTimeInMillis(date.getTime());// sumamos 24h a la fecha del login
        cal.add(Calendar.DATE, 1);
        return new Date(cal.getTimeInMillis());
    }

    public static Date toDate(String dateStr) {
        Date now = null;
        try {
            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
            now = sdf.parse(dateStr);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return now;
    }
}
