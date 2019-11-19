package com.denodo.connect.incrementalcacheload.storedprocedure.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtils {

    private static final SimpleDateFormat DATEFORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private static final SimpleDateFormat TIMEFORMAT = new SimpleDateFormat("HH:mm:ss");

    public static String millisecondsToStringDate(long milliseconds) {
                
        synchronized (DATEFORMAT) {
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(milliseconds);
            return DATEFORMAT.format(calendar.getTime());
        }
    }

    public static String millisecondsToStringTime(long milliseconds) {
        synchronized (TIMEFORMAT) {
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(milliseconds);
            calendar.set(Calendar.YEAR, 1900);
            calendar.set(Calendar.MONTH, 0);
            calendar.set(Calendar.DAY_OF_MONTH, 1);
            return TIMEFORMAT.format(calendar.getTime());
        }
    }

    public static Date stringToDate(String value) throws ParseException {
        synchronized (DATEFORMAT) {
            return DATEFORMAT.parse(value);
        }
    }

    public static SimpleDateFormat getDateFormat() {
        return DATEFORMAT;
    }

    public static SimpleDateFormat getTimeFormat() {
        return TIMEFORMAT;
    }

}
