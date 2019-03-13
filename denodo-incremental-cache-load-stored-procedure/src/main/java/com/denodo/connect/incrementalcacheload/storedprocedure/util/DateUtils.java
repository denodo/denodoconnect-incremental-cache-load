package com.denodo.connect.incrementalcacheload.storedprocedure.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class DateUtils {

    private static final SimpleDateFormat DATEFORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    
    public static String millisecondsToStringDate(long milliseconds) {
                
        synchronized (DATEFORMAT) {
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(milliseconds);
            return DATEFORMAT.format(calendar.getTime());
        }
        
    }
    
}
