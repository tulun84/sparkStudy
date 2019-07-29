package structuredStreaming.sink.util;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class TimeUtils {
    private static Logger logger = LoggerFactory.getLogger(TimeUtils.class);
    public static final String YEAR = "YEAR";
    public static final String MONTH = "MONTH";
    public static final String DAY = "DAY";
    public static final String HOUR = "HOUR";
    public static final String MINUTE = "MINUTE";
    public static final String SESCOND = "SESCOND";
    public static final String MILLISECOND = "MILLISECOND";
    public static final String WEEK = "WEEK";

    public static String now(String pattern) {
        Date date = new Date();
        SimpleDateFormat df = new SimpleDateFormat(pattern == null ? "yyyyMMddHHmmssSSS" : pattern);
        String formatTime = df.format(date);
        return formatTime;
    }

    public static String timeStemp2DateStr(Long ts, String pattern) {
        Date date = new Date(ts);
        SimpleDateFormat df = new SimpleDateFormat(pattern == null ? "yyyyMMddHHmmssSSS" : pattern);
        String formatTime = df.format(date);
        return formatTime;
    }

    public static String date2DateStr(Date date, String pattern) {
        SimpleDateFormat df = new SimpleDateFormat(pattern == null ? "yyyyMMddHHmmssSSS" : pattern);
        String formatTime = df.format(date);
        return formatTime;
    }

    public static String dateStr2NewDateStr(String oldDataStr, String oldPattern, String newPattern) {
        Date date = dateStr2Date(oldDataStr, oldPattern);
        String newDateStr = date2DateStr(date, newPattern);
        return newDateStr;
    }

    public static long dateStr2TimeStemp(String dateStr, String pattern) {
        Date date = null;
        try {
            SimpleDateFormat df = new SimpleDateFormat(pattern == null ? "yyyyMMddHHmmssSSS" : pattern);
            date = df.parse(dateStr);
        } catch (ParseException e) {
            logger.error(e.getMessage(), e);
        }
        return date.getTime();
    }

    public static Date dateStr2Date(String dateStr, String pattern) {
        Date date = null;
        try {
            SimpleDateFormat df = new SimpleDateFormat(pattern == null ? "yyyyMMddHHmmssSSS" : pattern);
            date = df.parse(dateStr);
        } catch (ParseException e) {
            logger.error(e.getMessage(), e);
        }
        return date;
    }

    /**
     * 计算时间处于当月的第几周
     *
     * @param str 时间格式:"yyyy-MM-dd"
     * @return
     * @throws Exception
     */
    public static int getWeekOfMonth(String str, String pattern) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern == null ? "yyyy-MM-dd" : pattern);
        Date date = sdf.parse(str);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        //第几周
        int week = calendar.get(Calendar.WEEK_OF_MONTH);
        return week;
    }

    /**
     * 计算时间处于当周的第几天
     *
     * @param str 时间格式:"yyyy-MM-dd"
     * @return
     * @throws Exception
     */
    public static int getDayOfWeek(String str, String pattern) throws Exception {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern == null ? "yyyy-MM-dd" : pattern);
        Date date = sdf.parse(str);
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        //第几天，从周日开始
        int day = calendar.get(Calendar.DAY_OF_WEEK);
        return day;
    }

    /**
     * 计算时间处于当周的第几天
     *
     * @param str 时间格式:"yyyy-MM-dd"
     * @return
     * @throws Exception
     */
    public static int getWeekOfYear(String str, String pattern) {
        int week_of_year = 0;
        try {
            SimpleDateFormat sdf = new SimpleDateFormat(pattern == null ? "yyyy-MM-dd" : pattern);
            Date date = sdf.parse(str);
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(date);
            //当年的第几周
            week_of_year = calendar.get(Calendar.WEEK_OF_YEAR);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return week_of_year;
    }

    public static long addTime(String timeStr, String pattern, String addTimeType, int amount) {
        Date date = TimeUtils.dateStr2Date(timeStr, pattern == null ? "yyyyMMddHHmmssSSS" : pattern);
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        if ("YEAR".equals(addTimeType)) {
            cal.add(Calendar.YEAR, amount);
        } else if ("MONTH".equals(addTimeType)) {
            cal.add(Calendar.MONTH, amount);
        } else if ("DAY".equals(addTimeType)) {
            cal.add(Calendar.DAY_OF_MONTH, amount);
        } else if ("HOUR".equals(addTimeType)) {
            cal.add(Calendar.HOUR_OF_DAY, amount);
        } else if ("MINUTE".equals(addTimeType)) {
            cal.add(Calendar.MINUTE, amount);
        } else if ("SESCOND".equals(addTimeType)) {
            cal.add(Calendar.SECOND, amount);
        } else if ("millisecond".equals(addTimeType)) {
            cal.add(Calendar.MILLISECOND, amount);
        } else if ("WEEK".equals(addTimeType)) {
            cal.add(Calendar.WEEK_OF_MONTH, amount);
        }
        long endTimeInMillis = cal.getTimeInMillis();
        return endTimeInMillis;
    }

    public static long addTime(long ts, String addTimeType, int amount) {
        Date date = new Date(ts);
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        if ("YEAR".equals(addTimeType)) {
            cal.add(Calendar.YEAR, amount);
        } else if ("MONTH".equals(addTimeType)) {
            cal.add(Calendar.MONTH, amount);
        } else if ("DAY".equals(addTimeType)) {
            cal.add(Calendar.DAY_OF_MONTH, amount);
        } else if ("HOUR".equals(addTimeType)) {
            cal.add(Calendar.HOUR_OF_DAY, amount);
        } else if ("MIMUE".equals(addTimeType)) {
            cal.add(Calendar.MINUTE, amount);
        } else if ("SESCOND".equals(addTimeType)) {
            cal.add(Calendar.SECOND, amount);
        } else if ("millisecond".equals(addTimeType)) {
            cal.add(Calendar.MILLISECOND, amount);
        } else if ("WEEK".equals(addTimeType)) {
            cal.add(Calendar.WEEK_OF_MONTH, amount);
        }
        long endTimeInMillis = cal.getTimeInMillis();
        return endTimeInMillis;
    }

    /**
     * 获取指定日期所在周的第一天：
     */
    public static String getFirstDayOfWeek(String timeStr, String patternSource, String patternTarget) {
        Date date = TimeUtils.dateStr2Date(timeStr, patternSource);
        Calendar cal = Calendar.getInstance();
        String firstDayStr = "";
        try {
            cal.setTime(date);
            //此处需求是一周的第一天按中国习惯，取周一
            cal.set(Calendar.DAY_OF_WEEK, 2);
            Date firstDay = cal.getTime();
            firstDayStr = date2DateStr(firstDay, patternTarget);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return firstDayStr;
    }

    /**
     * 获取指定日期所在周的最后一天：
     */
    public static String getLastDayOfWeek(String timeStr, String patternSource, String patternTarget) {
        Date date = TimeUtils.dateStr2Date(timeStr, patternSource);
        Calendar cal = Calendar.getInstance();
        String lastDayStr = "";
        try {
            cal.setTime(date);
            //此处需求是一周的第一天按中国习惯，取周一
            cal.set(Calendar.DAY_OF_WEEK, 2);
            cal.set(Calendar.DATE, cal.get(Calendar.DATE) + 6);
            Date lastDay = cal.getTime();
            lastDayStr = date2DateStr(lastDay, patternTarget);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return lastDayStr;
    }

    /**
     * 获取指定日期所在月的第一天：
     */
    public static String getFirstDayOfMonth(String timeStr, String patternSource, String patternTarget) {
        Date date = TimeUtils.dateStr2Date(timeStr, patternSource);
        Calendar cal = Calendar.getInstance();
        String firstDayStr = "";
        try {
            cal.setTime(date);
            cal.add(Calendar.MONTH, 0);
            cal.set(Calendar.DAY_OF_MONTH, 1);
            Date firstDay = cal.getTime();
            firstDayStr = date2DateStr(firstDay, patternTarget);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return firstDayStr;
    }

    /**
     * 获取指定日期所在月的最后一天：
     */
    public static String getLastDayOfMonth(String timeStr, String patternSource, String patternTarget) {
        Date date = TimeUtils.dateStr2Date(timeStr, patternSource);
        Calendar cal = Calendar.getInstance();
        String firstDayStr = "";
        try {
            cal.setTime(date);
            cal.add(Calendar.MONTH, 1);
            cal.set(Calendar.DAY_OF_MONTH, 0);
            Date firstDay = cal.getTime();
            firstDayStr = date2DateStr(firstDay, patternTarget);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return firstDayStr;
    }

    @Test
    public void testWeek() throws Exception {
        long yyyyMMdd = addTime("20181231", "yyyyMMdd", MONTH, -1);
        String yyyyMMdd2 = timeStemp2DateStr(yyyyMMdd, "yyyyMMdd");
        System.out.println(yyyyMMdd2);
        int yyyyMMdd1 = getWeekOfYear("20190105", "yyyyMMdd");
        System.out.println(yyyyMMdd1);
    }

    public static double getDifferenceTime(String time1, String time2, String pattern, String backType) {
        long ts1 = dateStr2TimeStemp(time1, pattern);
        long ts2 = dateStr2TimeStemp(time2, pattern);
        double tsDiff = ts1 - ts2;
        double diffTime = 0;
        if (backType.equalsIgnoreCase(DAY)) {
            diffTime = doubleFormat(tsDiff / (1000 * 3600 * 24), "#.000");
        } else if (backType.equalsIgnoreCase(HOUR)) {
            diffTime = doubleFormat(tsDiff / (1000 * 3600), "#.000");
        } else if (backType.equalsIgnoreCase(MINUTE)) {
            diffTime = doubleFormat(tsDiff / (1000 * 60), "#.000");
        } else if (backType.equalsIgnoreCase(SESCOND)) {
            diffTime = doubleFormat(tsDiff / (1000), "#.000");
        } else if (backType.equalsIgnoreCase(MILLISECOND)) {
            diffTime = tsDiff;
        } else {
            diffTime = tsDiff;
        }
        return diffTime;
    }

    public static double doubleFormat(double doubleNumber, String pattern) {
        DecimalFormat df = new DecimalFormat(pattern == null ? "#.00" : pattern);
        Double format = Double.valueOf(df.format(doubleNumber));
        return format;
    }

    public static void main(String[] args) {
        String firstDayOfWeek = getFirstDayOfWeek("2019-01-09", "yyyy-MM-dd", "yyyy-MM-dd");
        String lastDayOfWeek = getLastDayOfWeek("2019-01-09", "yyyy-MM-dd", "yyyy-MM-dd");
        String firstDayOfMonth = getFirstDayOfMonth("201901", "yyyyMM", "yyyy-MM-dd");
        String lastDayOfMonth = getLastDayOfMonth("201901", "yyyyMM", "yyyy-MM-dd");
        int weekOfYear = getWeekOfYear("20181013", "yyyyMMdd");


        long ts1 = addTime("20180101000000000", null, YEAR, 1);
        long ts2 = addTime("20180101000000000", null, MONTH, 1);
        long ts3 = addTime("20180101000000000", null, DAY, 1);
        long ts4 = addTime("20180101000000000", null, HOUR, 47);
        long ts5 = addTime("20180101000000000", null, MINUTE, 1);
        long ts6 = addTime("20180101000000000", null, SESCOND, 1);
        long ts7 = addTime("20180101000000000", null, MILLISECOND, 1);
        long ts8 = addTime("20180101000000000", null, WEEK, 1);
        String s1 = timeStemp2DateStr(ts1, null);
        String s2 = timeStemp2DateStr(ts2, null);
        String s3 = timeStemp2DateStr(ts3, null);
        String s4 = timeStemp2DateStr(ts4, null);
        String s5 = timeStemp2DateStr(ts5, null);
        String s6 = timeStemp2DateStr(ts6, null);
        String s7 = timeStemp2DateStr(ts7, null);
        String s8 = timeStemp2DateStr(ts8, null);
        System.out.println(s1);
        System.out.println(s2);
        System.out.println(s3);
        System.out.println(s4);
        System.out.println(s5);
        System.out.println(s6);
        System.out.println(s7);
        System.out.println(s8);
    }
//    YEAR,MONTH,DAY,HOUR,MIMUE,SESCOND,millisecond,WEEK


}
