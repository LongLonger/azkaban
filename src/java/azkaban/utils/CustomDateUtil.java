package azkaban.utils;

import azkaban.executor.ExecutableNode;
import azkaban.flow.CommonJobProperties;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * @author zhongshu
 * @since 2018/1/3 下午6:40
 */
public class CustomDateUtil {

    public static void customDate(Props props, Long submitTime) {
        try {
            Calendar cal = Calendar.getInstance();

            try {
                System.out.printf("submitTime=" + submitTime);
                if (null != submitTime) {
                    cal.setTimeInMillis(submitTime);
                    System.out.println("has submit time");//zhongshu-comment 如果没有submitTime的话即不是重跑，那就使用customTimeFlag标识的那个时间
                } else {
                    System.out.printf("no submit time");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMdd");
            SimpleDateFormat sdf2 = new SimpleDateFormat("yyyyMMddHH");
            SimpleDateFormat sdf3 = new SimpleDateFormat("yyyyMMddHHmm");

            props.put(CommonJobProperties.CUSTOM_DAY, sdf1.format(cal.getTime()));
            props.put(CommonJobProperties.CUSTOM_HOUR, sdf2.format(cal.getTime()));
            props.put(CommonJobProperties.CUSTOM_MINUTE, sdf3.format(cal.getTime()));

            cal.add(Calendar.HOUR_OF_DAY, -1);
            props.put(CommonJobProperties.CUSTOM_LAST_HOUR, sdf2.format(cal.getTime()));

            cal.add(Calendar.DATE, -1);
            props.put(CommonJobProperties.CUSTOM_LAST_DAY, sdf1.format(cal.getTime()));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void customTime(Props props, Long submitTime, String customTimeFlag) throws Exception {
        try {
            Calendar cal = Calendar.getInstance();
            Calendar anotherCal = Calendar.getInstance();

            try {
                System.out.println("submitTime=" + submitTime);
                if (null != submitTime) {
                    cal.setTimeInMillis(submitTime);
                    System.out.println("has submit time");//zhongshu-comment 如果没有submitTime的话即不是重跑，那就使用customTimeFlag标识的那个时间

                    /*
                    todo submitTime是当时提交任务的时间，而不是跑任务的时间
                    跑任务的时间还是要根据customTimeFlag来判断具体用哪个时间
                    那我应该要在executions_flow表加一个custom_time_flag字段咯？？
                     */


                } else {
                    System.out.println("no submit time");
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            SimpleDateFormat dayFormat = new SimpleDateFormat("yyyyMMdd");
            SimpleDateFormat hourFormat = new SimpleDateFormat("yyyyMMddHH");
            SimpleDateFormat minuteFormat = new SimpleDateFormat("yyyyMMddHHmm");

            if (ExecutableNode.CUSTOM_LAST_HOUR.equals(customTimeFlag)) {

                cal.add(Calendar.HOUR_OF_DAY, -1);
                props.put(CommonJobProperties.CUSTOM_TIME, hourFormat.format(cal.getTime()));

            } else if (ExecutableNode.CUSTOM_LAST_DAY.equals(customTimeFlag)) {

                cal.add(Calendar.DATE, -1);
                props.put(CommonJobProperties.CUSTOM_TIME, dayFormat.format(cal.getTime()));

            } else if (ExecutableNode.CUSTOM_HOUR.equals(customTimeFlag)) {

                props.put(CommonJobProperties.CUSTOM_TIME, hourFormat.format(cal.getTime()));

            } else if (ExecutableNode.CUSTOM_DAY.equals(customTimeFlag)) {

                props.put(CommonJobProperties.CUSTOM_TIME, dayFormat.format(cal.getTime()));

            } else {
                throw new Exception("unknown customTimeFlag: " + customTimeFlag);
            }


            props.put(CommonJobProperties.CUSTOM_DAY, dayFormat.format(anotherCal.getTime()));
            props.put(CommonJobProperties.CUSTOM_HOUR, hourFormat.format(anotherCal.getTime()));
            props.put(CommonJobProperties.CUSTOM_MINUTE, minuteFormat.format(anotherCal.getTime()));

            anotherCal.add(Calendar.HOUR_OF_DAY, -1);
            props.put(CommonJobProperties.CUSTOM_LAST_HOUR, hourFormat.format(anotherCal.getTime()));

            anotherCal.add(Calendar.DATE, -1);
            props.put(CommonJobProperties.CUSTOM_LAST_DAY, dayFormat.format(anotherCal.getTime()));

        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }

    }

    public static void main(String[] args) throws ParseException {
        Props props = new Props();

        String datePtn = "yyyy-MM-dd HH:mm";
        SimpleDateFormat sdf = new SimpleDateFormat(datePtn);


//        customDate(props, sdf.parse("2018-01-01 08:24").getTime());
        customDate(props, null);
    }
}
