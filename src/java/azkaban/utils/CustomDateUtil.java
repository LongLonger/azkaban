package azkaban.utils;

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
                    System.out.println("has submit time");
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

    public static void main(String[] args) throws ParseException {
        Props props = new Props();

        String datePtn = "yyyy-MM-dd HH:mm";
        SimpleDateFormat sdf = new SimpleDateFormat(datePtn);


//        customDate(props, sdf.parse("2018-01-01 08:24").getTime());
        customDate(props, null);
    }
}
