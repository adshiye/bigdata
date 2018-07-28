package com.adshiye.bigdata.util;

import com.thinkive.base.util.DateHelper;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.UUID;

public class DateUtil extends DateHelper

{
	public final static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

	//long to String(年月日)
	public static String long2String(long timestamp){
		Date date=new Date(timestamp);
		return sdf.format(date);
	}

	//long to String(时分秒)
	public static String long2hms(long timestamp){
		SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss");
		Date date=new Date(timestamp);
		return sdf.format(date);
	}


	public static Long toLongDate(Date date) {
		Long result = null;
		String strTime = formatDate(date, "yyyy-MM-dd");
		result = Long.parseLong(strTime);
		return result;
	}

	public static int getYear(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar.get(Calendar.YEAR);
	}

	public static int getMonth(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar.get(Calendar.MONTH) + 1;
	}

	/**
	 *
	 * 描述：获取一年中的第几周
	 *
	 * @author lenovo
	 * @created 2016年9月20日 上午11:04:51
	 * @since
	 * @param date
	 * @return
	 * @throws Exception
	 */
	public static int getWeek(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		// 第几周
		int week = calendar.get(Calendar.WEEK_OF_YEAR);
		return week;
	}

	public static int getHour(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar.get(Calendar.HOUR_OF_DAY);
	}

	public static int getMinute(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		return calendar.get(Calendar.MINUTE);
	}

	/**
	 * 描述：获取某年的第几周的结束日期
	 *
	 * @author WangZeyu
	 * @created 2016年5月25日 下午6:07:54
	 * @since
	 * @param year
	 * @param week
	 * @return
	 */
	public static String getLastDayOfWeek(int year, int week) {
		Calendar c = new GregorianCalendar();
		c.set(Calendar.YEAR, year);
		c.set(Calendar.MONTH, Calendar.JANUARY);
		c.set(Calendar.DATE, 1);

		Calendar cal = (GregorianCalendar) c.clone();
		cal.add(Calendar.DATE, (week - 1) * 7);

		return getLastDayOfWeek(cal.getTime());
	}

	/**
	 * 描述：获取某年的第几周的开始日期
	 *
	 * @author WangZeyu
	 * @created 2016年5月25日 下午6:06:27
	 * @since
	 * @param year
	 * @param week
	 * @return
	 */
	public static String getFirstDayOfWeek(int year, int week) {
		Calendar c = new GregorianCalendar();
		c.set(Calendar.YEAR, year);
		c.set(Calendar.MONTH, Calendar.JANUARY);
		c.set(Calendar.DATE, 1);

		Calendar cal = (GregorianCalendar) c.clone();
		cal.add(Calendar.DATE, (week - 1) * 7);

		return getFirstDayOfWeek(cal.getTime());
	}

	/**
	 * 描述：获取某年某月第一天
	 *
	 * @author WangZeyu
	 * @created 2016年5月25日 下午7:56:52
	 * @since
	 * @param year
	 * @param month
	 * @return
	 */
	public static Date getFirstDayOfMonth(int year, int month) {
		Calendar c = new GregorianCalendar();
		c.set(Calendar.YEAR, year);
		c.set(Calendar.MONTH, month - 1);
		c.set(Calendar.DATE, 1);
		Calendar cal = (GregorianCalendar) c.clone();
		return cal.getTime();
	}

	public static Date getFirstDayOfMonth(Date date) {
		int year = getYear(date);
		int month = getMonth(date);
		return getFirstDayOfMonth(year, month);
	}

	/**
	 * 描述：获取某年某月第一天
	 *
	 * @author WangZeyu
	 * @created 2016年5月25日 下午7:56:52
	 * @since
	 * @param year
	 * @param month
	 * @return
	 */
	public static Date getLastDayOfMonth(int year, int month) {
		Calendar c = new GregorianCalendar();
		c.set(Calendar.YEAR, year);
		c.set(Calendar.MONTH, month);
		c.set(Calendar.DATE, 1);
		Calendar cal = (GregorianCalendar) c.clone();
		cal.add(Calendar.DATE, -1);
		return cal.getTime();
	}

	/**
	 * 描述：获取当前时间所在周的开始日期
	 *
	 * @author WangZeyu
	 * @created 2016年5月25日 下午6:06:07
	 * @since
	 * @param date
	 * @return
	 */
	public static String getFirstDayOfWeek(Date date) {
		Calendar c = new GregorianCalendar();
		c.setFirstDayOfWeek(Calendar.MONDAY);
		c.setTime(date);
		c.set(Calendar.DAY_OF_WEEK, c.getFirstDayOfWeek()); // Monday
		return sdf.format(c.getTime());
	}


	/**
	 * 描述：获取当前时间所在周的结束日期
	 *
	 * @author WangZeyu
	 * @created 2016年5月25日 下午6:06:17
	 * @since
	 * @param date
	 * @return
	 */
	public static String getLastDayOfWeek(Date date) {
		Calendar c = new GregorianCalendar();
		c.setFirstDayOfWeek(Calendar.MONDAY);
		c.setTime(date);
		c.set(Calendar.DAY_OF_WEEK, c.getFirstDayOfWeek() + 6); // Sunday
		return sdf.format(c.getTime());
	}

	public static Date addDate(Date date, int days) {
		Calendar c = new GregorianCalendar();
		c.setTime(date);
		c.add(Calendar.DATE, days);
		return c.getTime();
	}

	public static void main(String[] args) throws Exception {
		long current = System.currentTimeMillis();
		Date d = new Date(current);
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHH");
		String e = sdf.format(d);
		String uuid = UUID.randomUUID().toString().replaceAll("-", "");
		String newFileName=e+"_"+uuid;
		System.out.println(newFileName);
	}
}
