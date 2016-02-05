package com.actian.spark_vectorh.test.util

import java.sql.{ Date => sqlDate, Timestamp }
import java.util.{ Calendar, Date, GregorianCalendar, TimeZone }

object DateHelper {
  def dateFor(year: Int, month: Int, day: Int, hours: Int = 0, minutes: Int = 0, seconds: Int = 0, millis: Int = 0, tz: TimeZone = TimeZone.getDefault): Date = {
    import java.util.Calendar._
    val cal = new GregorianCalendar()
    cal.setTimeZone(tz)
    if (year > 0) cal.set(YEAR, year)
    if (month >= 0) cal.set(MONTH, month)
    if (day > 0) cal.set(DAY_OF_MONTH, day)
    cal.set(HOUR_OF_DAY, hours)
    cal.set(MINUTE, minutes)
    cal.set(SECOND, seconds)
    cal.set(MILLISECOND, millis)
    cal.getTime
  }

  def timestampFor(year: Int, month: Int, day: Int, hours: Int = 0, minutes: Int = 0, seconds: Int = 0, millis: Int = 0, tz: TimeZone = TimeZone.getDefault): Timestamp = {
    new Timestamp(dateFor(year, month, day, hours, minutes, seconds, millis, tz).getTime)
  }

  def timeFor(hours: Int, minutes: Int, seconds: Int): Timestamp = {
    import java.util.Calendar
    val year = Calendar.getInstance().get(Calendar.YEAR)
    val month = Calendar.getInstance().get(Calendar.MONTH)
    val day = Calendar.getInstance().get(Calendar.DATE)
    timestampFor(year, month, day, hours, minutes, seconds)
  }

  def ansiDateFor(year: Int, month: Int, day: Int): sqlDate = new sqlDate(dateFor(year, month, day).getTime())
}
