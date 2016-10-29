import os,sys

from datetime import date,timedelta
import calendar
import math
import collections

QTR_NAMES = {1:'First',2:'Second',3:'Third',4:'Fourth'}

from pyspark.sql import Row,HiveContext
from pyspark import SparkConf,SparkContext

def isWeekEnd(dayname):
    return  'Y' if (dayname == 'Sat') | (dayname == 'Sun') else 'N'

def getCalendarDetails(d):
    stdFmt = "%Y-%m-%d"
    vdateKey = d.year * 10000 + d.month * 100 + d.day
    vDate = d
    vdateStandard = d.strftime(stdFmt)
    vdateStandard1 = d.strftime("%d-%m-%y")
    vdateStandard2 = d.strftime("%m-%d-%y")
    vdateStandard3 = d.strftime("%m%Y")
    vdateStandard4 = calendar.month_abbr[d.month] + "-" + str(d.year)

    vDayofMonth = d.day


    vYear = d.year
    vFirstDayOfYear = date(d.year,1,1).strftime(stdFmt)
    vLastDayOfyear = date(d.year,12,31).strftime(stdFmt)
    vDayOfYear = (d-date(d.year,1,1)).days + 1


    vWeekDay = d.weekday()
    vISOWeekDay = d.isoweekday()
    vWeekOfYear = int(d.strftime("%W"))
    vWeekdayShortName = calendar.day_abbr[d.weekday()]
    vWeekdayFullName = calendar.day_name[d.weekday()]
    visWeekEnd = isWeekEnd(calendar.day_abbr[d.weekday()])


    vCalendarQuarter = int(math.ceil(d.month/3.0))
    vFirstDayOfQuarter = date(d.year,3 * vCalendarQuarter - 2,1).strftime(stdFmt)
    vLastDayOfQuarter = date(d.year,3 * vCalendarQuarter ,calendar.monthrange(d.year,3 * vCalendarQuarter)[1]).strftime(stdFmt)
    vDayOfQuarter = (d-date(d.year,3 * vCalendarQuarter - 2,1)).days
    vQuarterName = QTR_NAMES[int(math.ceil(d.month/3.0))]

    vMonth = d.month
    vFirstDayOfMonth = date(d.year,d.month,1).strftime(stdFmt)
    vLastDayOfMonth = date(d.year,d.month,calendar.monthrange(d.year,d.month)[1]).strftime(stdFmt)
    vMonthShortname = calendar.month_abbr[d.month]
    vMonthFullName = calendar.month_name[d.month]


    details = {}
    details['vdateKey'] = vdateKey
    details['vDate'] = vDate
    details['vdateStandard'] = vdateStandard
    details['vdateStandard1'] = vdateStandard1
    details['vdateStandard2'] = vdateStandard2
    details['vdateStandard3'] = vdateStandard3
    details['vdateStandard4'] = vdateStandard4

    details['vDayofMonth'] = vDayofMonth

    details['vYear'] = vYear
    details['vFirstDayOfYear'] = vFirstDayOfYear
    details['vLastDayOfyear'] = vLastDayOfyear
    details['vDayOfYear'] = vDayOfYear

    details['vWeekdayShortName'] = vWeekdayShortName
    details['vWeekdayFullName'] = vWeekdayFullName
    details['visWeekEnd'] = visWeekEnd

    details['vCalendarQuarter'] = vCalendarQuarter
    details['vFirstDayOfQuarter'] = vFirstDayOfQuarter
    details['vLastDayOfQuarter'] = vLastDayOfQuarter
    details['vDayOfQuarter'] = vDayOfQuarter + 1
    details['vQuarterName'] = vQuarterName

    details['vMonth'] = vMonth
    details['vFirstDayOfMonth'] = vFirstDayOfMonth
    details['vLastDayOfMonth'] = vLastDayOfMonth
    details['vMonthShortname'] = vMonthShortname
    details['vMonthFullName'] = vMonthFullName


    return details
def daterange(start_date, end_date):
    for ordinal in range(start_date.toordinal(), end_date.toordinal()):
        yield date.fromordinal(ordinal)


def dictToRow(d):

    dc = getCalendarDetails(d)
    row = Row(dc.keys)
    for k in dc:
        row(dc[k])
    return row

def main():
    d = date.today()
    t = timedelta(days=50*365)
    start = d-t
    end = d + t
    total = []
    for k in daterange(start,end):
        total.append(k)
    print sys.getsizeof(total)
    conf = SparkConf()
    sc = SparkContext(conf=conf)
    hc = HiveContext(sc)
    rd = sc.parallelize(total).map(getCalendarDetails)
    df = hc.createDataFrame(rd)
    df.printSchema()
    print rd.take(10)
    df.saveAsTable("DIM_DATE")


main()

if "__name__" == "__main__":
    main()
