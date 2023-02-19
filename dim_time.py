

from datetime import timedelta,datetime

def getTimeDetails(d):
    vTimeKey = d.strftime("%H%M%S")
    vHour = d.hour
    vHour24 = d.strftime("%H")
    vHour24MinString = d.strftime("%H") + ":00"
    vHour24FullString = d.strftime("%H") + ":00:00"
    vHour12ShortString = d.strftime("%I")
    vHour12MinString = d.strftime("%I") + ":00"
    vHour12FullString = d.strftime("%I") + ":00:00"
    vAmPmString = d.strftime("%p")
    vMinute = d.minute
    vMinuteCode = d.strftime("%H%M")
    vMinuteShortString = d.strftime("%M")
    vMinuteFullString24 = d.strftime("%H:%M") + ":00"
    vMinuteFullString12 = d.strftime("%I:%M") + ":00"
    vSecond = d.second
    vSecondShortString = d.strftime("%S")
    vFullTimeString24 = d.strftime("%H:%M:%S")
    vFullTimeString12 = d.strftime("%I:%M:%S")


    details = {}
    details['vTimeKey'] = vTimeKey
    details['vHour24'] = vHour24
    details['vHour'] = vHour
    details['vHour24MinString'] = vHour24MinString
    details['vHour24FullString'] = vHour24FullString
    details['vHour12ShortString'] = vHour12ShortString
    details['vHour12MinString'] = vHour12MinString
    details['vHour12FullString'] = vHour12FullString
    details['vAmPmString'] = vAmPmString
    details['vMinute'] = vMinute
    details['vMinuteCode'] = vMinuteCode
    details['vMinuteShortString'] = vMinuteShortString
    details['vMinuteFullString24'] = vMinuteFullString24
    details['vMinuteFullString12'] = vMinuteFullString12
    details['vSecondShortString'] = vSecondShortString
    details['vFullTimeString24'] = vFullTimeString24
    details['vFullTimeString12'] = vFullTimeString12
    return details


def main():
    start = datetime(1900,1,1,0,0,0)
    end = start + timedelta(days=1)
    total = []
    while start < end:
        t = getTimeDetails(start)
        total.append(t)
        start = start + timedelta(seconds = 1)
    
    with open("dim_time.txt","w") as fo:
        header = total[0].keys()
        fo.write(",".join(header))
        fo.write("\n")
        for t in total:
            v = t.values()
            fo.write(",".join([str(x) for x in v]))
            fo.write("\n")


if __name__ == '__main__':
    main()
