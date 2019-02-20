import datetime
from dateutil.relativedelta import relativedelta
import calendar


def timestamp_by_age(age):
    value = 0
    if type(age).__name__ == 'str':
        value = int(age)
    else:
        value = age

    now = datetime.datetime(2017, 8, 29, 7, 30) - relativedelta(years=value)
    return calendar.timegm(now.timetuple())


def raise_(err):
    raise err