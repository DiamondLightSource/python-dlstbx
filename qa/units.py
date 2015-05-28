# unit definitions for improved test readability.

## Time (all to seconds)

def milliseconds(ms):
  return seconds(ms / 1000)

def millisecond(ms):
  return milliseconds(ms)

def seconds(s):
  return s

def second(s):
  return seconds(s)

def minutes(m):
  return seconds(m * 60)

def minute(m):
  return minutes(m)

def hours(h):
  return minutes(h * 60)

def hour(h):
  return hours(h)

def days(d):
  return hours(d * 24)

def day(d):
  return days(d)

def weeks(w):
  return days(w * 7)

def week(w):
  return weeks(w)

## From seconds to readable time

def readable_time(t):
  if (t < 1):
    return "less than 1 second"
  if (t < 2):
    return "1 second"
  if (t < 120):
    return "%d seconds" % t
  t = t / 60
  if (t < 120):
    return "%d minutes" % t
  t = t / 60
  if (t < 48):
    return "%d hours" % t
  t = t / 24
  return "%d days" % t
