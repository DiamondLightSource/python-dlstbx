import colorama
import json
import signal
import sys
import time
import urllib

def load_status():
  baseurl = "http://jenkins.diamond.ac.uk:8080"
  view = "/view/DIALS-monitor"
  api = "/api/json?tree="
  selector = "jobs[name,displayName,lastBuild[result,building,executor[likelyStuck,progress]],lastCompletedBuild[result,actions[failCount]]]"

  url = baseurl + view + api + selector
  json_data = json.loads(urllib.urlopen(url).read())
  return { job['displayName']: job for job in json_data['jobs'] }

def write_status(status, blink=False):
  jobnames = sorted(status.keys())

  for job in jobnames:
    jobcolor = colorama.Style.RESET_ALL
    if status[job].get('lastCompletedBuild') is not None:
      if status[job]['lastCompletedBuild']['result'] == 'SUCCESS':
        jobcolor += colorama.Fore.GREEN
      elif status[job]['lastCompletedBuild']['result'] == 'UNSTABLE':
        jobcolor += colorama.Fore.YELLOW
      elif status[job]['lastCompletedBuild']['result'] == 'FAILURE':
        jobcolor += colorama.Fore.RED
      else:
        raise Exception("unknown build status %s" % status[job]['lastCompletedBuild']['result'])
    if status[job].get('lastBuild',{}).get('building', False):
      jobcolor += colorama.Style.BRIGHT
  
    progress = ""
    if status[job].get('lastBuild',{}).get('building', False):
      progcolor = colorama.Style.RESET_ALL
      if status[job].get('lastBuild',{}).get('executor',{}).get('likelyStuck',False):
        progcolor += colorama.Fore.RED
      else:
        progcolor += colorama.Fore.BLUE
      p = status[job].get('lastBuild',{}).get('executor',{}).get('progress')
      if p is not None:
        p = max(0, int(p/5)-1)
        progress = progcolor + " [%-20s]" % (p * "=" + ">")

    teststatus = ""
    if status[job].get('lastCompletedBuild',{}).get('result') == 'UNSTABLE':
      actions = status[job].get('lastCompletedBuild',{}).get('actions', [])
      testfails = 0
      for row in actions:
        if isinstance(row, dict) and 'failCount' in row:
          testfails = row['failCount']
      teststatus = colorama.Fore.YELLOW
      if not status[job].get('lastBuild',{}).get('building', False):
        teststatus += colorama.Style.BRIGHT
      teststatus += " (%d test%s failing)" % (testfails, "s" if testfails != 1 else "")

    clear_line()
    sys.stdout.write(jobcolor)
    if status[job].get('lastBuild',{}).get('building', False):
      if blink:
        sys.stdout.write(unichr(9642).encode('utf-8'))
        sys.stdout.write(unichr(9643).encode('utf-8'))
      else:
        sys.stdout.write(unichr(9643).encode('utf-8'))
        sys.stdout.write(unichr(9642).encode('utf-8'))
    else:
      sys.stdout.write(unichr(9642).encode('utf-8'))
      sys.stdout.write(unichr(9642).encode('utf-8'))
    print " " + job + progress + teststatus

def clear_screen():
  sys.stdout.write("\x1B[2J")

def reset_cursor():
  sys.stdout.write("\x1B[1;1H")

def clear_line():
  sys.stdout.write("\x1B[2K")

# capture window resize events
window_resize = False
def resizeHandler(signum, frame):
  global window_resize
  window_resize = True
signal.signal(signal.SIGWINCH, resizeHandler)

def updateHandler(signum, frame):
  global iteration, prev_status, window_resize
  status = load_status()
  if window_resize or prev_status.keys() != status.keys():
    clear_screen()
    window_resize=False
  prev_status = status
  reset_cursor()
  write_status(status, blink=(iteration % 2) > 0)
  print "%s   [%d]" % (colorama.Style.RESET_ALL + colorama.Style.DIM, iteration)
  iteration = iteration + 1
  assert iteration < 24*3600

colorama.init()

status = load_status()
if True:
  clear_screen()
  reset_cursor()
write_status(status)
prev_status = status

iteration = 1

if True:
  signal.signal(signal.SIGALRM, updateHandler)
  signal.setitimer(signal.ITIMER_REAL, 1, 1)
  while True:
    time.sleep(3)
