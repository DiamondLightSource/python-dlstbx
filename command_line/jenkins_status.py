import colorama
import json
import signal
import sys
import threading
import time
import urllib

class Jenkins():
  def __init__(self):
    colorama.init()
    self._update_lock = threading.Lock()
    self._update_cache = None
    self._update_cache_expire = 0

    self._redraw_lock = threading.Lock()
    self._resize_detected = True
    self._update_required = False

  def load_status(self):
    baseurl = "http://jenkins.diamond.ac.uk:8080"
    view = "/view/DIALS-monitor"
    api = "/api/json?tree="
    selector = "jobs[name,displayName,buildable,inQueue,lastBuild[result,building,executor[likelyStuck,progress]],lastCompletedBuild[result,timestamp,duration,actions[failCount]],healthReport[score]]"

    url = baseurl + view + api + selector
    with self._update_lock:
      if self._update_cache_expire < time.time():
        self._update_cache = json.loads(urllib.urlopen(url).read())
        self._update_cache_expire = time.time() + 5 # cache status
      return { job['displayName']: job for job in self._update_cache['jobs'] }

  def write_status(self, blink=False):
    status = self._status
    jobnames = sorted(status.keys())

    self._redraw_lock.acquire()

    for job in jobnames:
      disabled = not status[job].get('buildable', True)
      for ensure_not_none in ('lastBuild', 'lastCompletedBuild'):
        if status[job].get(ensure_not_none,{}) is None:
          del(status[job][ensure_not_none])
      building = status[job].get('lastBuild',{}).get('building', False)
      recent = time.time() - (status[job].get('lastCompletedBuild',{}).get('timestamp', 0) + status[job].get('lastCompletedBuild',{}).get('duration', 0)) / 1000 < 180
      queued = status[job].get('inQueue', False)

      jobcolor = colorama.Style.RESET_ALL
      health, healthcolor, healthsymbol = None, "", " "
      if status[job].get('healthReport') is not None:
        scores = [ hr['score'] for hr in status[job].get('healthReport') if 'score' in hr ]
        if scores:
          health = min(scores)
      if health is not None:
        if health >= 100:
          healthcolor = colorama.Fore.GREEN
          healthsymbol = unichr(10004).encode('utf-8')
        elif health > 0:
          healthcolor = colorama.Fore.YELLOW
          healthsymbol = unichr(10008).encode('utf-8')
        else:
          healthcolor = colorama.Fore.RED
          healthsymbol = unichr(10008).encode('utf-8')

      if status[job].get('lastCompletedBuild') is not None:
        if status[job]['lastCompletedBuild']['result'] == 'SUCCESS':
          jobcolor += colorama.Fore.GREEN
        elif status[job]['lastCompletedBuild']['result'] == 'UNSTABLE':
          jobcolor += colorama.Fore.YELLOW
        elif status[job]['lastCompletedBuild']['result'] == 'FAILURE':
          jobcolor += colorama.Fore.RED
        elif status[job]['lastCompletedBuild']['result'] == 'ABORTED':
          jobcolor += colorama.Fore.CYAN
        else:
          raise Exception("unknown build status %s" % status[job]['lastCompletedBuild']['result'])
      elif health is not None:
        jobcolor += healthcolor
  
      progress = ""
      if building:
        progcolor = colorama.Style.RESET_ALL
        if status[job].get('lastBuild',{}).get('executor',{}).get('likelyStuck',False):
          progcolor += colorama.Fore.RED + colorama.Style.BRIGHT
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
        if not building:
          teststatus += colorama.Style.BRIGHT
        teststatus += " (%d test%s failing)" % (testfails, "s" if testfails != 1 else "")

      self.clear_line()
      sys.stdout.write(jobcolor + colorama.Style.BRIGHT)
      if disabled:
        sys.stdout.write('xx')
      elif building:
        if blink:
          sys.stdout.write(unichr(9632).encode('utf-8'))
          sys.stdout.write(unichr(9633).encode('utf-8'))
        else:
          sys.stdout.write(unichr(9633).encode('utf-8'))
          sys.stdout.write(unichr(9632).encode('utf-8'))
      elif queued:
        sys.stdout.write(unichr(9723).encode('utf-8'))
        sys.stdout.write(unichr(9723).encode('utf-8'))
      elif recent:
        sys.stdout.write(unichr(9724).encode('utf-8'))
        sys.stdout.write(unichr(9724).encode('utf-8'))
      else:
        sys.stdout.write(unichr(9643).encode('utf-8'))
        sys.stdout.write(unichr(9643).encode('utf-8'))
      sys.stdout.write(healthcolor)
      sys.stdout.write(healthsymbol)
      sys.stdout.write(jobcolor + colorama.Style.BRIGHT)
      if not (building or recent or queued):
        sys.stdout.write(jobcolor)
      print " " + job + progress + teststatus

    self._redraw_lock.release()

  def clear_screen(self):
    sys.stdout.write("\x1B[2J")
    self.reset_cursor()

  def reset_cursor(self):
    sys.stdout.write("\x1B[1;1H")

  def clear_line(self):
    sys.stdout.write("\x1B[2K")

  def resize_handler(self, signum, frame):
    self._resize_detected = True
 
  def update_handler(self, signum, frame):
    self._update_required = True

  def run(self):
    self._status = self.load_status()
    self.write_status()

    signal.signal(signal.SIGALRM, self.update_handler)
    signal.signal(signal.SIGWINCH, self.resize_handler)
    signal.setitimer(signal.ITIMER_REAL, 2, 5)

    prev_status = self._status
    iteration = 0
    while True:
      iteration += 1

      time.sleep(1)
      if self._update_required:
        self._update_required = False
        self._status = self.load_status()
      if self._resize_detected or prev_status.keys() != self._status.keys():
        self._resize_detected = False
        self.clear_screen()
      prev_status = self._status
      self.reset_cursor()
      self.write_status(blink=(iteration % 2) > 0)
      print "%s   [%d]" % (colorama.Style.RESET_ALL + colorama.Style.DIM, iteration)
      assert iteration < 24*3600

try:
  Jenkins().run()
except KeyboardInterrupt:
  print olorama.Style.RESET_ALL
