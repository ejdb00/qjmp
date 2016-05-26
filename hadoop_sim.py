import os
import random
import timer
import shutil
import sets

class HadoopSim:

  mb = 1024 * 1024
  kb = 1024

  def __init__(self, hostnames, master, workers, dirPath, sizes, priority):
    self.hostnames = hostnames
    self.dirPath = dirPath
    self.sizes = sizes
    self.master = master
    self.workers = workers
    self.priority = priority

  def generateFiles(self):
    for hn in self.hostnames:
      for sz in self.sizes:
        filename = hn + str(sz)
        full_path = os.path.join(self.dir_path, filename)
        f = open(full_path, 'br+')
        for b in range(sz/16):
          f.write(os.urandom(16))
        f.close()

  def sendFileOfSize(self, sender, receiver, size):
    r_name = receiver.name
    s_name = sender.name
    filename = os.path.join(self.dirPath, s_name + str(size))

    port = random.randint(1024, 65535)

    recCmd = 'nc -l %d' % port
    sendCmd = './qjau.py -p %d -c \"nc %s %d < %s\"' % (self.priority, receiver.IP(), port, filename)

    receiver.popen(recCmd, shell=True)
    sender.popen(sendCmd, shell=True)


  def removeFiles(self):
    shutil.rmtree(self.dirPath, ignore_errors=True)

  def generateShuffleSets(self):
    self.workersSet = sets.Set(range(len(self.workers)))
    self.shuffleSets = []
    for w in range(len(self.workers)):
      self.shuffleSets.append(sets.Set(range(len(self.workers))))
      self.shuffleSets[w].remove(w)

  def runDistribution(self):
    for n in range(self.replicationFactor):
      order = range(len(self.workers))
      random.shuffle(order)
      for i in order:
        sendFileOfSize(self.master, self.workers[i], 5 * self.mb)
        timer.sleep(random.random() / 4.0)

  def runShuffle(self):
    nWorkers = len(self.workers)
    while len(self.workersSet) > 0:
      rs = random.randint(0, nWorkers - 1)
      if rs in self.workersSet:
        sender = workers[rs]
        rr = random.randint(0, nWorkers - 1)
        if rr in self.shuffleSets[rs]:
          receiver = workers[rr]
          sendFileOfSize(sender, receiver, 10 * self.mb)
          self.shuffleSets[rs].remove(rr)
          if len(self.shuffleSets[rs]) == 0:
            self.workersSet.remove(rs)
          timer.sleep(random.random() / 2.0)

  def runCollection(self):
    for n in range(self.replicationFactor):
      order = range(len(self.workers))
      random.shuffle(order)
      for i in order:
        sendFileOfSize(self.workers[i], self.master, 10 * self.mb)
        timer.sleep(random.random() * 2)

  def runHadoopSimulation(self):
    self.generateShuffleSets()
    self.runDistribution()
    timer.sleep(20)
    self.runShuffle()
    timer.sleep(5)
    self.runCollection()


