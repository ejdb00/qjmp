import os
import random
import time
import shutil
import sets

class HadoopSim:

  MB = 1024 * 1024
  KB = 1024


  def __init__(self, hostnames, master, workers, dirPath, sizes, replicationFactor, priority):
    self.hostnames = hostnames
    self.dirPath = dirPath
    self.sizes = sizes
    self.master = master
    self.workers = workers
    self.priority = priority
    self.replicationFactor = replicationFactor
    self.qjump = False


  def useQjump(self, use):
      self.qjump = use


  def generateFiles(self):
    for hn in self.hostnames:
      for sz in self.sizes:
        filename = hn + str(sz)
        full_path = os.path.join(self.dirPath, filename)
        f = open(full_path, 'w')
        for b in range(sz/16):
          f.write(os.urandom(16))
        f.close()


  def sendFileOfSize(self, sender, receiver, size):
    r_name = receiver.name
    s_name = sender.name
    filename = os.path.join(self.dirPath, s_name + str(size))

    port = random.randint(1024, 65535)

    recCmd = ['nc', '-l', str(port)]
    sendCmd = ['./qjau.py', '-p', str(self.priority), '-v', '0', '-w', '9999999', '-c', '\"nc %s %d < %s\"' % (receiver.IP(), port, filename)]
    if not self.useQjump:
        sendCmd = ['nc', receiver.IP(), str(port), '<', filename]

    receiver.popen(*recCmd, shell=True)
    sender.popen(*sendCmd, shell=True)


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
        self.sendFileOfSize(self.master, self.workers[i], self.sizes[1])
      time.sleep(10)


  def runShuffle(self):
    nWorkers = len(self.workers)

    for i in range(self.replicationFactor):
        self.generateShuffleSets()
        while len(self.workersSet) > 0:

          rs = random.randint(0, nWorkers - 1)
          if rs in self.workersSet:
            sender = self.workers[rs]

            rr = random.randint(0, nWorkers - 1)
            if rr in self.shuffleSets[rs]:
              receiver = self.workers[rr]

              self.sendFileOfSize(sender, receiver, self.sizes[0])

              self.shuffleSets[rs].remove(rr)
              if len(self.shuffleSets[rs]) == 0:
                self.workersSet.remove(rs)

        time.sleep(10)


  def runCollection(self):
    for n in range(self.replicationFactor):
      order = range(len(self.workers))
      random.shuffle(order)
      for i in order:
        self.sendFileOfSize(self.workers[i], self.master, self.sizes[1])
      time.sleep(10)


  def runHadoopSimulation(self):
    self.generateShuffleSets()
    self.runDistribution()
    time.sleep(20)
    self.runShuffle()
    time.sleep(5)
    self.runCollection()


