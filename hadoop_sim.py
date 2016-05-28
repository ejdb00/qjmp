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
    self.useQjump = False


  def useQjump(qjump):
      self.useQjump = qjump


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
    sendCmd = ['./qjau.py', '-p', str(self.priority), '-c', '\"nc %s %d < %s\"' % (receiver.IP(), port, filename)]
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
        sendFileOfSize(self.master, self.workers[i], 5 * self.MB)
        time.sleep(random.random() / 4.0)


  def runShuffle(self):
    nWorkers = len(self.workers)

    for i in range(self.replicationFactor):
        self.generateShuffleSets()
        while len(self.workersSet) > 0:

          rs = random.randint(0, nWorkers - 1)
          if rs in self.workersSet:
            sender = workers[rs]

            rr = random.randint(0, nWorkers - 1)
            if rr in self.shuffleSets[rs]:
              receiver = workers[rr]

              sendFileOfSize(sender, receiver, 5 * self.MB)

              self.shuffleSets[rs].remove(rr)
              if len(self.shuffleSets[rs]) == 0:
                self.workersSet.remove(rs)

              time.sleep(random.random() / 2.0)


  def runCollection(self):
    for n in range(self.replicationFactor):
      order = range(len(self.workers))
      random.shuffle(order)
      for i in order:
        sendFileOfSize(self.workers[i], self.master, 10 * self.MB)
        time.sleep(random.random() * 2)


  def runHadoopSimulation(self):
    self.generateShuffleSets()
    self.runDistribution()
    time.sleep(20)
    self.runShuffle()
    time.sleep(5)
    self.runCollection()


