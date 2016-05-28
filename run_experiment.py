#!/usr/bin/python

from mininet.topo import Topo
from mininet.node import CPULimitedHost, OVSHtbQosSwitch
from mininet.link import TCLink
from mininet.net import Mininet
from mininet.util import dumpNodeConnections
from mininet.cli import CLI
from mininet.log import setLogLevel

import hadoop_sim
import os
import signal
import subprocess
import time

from functools import partial
from mininet.examples.vlanhost import VLANHost

MB = 1024 * 1024
KB = 1024


class QJmpTopo(Topo):
  "Topology for QJump experiments"

  def build(self, n_hosts=12, n_switches=4):
    hosts = []
    for h in range(n_hosts):
      hosts.append(self.addHost('h%s' % (h + 1)))
    switches = []
    for s in range(n_switches):
      switches.append(self.addSwitch('s%s' % (s + 1)))

    # Link the switches together
    for s in range(3):
      self.addLink(switches[s], switches[3], bw=15)

    # Link the hosts to the switches
    for h in range(n_hosts):
      s = 1
      if h < 6:
        s = 0
      if h > 8:
        s = 2
      self.addLink(hosts[h], switches[s], bw=15)

    return

def configureQueues(net):
  for node in net.hosts:
    for ifname in node.intfNames():
      if ifname == "lo":
        continue
      for i in range(8):
        node.pexec('vconfig', 'set_egress_map', ifname, str(i), str(i))
        node.pexec('vconfig', 'set_ingress_map', ifname, str(i), str(i))

def installQjump(net, root_name):
  for node in net.hosts:
    ifnames = set(name.split('.')[0] for name in node.intfNames())
    for ifname in ifnames:
#      if ifname != root_name:
      ifname = ifname.replace('.2', '')
      node.pexec('tc', 'qdisc', 'add' , 'dev', ifname, 'parent', '5:1', 'handle', '6:', 'qjump')
#node.pexec('tc qdisc add dev %s parent 5:1 handle 6: qjump' % ifname)
#      else:
#        node.pexec('tc qdisc add dev %s root qjump' % ifname)

def startPTPd(net, outfile, priority, qjump):
  ptpdServer = net.getNodeByName("h8")
  ptpdClient = net.getNodeByName("h1")
  server_cmd = ['./qjau.py', '-v', '0', '-w', '999999', '-p', str(priority), '-c', '\"ptpd -W -c -b h8-eth0.2 -y 0 -D -h -T 10 -L\"']
  client_cmd = ['./qjau.py', '-v', '0', '-w', '999999', '-p', str(priority), '-c', '\"ptpd -x -c -g -D -b h1-eth0.2 -h -T 10 -L -f %s\"' % (outfile)]
  if not qjump:
      server_cmd = ['ptpd', '-W', '-c', '-b', 'h8-eth0.2', '-y', '0', '-D', '-h', '-T', '10', '-L']
      client_cmd = ['ptpd', '-x', '-c', '-g', '-D', '-b', 'h1-eth0.2', '-h', '-T', '10', '-f', outfile, '-L']
  ptpdServerProcess = ptpdServer.popen(*server_cmd, stdout=subprocess.PIPE, shell=True)
  ptpdClientProcess = ptpdClient.popen(*client_cmd, stdout=subprocess.PIPE, shell=True)
  return (ptpdServerProcess, ptpdClientProcess)


def startMemcached(net, outfile, priority, qjump):
  memcachedServer = net.getNodeByName("h11")
  memcachedClient = net.getNodeByName("h3")

  server_cmd = ['./qjau.py', '-v', '0', '-w', '999999', '-p', str(priority), '-c', '\"/usr/bin/memcached -m 64 -p 11211 -u memcache\"']
  if not qjump:
      server_cmd = ['/usr/bin/memcached', '-m', '64', '-p', '11211', '-u', 'memcache']
  memcachedServerProcess = memcachedServer.popen(*server_cmd, stdout=subprocess.PIPE, shell=True)

  for i in range(10):
    partial_out = outfile + '.%02d' % (i + 1)
    client_cmd = ['./qjau.py', '-v', '0', '-w', '999999', '-p', str(priority), '-c', '\"./clients/memaslap -s %s:11211 -S 1s -B -T1 -c 1 -X 128 > %s\"' % (memcachedServer.IP(), partial_out)]
    if not qjump:
      client_cmd = ['./clients/memaslap', '-s', memcachedServer.IP() + ':11211', '-S', '1s', '-B', '-T2', '-c', '4', '-X', '128', '>', partial_out]
    memcachedClientProcess = memcachedClient.popen(*client_cmd, stdout=subprocess.PIPE, shell=True)
  return (memcachedServerProcess, memcachedClientProcess)


def stopPTPd():
  pgrep_out = subprocess.Popen("pgrep ptpd", stdout=subprocess.PIPE, shell=True)
  pidstr = pgrep_out.stdout.readline()
  while pidstr != '':
      os.killpg(os.getpgid(int(pidstr)), signal.SIGTERM)
      pidstr = pgrep_out.stdout.readline()


def stopMemcached():
  pgrep_out = subprocess.Popen("pgrep memcached", stdout=subprocess.PIPE, shell=True)
  pidstr = pgrep_out.stdout.readline()
  while pidstr != '':
      os.killpg(os.getpgid(int(pidstr)), signal.SIGTERM)
      pidstr = pgrep_out.stdout.readline()


def runExp1(net, expTime, dataDir):
  ptpdOutfile = os.path.join(dataDir, "exp1_PTPd_out")
  memcachedName = "exp1_memcached_out"
  os.makedirs(os.path.join(dataDir, memcachedName))
  memcachedOutfile = os.path.join(dataDir, memcachedName, memcachedName)
  ptpdProcesses = startPTPd(net, ptpdOutfile, 0, True)
  memcachedProcesses = startMemcached(net, memcachedOutfile, 0, True)
  for i in range(expTime):
      time.sleep(60)
      print "You've been waiting for %d minutes" % (i + 1)
  stopPTPd()
  stopMemcached()


def runExp2(net, hadoop, expTime, dataDir):
  ptpdOutfile = os.path.join(dataDir, "exp2_PTPd_out")
  memcachedName = "exp2_memcached_out"
  os.makedirs(os.path.join(dataDir, memcachedName))
  memcachedOutfile = os.path.join(dataDir, memcachedName, memcachedName)

  start = time.time()

  ptpdProcesses = startPTPd(net, ptpdOutfile, 0, True)
  memcachedProcesses = startMemcached(net, memcachedOutfile, 0, True)
  hadoop.runHadoopSimulation()

  cur = time.time()
  while(cur - start < expTime * 60):
      print "%d seconds left" % int((expTime * 60) - (cur - start))
      time.sleep(10)
      cur = time.time()

  stopPTPd()
  stopMemcached()


def runExp3(net, hadoop, expTime, dataDir):
  hadoop.useQjump(True)
  ptpdOutfile = os.path.join(dataDir, "exp3_PTPd_out")
  memcachedName = "exp3_memcached_out"
  os.makedirs(os.path.join(dataDir, memcachedName))
  memcachedOutfile = os.path.join(dataDir, memcachedName, memcachedName)

  start = time.time()

  ptpdProcesses = startPTPd(net, ptpdOutfile, 7, True)
  memcachedProcesses = startMemcached(net, memcachedOutfile, 5, True)
  hadoop.runHadoopSimulation()

  cur = time.time()
  while(cur - start < expTime * 60):
      print "%d seconds left" % int((expTime * 60) - (cur - start))
      time.sleep(10)
      cur = time.time()

  stopPTPd()
  stopMemcached()


def configureHadoopSim(net, hadoopDir):
  hostnames = []
  for i in range(12):
    hostname = "h%d" % (i + 1)
    hostnames.append(hostname)

  workernames = ["h4", "h5", "h6", "h7", "h9", "h10", "h12"]
  workers = []
  for wn in workernames:
    workers.append(net.getNodeByName(wn))

  master = net.getNodeByName("h2")
  sizes = [1 * MB, 2 * MB, 5 * MB]
  replicationFactor = 6
  priority = 0

  hadoop = hadoop_sim.HadoopSim(hostnames, master, workers, hadoopDir, sizes, replicationFactor, priority)

  return hadoop


def plotData(dataDir):
  exp1ptpd = os.path.join(dataDir, "exp1_PTPd_out")
  exp1memcached = os.path.join(dataDir, "exp1_memcached_out")
  exp2ptpd = os.path.join(dataDir, "exp2_PTPd_out")
  exp2memcached = os.path.join(dataDir, "exp2_memcached_out")
  exp3ptpd = os.path.join(dataDir, "exp3_PTPd_out")
  exp3memcached = os.path.join(dataDir, "exp3_memcached_out")
  cmd = "python plot_ptp_memcached_hadoop_timeline.py %s %s %s %s %s %s" % (epx1ptpd, exp1memcached, exp2ptpd, exp2memcached, exp3ptpd, exp3memcached)
  os.system(cmd)

def configureExpTopo():
  topo = QJmpTopo()
  host = partial(VLANHost, vlan=2)
  net = Mininet(topo=topo, host=host, link = TCLink, switch = OVSHtbQosSwitch)
  return net

def main():
  if not os.path.exists("./data"):
    os.makedirs("./data")
  dataDir = os.path.join("./data", time.strftime("%d-%m-%Y_%H-%M-%S"))
  os.makedirs(dataDir)

  os.system("sysctl -w net.ipv4.tcp_congestion_control=cubic")

  net = configureExpTopo()

  hadoopDir = "./tmp/hadoop"
  if not os.path.exists(hadoopDir):
      os.makedirs(hadoopDir)
  hadoop = configureHadoopSim(net, hadoopDir)
  hadoop.generateFiles()

  expTime = 11

  net.start()

  #runExp1(net, expTime, dataDir)

  runExp2(net, hadoop, expTime, dataDir)

  #runExp3(net, hadoop, expTime, dataDir)

  net.stop()
  hadoop.removeFiles()

  #plotData(dataDir)


if __name__ == "__main__":
  main()
