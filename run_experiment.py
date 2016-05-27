#!/usr/bin/python

from mininet.topo import Topo
from mininet.node import CPULimitedHost, OVSHtbQosSwitch
from mininet.link import TCLink
from mininet.net import Mininet
from mininet.util import dumpNodeConnections
from mininet.cli import CLI
import hadoop_sim
import os
import signal
import subprocess
import time

from functools import partial
from mininet.examples.vlanhost import VLANHost

ptpdClientProcess = {}
ptpdServerProcess = {}
memcachedClientProcess = {}
memcachedServerProcess = {}

MB = 1024 * 1024
KB = 1024


class QJmpTopo(Topo):
  "Topology for QJump experiments"

  def build(self, n_hosts=12, n_switches=4):
    hosts = []
    for h in range(n_hosts):
      hosts.append(self.addHost('h%s' % (h + 1), cpu=0.5/n_hosts))
    switches = []
    for s in range(n_switches):
      switches.append(self.addSwitch('s%s' % (s + 1)))

    # Link the switches together
    for s in range(3):
      self.addLink(switches[s], switches[3], bw=10, delay="10ms")

    # Link the hosts to the switches
    for h in range(n_hosts):
      s = 1
      if h < 6:
        s = 0
      if h > 8:
        s = 2
      self.addLink(hosts[h], switches[s], bw=10, delay="10ms")

    return


def startPTPd(net, outfile, priority):
  ptpdServer = net.getNodeByName("h8")
  ptpdClient = net.getNodeByName("h1")
  server_cmd = './qjau.py -p %d -c \"ptpd -M -c -b h8-eth0 -y 0 -D -h -T 10\"' % priority
  client_cmd = './qjau.py -p %d -c \"ptpd -x -c -g -D -b h1-eth0 -h -T 10 -f %s\"' % (priority, outfile)
  ptpdServerProcess = ptpdServer.popen(server_cmd, stdout=subprocess.PIPE, shell=True, preexec_fn=os.setsid)
  ptpdClientProcess = ptpdClient.popen(client_cmd, stdout=subprocess.PIPE, shell=True, preexec_fn=os.setsid)


def startMemcached(net, outfile, priority):
  memcachedServer = net.getNodeByName("h11")
  memcachedClient = net.getNodeByName("h3")
  server_cmd = './qjau.py -p %d -c \"/usr/bin/memcached -m 64 -p 11211 -u memcache\"' % priority
  client_cmd = './qjau.py -p %d -c \"../clients/memaslap -s %s:11211 -S 1s -B -T2 -c 4 -X 128 > %s\"' % (priority, memcachedServer.IP(), outfile)
  memcachedServerProcess = memcachedServer.popen(server_cmd, stdout=subprocess.PIPE, shell=True, preexec_fn=os.setsid)
  memcachedClientProcess = memcachedClient.popen(client_cmd, stdout=subprocess.PIPE, shell=True, preexec_fn=os.setsid)


def stopPTPd():
  os.killpg(os.getpgid(ptpdClientProcess.pid), signal.SIGTERM)
  os.killpg(os.getpgid(ptpdServerProcess.pid), signal.SIGTERM)


def stopMemcached():
  os.killpg(os.getpgid(memcachedClientProcess.pid), signal.SIGTERM)
  os.killpg(os.getpgid(memcachedServerProcess.pid), signal.SIGTERM)


def runExp1(net, expTime):
  startPTPd(net, "./data/exp1_PTPd_out", 0)
  startMemcached(net, "./data/exp1_memcached_out", 0)
  time.sleep(expTime * 60)
  stopPTPd()
  stopMemcached()


def runExp2(net, hadoop, expTime):
  startPTPd(net, "./data/exp2_PTPd_out", 0)
  startMemcached(net, "./data/exp2_memcached_out", 0)
  hadoop.runHadoopSimulation()
  stopPTPd()
  stopMemcached()


def runExp3(net, hadoop, expTime):
  startPTPd(net, "./data/exp3_PTPd_out", 7)
  startMemcached(net, "./data/exp3_memcached_out", 5)
  hadoop.runHadoopSimulation()
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
  sizes = [1 * MB, 5 * MB, 10 * MB]
  replicationFactor = 6
  priority = 0

  hadoop = hadoop_sim.HadoopSim(hostnames, master, workers, hadoopDir, sizes, replicationFactor, priority)

  return hadoop


def plotData():
  cmd = "python plot_ptp_memcached_hadoop_timeline.py \
      ./data/exp1_PTPd_out \
      ./data/exp1_memcached_out \
      ./data/exp2_PTPd_out \
      ./data/exp2_memcached_out \
      ./data/exp3_PTPd_out \
      ./data/exp3_memcached_out"
  os.system(cmd)

def configureExpTopo():
  topo = QJmpTopo()
  net = Mininet(topo=topo, host=CPULimitedHost, link=TCLink)
  return net

def main():
  if not os.path.exists("./data"):
    os.makedirs("./data")

  os.system("sysctl -w net.ipv4.tcp_congestion_control=cubic")

  topo = QJmpTopo()
  host = partial(VLANHost, vlan=2)
  net = Mininet(topo=topo, host=host, link = TCLink, switch = OVSHtbQosSwitch)

  if not os.path.exists("./tmp/hadoop"):
      os.makedirs("./tmp/hadoop")
  hadoopDir = "./tmp/hadoop"
  hadoop = configureHadoopSim(net, hadoopDir)
  hadoop.generateFiles()
  expTime = 10

  net.start()

  runExp1(net, expTime)

  #runExp2(net, hadoop, expTime)

  #runExp3(net, hadoop, expTime)

  net.stop()
  hadoop.removeFiles()

  #plotData()


if __name__ == "__main__":
  main()
