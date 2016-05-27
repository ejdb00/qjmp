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
from vlanhost import VLANHost

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
  server_cmd = ['../qjump-app-util/qjau.py', '-v', '2', '-w', '999999', '-p', str(priority), '-c', '\"ptpd -W -c -b h8-eth0 -y 0 -D -h -T 10\"']
  client_cmd = ['../qjump-app-util/qjau.py', '-v', '2', '-w', '999999', '-p', str(priority), '-c', '\"ptpd -x -c -g -D -b h1-eth0 -h -T 10 -f %s\"' % (outfile)]
  ptpdServerProcess = ptpdServer.popen(*server_cmd, stdout=subprocess.PIPE, shell=True, preexec_fn=os.setsid)
  ptpdClientProcess = ptpdClient.popen(*client_cmd, stdout=subprocess.PIPE, shell=True, preexec_fn=os.setsid)
  return (ptpdServerProcess, ptpdClientProcess)


def startMemcached(net, outfile, priority):
  memcachedServer = net.getNodeByName("h11")
  memcachedClient = net.getNodeByName("h3")
  server_cmd = ['../qjump-app-util/qjau.py', '-v', '2', '-w', '999999', '-p', str(priority), '-c', '\"/usr/bin/memcached -m 64 -p 11211 -u memcache\"']
  client_cmd = ['../qjump-app-util/qjau.py', '-v', '2', '-w', '999999', '-p', str(priority), '-c', '\"./clients/memaslap -s %s:11211 -S 1s -B -T2 -c 4 -X 128 > %s\"' % (memcachedServer.IP(), outfile)]
  memcachedServerProcess = memcachedServer.popen(*server_cmd, stdout=subprocess.PIPE, shell=True, preexec_fn=os.setsid)
  memcachedClientProcess = memcachedClient.popen(*client_cmd, stdout=subprocess.PIPE, shell=True, preexec_fn=os.setsid)
  return (memcachedServerProcess, memcachedClientProcess)


def stopPTPd(processes):
  (serverProcess, clientProcess) = processes
  os.killpg(os.getpgid(clientProcess.pid), signal.SIGTERM)
  os.killpg(os.getpgid(serverProcess.pid), signal.SIGTERM)


def stopMemcached(processes):
  (serverProcess, clientProcess) = processes
  os.killpg(os.getpgid(clientProcess.pid), signal.SIGTERM)
  os.killpg(os.getpgid(serverProcess.pid), signal.SIGTERM)


def runExp1(net, expTime, dataDir):
  ptpdOutfile = os.path.join(dataDir, "exp1_PTPd_out")
  memcachedOutfile = os.path.join(dataDir, "exp1_memcached_out")
  ptpdProcesses = startPTPd(net, ptpdOutfile, 0)
  memcachedProcesses = startMemcached(net, memcachedOutfile, 0)
  time.sleep(expTime * 60)
  stopPTPd(ptpdProcesses)
  stopMemcached(memcachedProcesses)


def runExp2(net, hadoop, expTime, dataDir):
  ptpdOutfile = os.path.join(dataDir, "exp2_PTPd_out")
  memcachedOutfile = os.path.join(dataDir, "exp2_memcached_out")
  ptpdProcesses = startPTPd(net, ptpdOutfile, 0)
  memcachedProcesses = startMemcached(net, memcachedOutfile, 0)
  hadoop.runHadoopSimulation()
  stopPTPd(ptpdProcesses)
  stopMemcached(memcachedProcesses)


def runExp3(net, hadoop, expTime, dataDir):
  ptpdOutfile = os.path.join(dataDir, "exp3_PTPd_out")
  memcachedOutfile = os.path.join(dataDir, "exp3_memcached_out")
  ptpdProcesses = startPTPd(net, ptpdOutfile, 7)
  memcachedProcesses = startMemcached(net, memcachedOutfile, 5)
  hadoop.runHadoopSimulation()
  stopPTPd(ptpdProcesses)
  stopMemcached(memcachedProcesses)


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

  expTime = 10

  net.start()

  runExp1(net, expTime, dataDir)

  #runExp2(net, hadoop, expTime, dataDir)

  #runExp3(net, hadoop, expTime, dataDir)

  net.stop()
  hadoop.removeFiles()

  #plotData(dataDir)


if __name__ == "__main__":
  main()
