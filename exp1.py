#!/usr/bin/python

from mininet.topo import Topo
from mininet.node import CPULimitedHost
from mininet.link import TCLink
from mininet.net import Mininet
from mininet.util import dumpNodeConnections
from mininet.cli import CLI

class QJmpTopo(Topo):
  "Topology for QJump experiments"

  def build(self, n_hosts=12, n_switches=4):
    hosts = []
    for h in range(n_hosts):
      hosts.append(self.addHost('h%s' % (h + 1), cpu=0.5/n_hosts))
    switches = []
    for s in range(n_switchs):
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


def main():
  topo = QJmpTopo
  net = Mininet(topo=topo, host=CPULimitedHost, link = TCLink)


if __name__ == "__main__":
  main()
