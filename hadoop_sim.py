import os
import random
import timer
import shutil

def generate_files(hostnames, dir_path, sizes):
  for hn in hostnames:
    for sz in sizes:
      filename = hn + str(sz)
      full_path = os.path.join(dir_path, filename)
      f = open(full_path, 'br+')
      for b in range(sz/16):
        f.write(os.urandom(16))
      f.close()



def send_file_of_size(sender, receiver, size, dir_path):
  r_name = receiver.name
  s_name = sender.name
  filename = os.path.join(dir_path, s_name + str(size))

  port = random.randint(1024, 65535)

  rec_cmd = 'nc -l %d' % port
  receiver.popen(rec_cmd, shell=True)

  send_cmd = 'nc %s %s < %s' % (receiver.IP(), port, filename)
  sender.popen(send_cmp, shell=True)


def remove_files(dir_path):
  shutil.rmtree(dir_path, ignore_errors=True)

def run_distribution(master, workers, dir_path):
  for w in workers:
    send_file_of_size(master, workers[w], 50 * 1024 * 1024)

def run_mappers_to_reducers(master, workers, dir_path):


def run_reducers_to_master(master, workers, dir_path):


def run_hadoop_emulation(master, workers, dir_path):
  run_distribution(master, workers)
  timer.sleep(10)
  run_mappers_to_reducers(master, workers)
  run_reducers_to_master(master, workers)

