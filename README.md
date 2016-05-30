# cs244 qjump

First bring up an instance of our public AMI: cs244-16-qjump

0. cd pa3/experiment
1. git pull origin master
1. sudo lsmod | grep 8021q
  - if there is output, skip the next step
2. sudo modprobe 8021q
3. sudo lsmod | grep sch_qjump
  - if there is output, 'sudo rmmod sch_qjump'
4. sudo insmod sch_qjump.ko bytesq=256 timeq=192000 p1rate=1 p5rate=100 p7rate=9999999

5. sudo python run_experiment.py -- THIS TAKES A LITTLE OVER 33 MINUTES, SORRY
6. cd to newly created data/[timestamp] dir
7. sudo python ../../process_data.py

8. copy the files in the new ‘processed’ dir to a machine with latex installed
9. git clone https://github.com/camsas/qjump-nsdi15-plotting
10. sudo python qjump-nsdi15-plotting/figure1a_5/plot_ptpd_memcached_hadoop_timeline.py exp1_PTPd_out exp1_memcached_out exp2_PTPd_out exp2_memcached_out exp3_PTPd_out exp3_memcached_out

Open the PDF with your favorite viewer   
Marvel at how it almost looks intelligible

Note: The testing script is relatively fragile in that it has very little error handling. If it fails at runtime for some reason, you probably have to 'sudo reboot' and start this whole list of instructions over again because the mininet state will not have been cleaned up. Would have liked to have added more graceful handling into the script with a little more time :/
