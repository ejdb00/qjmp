# cs244 qjump

0. cda3/experiment
1. sudo lsmod | grep 8021q
2. sudo modprobe 8021q
3. sudo lsmod | grep sch_qjump
4. sudo insmod sch_qjump.ko bytesq=256 timeq=192000rate=15rate=1007rate=9999999

5. sudoython run_experiment.py
6. cd to newly created data/[timestamp] dir
7. sudoython ../../process_data.py

8. copy the files in the new ‘processed’ dir to a machine with latex installed
9. git clone https://github.com/camsas/qjump-nsdi15-plotting
10. sudoython qjump-nsdi15-plotting/figure1a_5/plot_ptpd_memcached_hadoop_timeline.py exp1_PTPd_out exp1_memcached_out exp2_PTPd_out exp2_memcached_out exp3_PTPd_out exp3_memcached_out

Open the PDF with your favorite viewer
Marvel at how it almost looks intelligible
