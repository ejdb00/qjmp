import sys
import os

def main():
    os.makedirs('./processed')
    for i in range(1, 4):
        ptpdFile = 'exp%d_PTPd_out' % i
        memcachedDir = 'exp%d_memcached_out' % i

        ptpd_cmd = ['python ../../process_ptpd.py', ptpdFile]
        mem_cmd = ['../../qjump-nsdi15-plotting/figure1a_5/process_mem.py', memcachedDir]

        os.system(' '.join(ptpd_cmd))
        os.system(' '.join(mem_cmd))
        os.system('mv %s_processed ./processed/%s' % (ptpdFile, ptpdFile))
        os.system('mv %s.set.processed2 ./processed/%s' % (memcachedDir, memcachedDir))

if __name__ == '__main__':
    main()
