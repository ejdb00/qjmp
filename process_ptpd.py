import sys

def main():
    infilename = sys.argv[1]
    infile = open(infilename, 'r')
    outfilename = infilename + '_processed'
    outfile = open(outfilename, 'w')
    ln = infile.readline()
    while ln != '':
        if not ln.startswith('QJAU') and not ln.startswith('#') and len(ln.split(',')) > 5:
            outfile.write(ln)
        ln = infile.readline()
    infile.close()
    outfile.close()

if __name__ == '__main__':
    main()
