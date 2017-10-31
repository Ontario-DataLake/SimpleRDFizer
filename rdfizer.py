#!/usr/bin/env python

__author__ = 'Kemele M. Endris'

from ontario.rdfizer import *
import getopt, sys


if __name__ == '__main__':
    mappingfile = "/home/dsdl/PycharmProjects/SparkRDFizer/config/tcgamapping.ttl"
    argv = sys.argv[1:]
    try:
        opts, args = getopt.getopt(argv, "hm:", ["mappingFile="])
    except getopt.GetoptError:
        print('rdfizer.py -m <mappingFile>')
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            print('rdfizer.py -m <mappingFile>')
            sys.exit()
        elif opt in ("-m", "--mappingFile"):
            mappingfile = arg

    main(mappingfile)
