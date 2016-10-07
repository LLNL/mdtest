#!/usr/bin/env python
#
# Tester for mdtest
#
#/*****************************************************************************\
#*                                                                             *
#*       Copyright (c) 2003, The Regents of the University of California       *
#*     See the file COPYRIGHT for a complete copyright notice and license.     *
#*                                                                             *
#\*****************************************************************************/
#
# CVS info:
#   $RCSfile: tester.py,v $
#   $Revision: 1.1.2.1 $
#   $Date: 2010/05/11 21:25:16 $
#   $Author: loewe6 $

import sys
import os.path
import string
import time

debug = 0

# definitions
RMPOOL     = 'systest'
NODES      = 1
TPN        = 4
PROCS      = NODES * TPN
EXECUTABLE = '/fs/home/bloewe/benchmarks/mdtest/mdtest'
TEST_DIR_LOC1 = '/panfs/REALM226/home/V1'
TEST_DIR_LOC3 = '/panfs/REALM226/home/V1@/panfs/REALM226/home/V2@/panfs/REALM226/home/V3'
TEST_DIRS = '/panfs/REALM226/home/V1 /panfs/REALM226/home/V2 /panfs/REALM226/home/V3'

# tests
tests = [

        # default
        "",

        # test directory
        "-d " + TEST_DIR_LOC1,

        # number of files per processor
        "-d " + TEST_DIR_LOC1 + " -n 3",

        # number of iterations of test
        "-d " + TEST_DIR_LOC1 + " -n 3 -i 2",

        # serially create before parallel access
        "-d " + TEST_DIR_LOC1 + " -n 3 -i 2 -c",

        # pre-test delay
        "-d " + TEST_DIR_LOC1 + " -n 3 -i 2 -p 1",

        # verbosity=1
        "-d " + TEST_DIR_LOC1 + " -n 3 -i 2 -v",

        # verbosity=2
        "-d " + TEST_DIR_LOC1 + " -n 3 -i 2 -v -v",

        # verbosity=3
        "-d " + TEST_DIR_LOC1 + " -n 3 -i 2 -V 3",

        # shared file
        "-d " + TEST_DIR_LOC1 + " -n 3 -i 2 -S",

        # read-your-neighbor
        "-d " + TEST_DIR_LOC1 + " -n 3 -i 2 -S -N " + str(TPN),

        # unique subdirectory
        "-d " + TEST_DIR_LOC1 + " -n 3 -i 2 -u",

        # time unique subdirectory creation/deletion
        "-d " + TEST_DIR_LOC1 + " -n 3 -i 2 -u -t",

        # directories only
        "-d " + TEST_DIR_LOC1 + " -n 3 -i 2 -u -t -D",

        # files only
        "-d " + TEST_DIR_LOC1 + " -n 3 -i 2 -u -t -F",

        # write 0 bytes
        "-d " + TEST_DIR_LOC1 + " -n 3 -i 2 -u -t -F -w 0",

        # write 1 byte
        "-d " + TEST_DIR_LOC1 + " -n 3 -i 2 -u -t -F -w 1",

        # write 0 bytes w/fsync
        "-d " + TEST_DIR_LOC1 + " -n 3 -i 2 -u -t -F -w 0 -y",

        # write 1 byte w/fsync
        "-d " + TEST_DIR_LOC1 + " -n 3 -i 2 -u -t -F -w 1 -y",

        # read-your-neighbor w/unique subdirectory
        "-d " + TEST_DIR_LOC1 + " -n 3 -i 2 -u -t -N " + str(TPN),

        # number of tasks to run
        "-d " + TEST_DIR_LOC1 + " -n 3 -i 2 -N " + str(TPN) + " -f 1 -l " \
            + str(PROCS-1) + " -s " + str(PROCS/3),

        # remove any remaining tests from previous run
        "-d " + TEST_DIR_LOC1 + " -n 3 -i 2 -N " + str(TPN) + " -f 1 -l " \
            + str(PROCS-1) + " -s " + str(PROCS/3) + " -r ",

        # test directories
        "-d " + TEST_DIR_LOC3,

        # number of files per processor
        "-d " + TEST_DIR_LOC3 + " -n 3",

        # number of iterations of test
        "-d " + TEST_DIR_LOC3 + " -n 3 -i 2",

        # pre-test delay
        "-d " + TEST_DIR_LOC3 + " -n 3 -i 2 -p 1",

        # verbosity=1
        "-d " + TEST_DIR_LOC3 + " -n 3 -i 2 -v",

        # verbosity=2
        "-d " + TEST_DIR_LOC3 + " -n 3 -i 2 -v -v",

        # verbosity=3
        "-d " + TEST_DIR_LOC3 + " -n 3 -i 2 -V 3",

        # shared file
        "-d " + TEST_DIR_LOC3 + " -n 3 -i 2 -S",

        # unique subdirectory
        "-d " + TEST_DIR_LOC3 + " -n 3 -i 2 -u",

        # time unique subdirectory creation/deletion
        "-d " + TEST_DIR_LOC3 + " -n 3 -i 2 -u -t",

        # directories only
        "-d " + TEST_DIR_LOC3 + " -n 3 -i 2 -u -t -D",

        # files only
        "-d " + TEST_DIR_LOC3 + " -n 3 -i 2 -u -t -F",

        # write 0 bytes
        "-d " + TEST_DIR_LOC3 + " -n 3 -i 2 -u -t -F -w 0",

        # write 1 byte
        "-d " + TEST_DIR_LOC3 + " -n 3 -i 2 -u -t -F -w 1",

        # write 0 bytes w/fsync
        "-d " + TEST_DIR_LOC3 + " -n 3 -i 2 -u -t -F -w 0 -y",

        # write 1 byte w/fsync
        "-d " + TEST_DIR_LOC3 + " -n 3 -i 2 -u -t -F -w 1 -y",

        # number of tasks to run
        "-d " + TEST_DIR_LOC1 + " -n 3 -i 2 -f 1 -l " \
            + str(PROCS-1) + " -s " + str(PROCS/3)

]


#############################
# set environment variables #
#############################
def SetEnvironment(rmpool, nodes, procs):
    os.environ['MP_RMPOOL'] = str(rmpool)
    os.environ['MP_NODES']  = str(nodes)
    os.environ['MP_PROCS']  = str(procs)
    return


#################
# flush to file #
#################
def Flush2File(resultsFile, string):
    resultsFile.write(string + '\n')
    resultsFile.flush()


###################
# run test script #
###################
def RunScript(resultsFile, test):
    # -- for poe --   command = "poe " + EXECUTABLE + " " + test
    command = "mpiexec -n " + str(PROCS) + " " + EXECUTABLE + " " + test
    if debug == 1:
        Flush2File(resultsFile, command)
    else:
        childIn, childOut = os.popen4(command)
        childIn.close()
        while 1:
            line = childOut.readline()
            if line == '': break
            Flush2File(resultsFile, line[:-1])
        childOut.close()
    return


########
# main #
########
def main():
    resultsFile = open("./results.txt-" + \
                       os.popen("date +%m.%d.%y").read()[:-1], "w")

    Flush2File(resultsFile, "Testing mdtest")

    # test -h option on one task
    SetEnvironment(RMPOOL, 1, 1)
    RunScript(resultsFile, '-h')

    # set environ and run tests
    SetEnvironment(RMPOOL, NODES, PROCS)
    for i in range(0, len(tests)):
        time.sleep(0)                     # delay any cleanup for previous test
        #os.system("rm -rf " + TEST_DIRS)	# cleanup TEST_DIRS between tests
        RunScript(resultsFile, tests[i])
            
    Flush2File(resultsFile, "\nFinished testing mdtest")
    resultsFile.close()

if __name__ == "__main__":
    main()

