/******************************************************************************\
*                                                                              *
*        Copyright (c) 2003, The Regents of the University of California       *
*      See the file COPYRIGHT for a complete copyright notice and license.     *
*                                                                              *
\******************************************************************************/

Usage:	mdtest [-b #] [-B] [-c] [-C] [-d testdir] [-D] [-e] [-E] [-f first] [-F]
               [-h] [-i iterations] [-I #] [-l last] [-L] [-n #] [-N #] [-p seconds] 
               [-r] [-R[#]] [-s #] [-S] [-t] [-T] [-u] [-v] [-V #] [-w #] [-y]
               [-z #]

    -b: branching factor of hierarchical directory structure
    -B: no barriers between phases (create/stat/remove)
    -c: collective creates: task 0 does all creates and deletes
    -C: only create files/dirs
    -d: the directory in which the tests will run
    -D: perform test on directories only (no files)
    -e: number of bytes to read from each file
    -E: only read files
    -f: first number of tasks on which the test will run
    -F: perform test on files only (no directories)
    -h: prints help message
    -i: number of iterations the test will run
    -I: number of items per tree node    
    -l: last number of tasks on which the test will run
    -L: files/dirs created only at leaf level
    -n: every task will create/stat/remove # files/dirs per tree
    -N: stride # between neighbor tasks for file/dir stat (local=0)
    -p: pre-iteration delay (in seconds)
    -r: only remove files/dirs
    -R: randomly stat files/dirs (optional seed can be provided)    
    -s: stride between the number of tasks for each test
    -S: shared file access (file only, no directories)
    -t: time unique working directory overhead
    -T: only stat files/dirs        
    -u: unique working directory for each task
    -v: verbosity (each instance of option increments by one)
    -V: verbosity value
    -w: number of bytes to write to each file
    -y: sync file after write completion
    -z: depth of hierarchical directory structure

NOTES:
 * -N allows a "read-your-neighbor" approach by setting stride to 
    tasks-per-node
 * -d allows multiple paths for the form '-d fullpath1@fullpath2@fullpath3'
 * -B allows each task to time itself. The aggregate results reflect this 
    change.
 * -n and -I cannot be used together.  -I specifies the number of files/dirs 
   created per tree node, whereas the -n specifies the total number of 
   files/dirs created over an entire tree.  When using -n, integer division is 
   used to determine the number of files/dirs per tree node.  (E.g. if -n is 
   10 and there are 4 tree nodes (z=1 and b=3), there will be 2 files/dirs per 
   tree node.)
 * -R and -T can be used separately.  -R merely indicates that if files/dirs
   are going to be stat'ed, then they will be stat'ed randomly.


Illustration of terminology:

                     Hierarchical directory structure (tree)
   
                                   =======
                                  |       |  (tree node)
                                   =======
                                  /   |   \
                            ------    |    ------
                           /          |          \
                       =======     =======     =======
                      |       |   |       |   |       |    (leaf level)
                       =======     =======     =======
        
    In this example, the tree has a depth of one (z=1) and branching factor of 
    three (b=3).  The node at the top of the tree is the root node.  The level 
    of nodes furthest from the root is the leaf level.  All trees created by 
    mdtest are balanced.
    
    To see how mdtest operates, do a simple run like the following:
    
        mdtest -z 1 -b 3 -I 10 -C -i 3
    
    This command will create a tree like the one above, then each task will 
    create 10 files/dirs per tree node.  Three of these trees will be created 
    (one for each iteration).


Example usages:

mdtest -I 10 -z 5 -b 2

    A directory tree is created in the current working directory that has a 
    depth of 5 and a branching factor of 2.  Each task operates on 10 
    files/dirs in each tree node.
    
mdtest -I 10 -z 5 -b 2 -R

    This example is the same as the previous one except that the files/dirs are
    stat'ed randomly.
    
mdtest -I 10 -z 5 -b 2 -R4

    Again, this example is the same as the previous except a seed of 4 is 
    passed to the random number generator.
    
mdtest -I 10 -z 5 -b 2 -L

    A directory tree is created as described above, but in this example 
    files/dirs exist only at the leaf level of the tree.

mdtest -n 100 -i 3 -d /users/me/testing

    Each task creates 100 files/dirs in a root node (there are no branches
    out of the root node) within the path /users/me/testing.  This is done 
    three times.  Aggregate values are calculated over the iterations.
    
mdtest -n 100 -F -C

    Each task only creates 100 files in the current directory.
    Directories are not created.  The files are neither stat'ed nor
    removed.
    
mdtest -I 5 -z 3 -b 5 -u -d /users/me/testing

    Each task creates a directory tree in the /users/me/testing
    directory.  Each tree has a depth of 3 and a branching factor of
    5.  Five files/dirs are operated upon in each node of each tree.
    
mdtest -I 5 -z 3 -b 5 -u -d /users/me/testing@/some/other/location

    This run is the same as the previous except that each task creates
    its tree in a different directory.  Task 0 will create a tree in 
    /users/me/testing. Task 1 will create a tree in /some/other/location.
    After all of the directories are used, the remaining tasks round-
    robin over the directories supplied.  (I.e. Task 2 will create a 
    tree in /users/me/testing, etc.)
