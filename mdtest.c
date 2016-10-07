/*
 * Copyright (C) 2003, The Regents of the University of California.
 *  Produced at the Lawrence Livermore National Laboratory.
 *  Written by Christopher J. Morrone <morrone@llnl.gov>,
 *  Bill Loewe <loewe@loewe.net>, Tyce McLarty <mclarty@llnl.gov>,
 *  and Ryan Kroiss <rrkroiss@lanl.gov>.
 *  All rights reserved.
 *  UCRL-CODE-155800
 *
 *  Please read the COPYRIGHT file.
 *
 *  This program is free software; you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License (as published by
 *  the Free Software Foundation) version 2, dated June 1991.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the IMPLIED WARRANTY OF
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  terms and conditions of the GNU General Public License for more details.
 *
 *  You should have received a copy of the GNU General Public License
 *  along with this program; if not, write to the Free Software
 *  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 * CVS info:
 *   $RCSfile: mdtest.c,v $
 *   $Revision: 1.1.1.1.2.1 $
 *   $Date: 2010/05/11 21:25:16 $
 *   $Author: loewe6 $
 */

#include "mpi.h"
#include <math.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#ifdef Darwin
#include <sys/param.h>
#include <sys/mount.h>
#else
#include <sys/statfs.h>
#endif
#include <fcntl.h>
#include <string.h>
#include <unistd.h>
#include <dirent.h>
#include <errno.h>
#include <time.h>
#include <sys/time.h>

#define FILEMODE S_IRUSR|S_IWUSR|S_IRGRP|S_IWGRP|S_IROTH
#define DIRMODE S_IRUSR|S_IWUSR|S_IXUSR|S_IRGRP|S_IWGRP|S_IXGRP|S_IROTH|S_IXOTH
#define MAX_LEN 1024
#define RELEASE_VERS "1.8.4"
#define TEST_DIR "#test-dir"
#define ITEM_COUNT 25000

typedef struct
{
    double entry[10];
} table_t;

int rank;
int size;
int* rand_array;
char testdir[MAX_LEN];
char testdirpath[MAX_LEN];
char top_dir[MAX_LEN];
char base_tree_name[MAX_LEN];
char ** filenames = NULL;
char hostname[MAX_LEN];
char unique_dir[MAX_LEN];
char mk_name[MAX_LEN];
char stat_name[MAX_LEN];
char read_name[MAX_LEN];
char rm_name[MAX_LEN];
char unique_mk_dir[MAX_LEN];
char unique_chdir_dir[MAX_LEN];
char unique_stat_dir[MAX_LEN];
char unique_read_dir[MAX_LEN];
char unique_rm_dir[MAX_LEN];
char unique_rm_uni_dir[MAX_LEN];
char * write_buffer = NULL;
char * read_buffer = NULL;
int barriers = 1;
int create_only = 0;
int stat_only = 0;
int read_only = 0;
int remove_only = 0;
int leaf_only = 0;
int branch_factor = 1;
int depth = 0;
int num_dirs_in_tree = 0;
int items_per_dir = 0;
int random_seed = 0;
int shared_file = 0;
int files_only = 0;
int dirs_only = 0;
int pre_delay = 0;
int unique_dir_per_task = 0;
int time_unique_dir_overhead = 0;
int verbose = 0;
int throttle = 1;
int items = 0;
int collective_creates = 0;
int write_bytes = 0;
int read_bytes = 0;
int sync_file = 0;
int path_count = 0;
int nstride = 0; /* neighbor stride */
MPI_Comm testcomm;
table_t * summary_table;

/* for making/removing unique directory && stating/deleting subdirectory */
enum {MK_UNI_DIR, STAT_SUB_DIR, READ_SUB_DIR, RM_SUB_DIR, RM_UNI_DIR};

#ifdef __linux__
#define FAIL(msg) do { \
    fprintf(stdout, "%s: Process %d(%s): FAILED in %s, %s: %s\n",\
        timestamp(), rank, hostname, __func__, \
        msg, strerror(errno)); \
    fflush(stdout);\
    MPI_Abort(MPI_COMM_WORLD, 1); \
} while(0)
#else
#define FAIL(msg) do { \
    fprintf(stdout, "%s: Process %d(%s): FAILED at %d, %s: %s\n",\
        timestamp(), rank, hostname, __LINE__, \
        msg, strerror(errno)); \
    fflush(stdout);\
    MPI_Abort(MPI_COMM_WORLD, 1); \
} while(0)
#endif

char *timestamp() {
    static char datestring[80];
    time_t timestamp;

    fflush(stdout);
    timestamp = time(NULL);
    strftime(datestring, 80, "%m/%d/%Y %T", localtime(&timestamp));

    return datestring;
}

int count_tasks_per_node(void) {
    char       localhost[MAX_LEN],
               hostname[MAX_LEN];
    int        count               = 1,
               i;
    MPI_Status status;

    if (gethostname(localhost, MAX_LEN) != 0) {
        FAIL("gethostname()");
    }
    if (rank == 0) {
        /* MPI_receive all hostnames, and compare to local hostname */
        for (i = 0; i < size-1; i++) {
            MPI_Recv(hostname, MAX_LEN, MPI_CHAR, MPI_ANY_SOURCE,
                     MPI_ANY_TAG, MPI_COMM_WORLD, &status);
            if (strcmp(hostname, localhost) == 0) {
                count++;
            }
        }
    } else {
        /* MPI_send hostname to root node */
        MPI_Send(localhost, MAX_LEN, MPI_CHAR, 0, 0, MPI_COMM_WORLD);
    }
    MPI_Bcast(&count, 1, MPI_INT, 0, MPI_COMM_WORLD);

    return(count);
}

void delay_secs(int delay) {
    if (rank == 0 && delay > 0) {
        if (verbose >= 1) {
            fprintf(stdout, "delaying %d seconds . . .\n", delay);
            fflush(stdout);
        }
        sleep(delay);
    }
    MPI_Barrier(testcomm);
}

void offset_timers(double * t, int tcount) {
    double toffset;
    int i;

    toffset = MPI_Wtime() - t[tcount];
    for (i = 0; i < tcount+1; i++) {
        t[i] += toffset;
    }
}

void parse_dirpath(char *dirpath_arg) {
    char * tmp, * token;
    char delimiter_string[3] = { '@', '\n', '\0' };
    int i = 0;

    tmp = dirpath_arg;

    if (* tmp != '\0') path_count++;
    while (* tmp != '\0') {
        if (* tmp == '@') {
             path_count++;
        }
        tmp++;
    }
    filenames = (char **)malloc(path_count * sizeof(char **));
    if (filenames == NULL) {
        FAIL("out of memory");
    }

    token = strtok(dirpath_arg, delimiter_string);
    while (token != NULL) {
        filenames[i] = token;
        token = strtok(NULL, delimiter_string);
        i++;
    }
}

void unique_dir_access(int opt) {
    if (opt == MK_UNI_DIR) {
        MPI_Barrier(testcomm);
        if (chdir(unique_chdir_dir) == -1) {
            FAIL("Unable to chdir to unique test directory");
        }
    } else if (opt == STAT_SUB_DIR) {
        if (chdir(unique_stat_dir) == -1) {
            FAIL("Unable to chdir to test directory");
        }
    } else if (opt == READ_SUB_DIR) {
        if (chdir(unique_read_dir) == -1) {
            FAIL("Unable to chdir to test directory");
        }
    } else if (opt == RM_SUB_DIR) {
        if (chdir(unique_rm_dir) == -1) {
            FAIL("Unable to chdir to test directory");
        }
    } else if (opt == RM_UNI_DIR) {
        if (chdir(unique_rm_uni_dir) == -1) {
            FAIL("Unable to chdir to test directory");
        }
    }
}

/* helper for creating/removing items */
void create_remove_items_helper(int dirs,
                                int create, char* path, int itemNum) {

    int i;
    char curr_item[MAX_LEN];

    for (i=0; i<items_per_dir; i++) {
        if (dirs) {
            if (create) {

                if (rank == 0 && verbose >= 3 
                    && (itemNum+i) % ITEM_COUNT==0 && (itemNum+i != 0)) {
                    printf("create dir: %d\n", itemNum+i);
                    fflush(stdout);
                }

                //create dirs
                sprintf(curr_item, "%sdir.%s%d", path, mk_name, itemNum+i);
                if (rank == 0 && verbose >= 2) {
                    printf("create dir : %s\n", curr_item);
                    fflush(stdout);
                }
                if (mkdir(curr_item, DIRMODE) == -1) {
                    FAIL("unable to create directory");
                }

            } else {

                if (rank == 0 && verbose >= 3 
                    && (itemNum+i) % ITEM_COUNT==0 && (itemNum+i != 0)) {
                    printf("remove dir: %d\n", itemNum+i);
                    fflush(stdout);
                }

                //remove dirs
                sprintf(curr_item, "%sdir.%s%d", path, rm_name, itemNum+i);
                if (rank == 0 && verbose >= 2) {
                    printf("remove dir : %s\n", curr_item);
                    fflush(stdout);
                }
                if (rmdir(curr_item) == -1) {
                    FAIL("unable to remove directory");
                }
            }

        } else {

            int fd;
            if (create) {

                if (rank == 0 && verbose >= 3 
                    && (itemNum+i) % ITEM_COUNT==0 && (itemNum+i != 0)) {
                    printf("create file: %d\n", itemNum+i);
                    fflush(stdout);
                }

                //create files
                sprintf(curr_item, "%sfile.%s%d", path, mk_name, itemNum+i);
                if (rank == 0 && verbose >= 2) {
                    printf("create file: %s\n", curr_item);
                    fflush(stdout);
                }
                if (collective_creates) {
                    if ((fd = open(curr_item, O_RDWR)) == -1) {
                        FAIL("unable to open file");
                    }
                } else {
                    if (shared_file) {
                        if ((fd = open(curr_item, 
                                       O_CREAT|O_RDWR, FILEMODE)) == -1) {
                            FAIL("unable to create file");
                        }                        
                    } else {
                        if ((fd = creat(curr_item, FILEMODE)) == -1) {
                            FAIL("unable to create file");
                        }
                    }
                }

                if (write_bytes > 0) {
                    if (write(fd, write_buffer, write_bytes) != write_bytes)
                        FAIL("unable to write file");
                }

                if (sync_file && fsync(fd) == -1) {
                    FAIL("unable to sync file");
                }

                if (close(fd) == -1) {
                    FAIL("unable to close file");
                }

            } else {

                if (rank == 0 && verbose >= 3 
                    && (itemNum+i) % ITEM_COUNT==0 && (itemNum+i != 0)) {
                    printf("remove file: %d\n", itemNum+i);
                    fflush(stdout);
                }

                //remove files
                sprintf(curr_item, "%sfile.%s%d", path, rm_name, itemNum+i);
                if (rank == 0 && verbose >= 2) {
                    printf("remove file: %s\n", curr_item);
                    fflush(stdout);
                }
                if (!(shared_file && rank != 0)) {
                    if (unlink(curr_item) == -1) {
                       FAIL("unable to unlink file");
                    }
                }
            }
        }
    }
}

/* helper function to do collective operations */
void collective_helper(int dirs, int create, char* path, int itemNum) {

    int i;
    char curr_item[MAX_LEN];
    for (i=0; i<items_per_dir; i++) {
        if (dirs) {
            if (create) {

                //create dirs
                sprintf(curr_item, "%sdir.%s%d", path, mk_name, itemNum+i);
                if (rank == 0 && verbose >= 2) {
                    printf("create dir : %s\n", curr_item);
                    fflush(stdout);
                }
                if (mkdir(curr_item, DIRMODE) == -1) {
                    FAIL("unable to create directory");
                }

            } else {

                //remove dirs
                sprintf(curr_item, "%sdir.%s%d", path, rm_name, itemNum+i);
                if (rank == 0 && verbose >= 2) {
                    printf("remove dir : %s\n", curr_item);
                    fflush(stdout);
                }
                if (rmdir(curr_item) == -1) {
                    FAIL("unable to remove directory");
                }
            }

        } else {

            int fd;
            if (create) {

                //create files
                sprintf(curr_item, "%sfile.%s%d", path, mk_name, itemNum+i);
                if (rank == 0 && verbose >= 2) {
                    printf("create file: %s\n", curr_item);
                    fflush(stdout);
                }
                if ((fd = creat(curr_item, FILEMODE)) == -1) {
                    FAIL("unable to create file");
                }
                if (close(fd) == -1) {
                    FAIL("unable to close file");
                }

            } else {

                //remove files
                sprintf(curr_item, "%sfile.%s%d", path, rm_name, itemNum+i);
                if (rank == 0 && verbose >= 2) {
                    printf("remove file: %s\n", curr_item);
                    fflush(stdout);
                }
                if (!(shared_file && rank != 0)) {
                    if (unlink(curr_item) == -1) {
                        FAIL("unable to unlink file");
                    }
                }
            }
        }
    }
}

/* recusive function to create and remove files/directories from the 
   directory tree */
void create_remove_items(int currDepth, int dirs, int create, int collective, 
                         char *path, int dirNum) {

	int i;
	char dir[MAX_LEN];
	memset(dir, 0, MAX_LEN);
	
	if (currDepth == 0) {

	    /* create items at this depth */
	    if (!leaf_only || (depth == 0 && leaf_only)) {
            if (collective) {
                collective_helper(dirs, create, dir, 0);
            } else {
		        create_remove_items_helper(dirs, create, dir, 0);
		    }
	    }

	    if (depth > 0) {
		    create_remove_items(++currDepth, dirs, create, 
                                collective, dir, ++dirNum);
	    }

	} else if (currDepth <= depth) {

		char temp_path[MAX_LEN];
		strcpy(temp_path, path);
		int currDir = dirNum;

		/* iterate through the branches */
		for (i=0; i<branch_factor; i++) {

		    /* determine the current branch and append it to the path */
			sprintf(dir, "%s.%d/", base_tree_name, currDir);
			strcat(temp_path, dir);

			/* create the items in this branch */
            if (!leaf_only || (leaf_only && currDepth == depth)) {
                if (collective) {
                    collective_helper(dirs, create, temp_path, 
                                      currDir*items_per_dir);
                } else {
			        create_remove_items_helper(dirs, create, temp_path, 
                                               currDir*items_per_dir);
			    }
			}
			
			/* make the recursive call for the next level below this branch */
			create_remove_items(++currDepth, dirs, create, collective, 
                                temp_path, (currDir*branch_factor)+1);
			currDepth--;

			/* reset the path */
			strcpy(temp_path, path);
			currDir++;
		}
	}
}

/* stats all of the items created as specified by the input parameters */
void mdtest_stat(int random, int dirs) {
	
	struct stat buf;
	int i, parent_dir, item_num = 0;
	char item[MAX_LEN], temp[MAX_LEN];

	/* determine the number of items to stat*/
    int stop = 0;
    if (leaf_only) {
        stop = items_per_dir * pow(branch_factor, depth);
    } else {
        stop = items;
    }

	/* iterate over all of the item IDs */
	for (i = 0; i < stop; i++) {
   	  
		memset(&item, 0, MAX_LEN);
		memset(temp, 0, MAX_LEN);

        /* determine the item number to stat */
        if (random) {
            item_num = rand_array[i];
        } else {
            item_num = i;
        }

		/* make adjustments if in leaf only mode*/
        if (leaf_only) {
            item_num += items_per_dir * 
                (num_dirs_in_tree - pow(branch_factor,depth));
        }
        
		/* create name of file/dir to stat */
        if (dirs) {
            if (rank == 0 && verbose >= 3 && (i%ITEM_COUNT == 0) && (i != 0)) {
                printf("stat dir: %d\n", i);
                fflush(stdout);
            }
            sprintf(item, "dir.%s%d", stat_name, item_num);
        } else {
            if (rank == 0 && verbose >= 3 && (i%ITEM_COUNT == 0) && (i != 0)) {
                printf("stat file: %d\n", i);
                fflush(stdout);
            }
            sprintf(item, "file.%s%d", stat_name, item_num);
        }

        /* determine the path to the file/dir to be stat'ed */
        parent_dir = item_num / items_per_dir;

        if (parent_dir > 0) {        //item is not in tree's root directory

            /* prepend parent directory to item's path */
            sprintf(temp, "%s.%d/%s", base_tree_name, parent_dir, item);
            strcpy(item, temp);
            
            //still not at the tree's root dir
            while (parent_dir > branch_factor) {
                parent_dir = (int) ((parent_dir-1) / branch_factor);
                sprintf(temp, "%s.%d/%s", base_tree_name, parent_dir, item);
                strcpy(item, temp);
            }
        }

        /* below temp used to be hiername */
        if (rank == 0 && verbose >= 2) {
            if (dirs) {
                printf("stat   dir : %s\n", item);
            } else {
                printf("stat   file: %s\n", item);
            }
            fflush(stdout);
        }
        if (stat(item, &buf) == -1) {
            if (dirs) {
                FAIL("unable to stat directory");
            } else {
                FAIL("unable to stat file");
            }
        }
    }
}


/* reads all of the items created as specified by the input parameters */
void mdtest_read(int random, int dirs) {
	
	int i, parent_dir, item_num = 0;
        int fd;
	char item[MAX_LEN], temp[MAX_LEN];

        /* allocate read buffer */
        if (read_bytes > 0) {
            read_buffer = (char *)malloc(read_bytes);
            if (read_buffer == NULL) {
                FAIL("out of memory");
            }
        }

  	/* determine the number of items to read */
        int stop = 0;
        if (leaf_only) {
            stop = items_per_dir * pow(branch_factor, depth);
        } else {
            stop = items;
        }
  
	/* iterate over all of the item IDs */
	for (i = 0; i < stop; i++) {
   	  
		memset(&item, 0, MAX_LEN);
		memset(temp, 0, MAX_LEN);

        /* determine the item number to read */
        if (random) {
            item_num = rand_array[i];
        } else {
            item_num = i;
        }

		/* make adjustments if in leaf only mode*/
        if (leaf_only) {
            item_num += items_per_dir * 
                (num_dirs_in_tree - pow(branch_factor,depth));
        }
        
		/* create name of file to read */
        if (dirs) {
            ; /* N/A */
        } else {
            if (rank == 0 && verbose >= 3 && (i%ITEM_COUNT == 0) && (i != 0)) {
                printf("read file: %d\n", i);
                fflush(stdout);
            }
            sprintf(item, "file.%s%d", read_name, item_num);
        }

        /* determine the path to the file/dir to be read'ed */
        parent_dir = item_num / items_per_dir;

        if (parent_dir > 0) {        //item is not in tree's root directory

            /* prepend parent directory to item's path */
            sprintf(temp, "%s.%d/%s", base_tree_name, parent_dir, item);
            strcpy(item, temp);
            
            //still not at the tree's root dir
            while (parent_dir > branch_factor) {
                parent_dir = (int) ((parent_dir-1) / branch_factor);
                sprintf(temp, "%s.%d/%s", base_tree_name, parent_dir, item);
                strcpy(item, temp);
            }
        }

        /* below temp used to be hiername */
        if (rank == 0 && verbose >= 2) {
            if (dirs) {
                ;
            } else {
                printf("read   file: %s\n", item);
            }
            fflush(stdout);
        }

        /* open file for reading */
        if ((fd = open(item, O_RDWR, FILEMODE)) == -1) {
            FAIL("unable to open file");
        }

        /* read file */
        if (read_bytes > 0) {
            if (read(fd, read_buffer, read_bytes) != read_bytes)
                FAIL("unable to read file");
        }

        /* close file */
        if (close(fd) == -1) {
            FAIL("unable to close file");
        }
    }
}

/* This method should be called by rank 0.  It subsequently does all of
   the creates and removes for the other ranks */
void collective_create_remove(int create, int dirs, int ntasks) {

    int i;
    char temp[MAX_LEN];
    
    /* rank 0 does all of the creates and removes for all of the ranks */
    for (i=0; i<ntasks; i++) {
     
        memset(temp, 0, MAX_LEN);
        
        strcpy(temp, testdir);
        strcat(temp, "/");

        /* set the base tree name appropriately */
        if (unique_dir_per_task) {
            sprintf(base_tree_name, "mdtest_tree.%d", i);
        } else {
            sprintf(base_tree_name, "mdtest_tree");
        }

        /* change to the appropriate test dir */
        strcat(temp, base_tree_name);
        strcat(temp, ".0");
        if (chdir(temp) == -1) {
            FAIL("unable to change to test directory");
        }
        
        /* set all item names appropriately */
        if (!shared_file) {
            sprintf(mk_name, "mdtest.%d.", (i+(0*nstride))%ntasks);
            sprintf(stat_name, "mdtest.%d.", (i+(1*nstride))%ntasks);
            sprintf(read_name, "mdtest.%d.", (i+(2*nstride))%ntasks);
            sprintf(rm_name, "mdtest.%d.", (i+(3*nstride))%ntasks);
        }
        if (unique_dir_per_task) {
            sprintf(unique_mk_dir, "%s/mdtest_tree.%d.0", testdir, 
                    (i+(0*nstride))%ntasks);
            sprintf(unique_chdir_dir, "%s/mdtest_tree.%d.0", testdir, 
                    (i+(1*nstride))%ntasks);
            sprintf(unique_stat_dir, "%s/mdtest_tree.%d.0", testdir, 
                    (i+(2*nstride))%ntasks);
            sprintf(unique_read_dir, "%s/mdtest_tree.%d.0", testdir, 
                    (i+(3*nstride))%ntasks);
            sprintf(unique_rm_dir, "%s/mdtest_tree.%d.0", testdir, 
                    (i+(4*nstride))%ntasks);
            sprintf(unique_rm_uni_dir, "%s", testdir);
        }
        
        /* now that everything is set up as it should be, do the create
           or removes */
        create_remove_items(0, dirs, create, 1, NULL, 0);
    }

    /* have rank 0 change back to its test dir */
    if (chdir(top_dir) == -1) {
        FAIL("unable to change back to back to testdir of rank 0");
    }

    /* reset all of the item names */
    if (unique_dir_per_task) {
        sprintf(base_tree_name, "mdtest_tree.0");
    } else {
        sprintf(base_tree_name, "mdtest_tree");
    }
    if (!shared_file) {
        sprintf(mk_name, "mdtest.%d.", (0+(0*nstride))%ntasks);
        sprintf(stat_name, "mdtest.%d.", (0+(1*nstride))%ntasks);
        sprintf(read_name, "mdtest.%d.", (0+(2*nstride))%ntasks);
        sprintf(rm_name, "mdtest.%d.", (0+(3*nstride))%ntasks);
    }
    if (unique_dir_per_task) {
        sprintf(unique_mk_dir, "%s/mdtest_tree.%d.0", testdir, 
                (0+(0*nstride))%ntasks);
        sprintf(unique_chdir_dir, "%s/mdtest_tree.%d.0", testdir, 
                (0+(1*nstride))%ntasks);
        sprintf(unique_stat_dir, "%s/mdtest_tree.%d.0", testdir, 
                (0+(2*nstride))%ntasks);
        sprintf(unique_read_dir, "%s/mdtest_tree.%d.0", testdir, 
                (0+(3*nstride))%ntasks);
        sprintf(unique_rm_dir, "%s/mdtest_tree.%d.0", testdir, 
                (0+(4*nstride))%ntasks);
        sprintf(unique_rm_uni_dir, "%s", testdir);
    }
    
}

void directory_test(int iteration, int ntasks) {

    int size;
    double t[5] = {0};

    MPI_Barrier(testcomm);
    t[0] = MPI_Wtime();

    /* create phase */
    if(create_only) {
        if (unique_dir_per_task) {
            unique_dir_access(MK_UNI_DIR);
            if (!time_unique_dir_overhead) {
                offset_timers(t, 0);
            }
        }
        
        /* "touch" the files */
        if (collective_creates) {
            if (rank == 0) {
                collective_create_remove(1, 1, ntasks);
            }
        } else {
			/* create directories */
        	create_remove_items(0, 1, 1, 0, NULL, 0);
        }
    }
    
    if (barriers) {
        MPI_Barrier(testcomm);
    }
    t[1] = MPI_Wtime();

    /* stat phase */
    if (stat_only) {
        
        if (unique_dir_per_task) {
            unique_dir_access(STAT_SUB_DIR);
            if (!time_unique_dir_overhead) {
                offset_timers(t, 1);
            }
        }
        
		/* stat directories */
		if (random_seed > 0) {
	        mdtest_stat(1, 1);
        } else {
	        mdtest_stat(0, 1);
        }

    }
    
    if (barriers) {
        MPI_Barrier(testcomm);
    }
    t[2] = MPI_Wtime();

    /* read phase */
    if (read_only) {
        
        if (unique_dir_per_task) {
            unique_dir_access(READ_SUB_DIR);
            if (!time_unique_dir_overhead) {
                offset_timers(t, 2);
            }
        }
        
		/* read directories */
		if (random_seed > 0) {
	        ;	/* N/A */
        } else {	
	        ;	/* N/A */
        }

    }
    
    if (barriers) {
        MPI_Barrier(testcomm);
    }
    t[3] = MPI_Wtime();
    
    if (remove_only) {
       if (unique_dir_per_task) {
            unique_dir_access(RM_SUB_DIR);
            if (!time_unique_dir_overhead) {
                offset_timers(t, 3);
            }
        }
    }

    /* remove phase */
    if (remove_only) {

        /* remove directories */
        if (collective_creates) {
            if (rank == 0) {
                collective_create_remove(0, 1, ntasks);
            }
        } else {
        	create_remove_items(0, 1, 0, 0, NULL, 0);
        }
    }

    if (barriers) {
        MPI_Barrier(testcomm);
    }
    t[4] = MPI_Wtime();
    
    if (remove_only) {
        if (unique_dir_per_task) {
            unique_dir_access(RM_UNI_DIR);
        }
    }
    if (unique_dir_per_task && !time_unique_dir_overhead) {
        offset_timers(t, 4);
    }

    MPI_Comm_size(testcomm, &size);

    /* calculate times */
    if (create_only) {    
        summary_table[iteration].entry[0] = items*size/(t[1] - t[0]);
    } else {
        summary_table[iteration].entry[0] = 0;        
    }
    if (stat_only) {
        summary_table[iteration].entry[1] = items*size/(t[2] - t[1]);
    } else {
        summary_table[iteration].entry[1] = 0;
    }
    if (read_only) {
        summary_table[iteration].entry[2] = items*size/(t[3] - t[2]);
    } else {
        summary_table[iteration].entry[2] = 0;
    }
    if (remove_only) {
        summary_table[iteration].entry[3] = items*size/(t[4] - t[3]);
    } else {
        summary_table[iteration].entry[3] = 0;
    }
        
    if (verbose >= 1 && rank == 0) {
        printf("   Directory creation: %10.3f sec, %10.3f ops/sec\n",
              t[1] - t[0], summary_table[iteration].entry[0]);
        printf("   Directory stat    : %10.3f sec, %10.3f ops/sec\n",
              t[2] - t[1], summary_table[iteration].entry[1]);
/* N/A
        printf("   Directory read    : %10.3f sec, %10.3f ops/sec\n",
              t[3] - t[2], summary_table[iteration].entry[2]);
*/
        printf("   Directory removal : %10.3f sec, %10.3f ops/sec\n",
              t[4] - t[3], summary_table[iteration].entry[3]);
        fflush(stdout);
    }
}

void file_test(int iteration, int ntasks) {
    int size;
    double t[5] = {0};

    MPI_Barrier(testcomm);
    t[0] = MPI_Wtime();
        
    /* create phase */
    if (create_only) {
        if (unique_dir_per_task) {
            unique_dir_access(MK_UNI_DIR);
            if (!time_unique_dir_overhead) {
                offset_timers(t, 0);
            }
        }
        
        /* "touch" the files */
        if (collective_creates) {
            if (rank == 0) {
                collective_create_remove(1, 0, ntasks);
            }
            MPI_Barrier(testcomm);
        }
        
        /* create files */    
        create_remove_items(0, 0, 1, 0, NULL, 0);

    }

    if (barriers) {
        MPI_Barrier(testcomm);
    }
    t[1] = MPI_Wtime();

    /* stat phase */
    if (stat_only) {
    
        if (unique_dir_per_task) {
            unique_dir_access(STAT_SUB_DIR);
            if (!time_unique_dir_overhead) {
                offset_timers(t, 1);
            }
        }
        
		/* stat files */
		if (random_seed > 0) {
    	    mdtest_stat(1,0);
		} else {
    	    mdtest_stat(0,0);
		}
    }

    if (barriers) {
        MPI_Barrier(testcomm);
    }
    t[2] = MPI_Wtime();

    /* read phase */
    if (read_only) {
    
        if (unique_dir_per_task) {
            unique_dir_access(READ_SUB_DIR);
            if (!time_unique_dir_overhead) {
                offset_timers(t, 2);
            }
        }
        
		/* read files */
		if (random_seed > 0) {
    	    mdtest_read(1,0);
		} else {
    	    mdtest_read(0,0);
		}
    }

    if (barriers) {
        MPI_Barrier(testcomm);
    }
    t[3] = MPI_Wtime();
    
    if (remove_only) {
        if (unique_dir_per_task) {
            unique_dir_access(RM_SUB_DIR);
            if (!time_unique_dir_overhead) {
                offset_timers(t, 3);
            }
        }
    }

    /* remove phase */
    if (remove_only) {
        if (collective_creates) {
            if (rank == 0) {
                collective_create_remove(0, 0, ntasks);
            }
        } else {
        	create_remove_items(0, 0, 0, 0, NULL, 0);
        }
    }

    if (barriers) {
        MPI_Barrier(testcomm);
    }
    t[4] = MPI_Wtime();
    
    if (remove_only) {
        if (unique_dir_per_task) {
            unique_dir_access(RM_UNI_DIR);
        }
    }
    if (unique_dir_per_task && !time_unique_dir_overhead) {
        offset_timers(t, 4);
    }
    
    MPI_Comm_size(testcomm, &size);

    /* calculate times */
    if (create_only) {
        summary_table[iteration].entry[4] = items*size/(t[1] - t[0]);
    } else {
        summary_table[iteration].entry[4] = 0;
    }
    if (stat_only) {
        summary_table[iteration].entry[5] = items*size/(t[2] - t[1]);
    } else {
        summary_table[iteration].entry[5] = 0;
    }
    if (read_only) {
        summary_table[iteration].entry[6] = items*size/(t[3] - t[2]);
    } else {
        summary_table[iteration].entry[6] = 0;
    }
    if (remove_only) {
        summary_table[iteration].entry[7] = items*size/(t[4] - t[3]);
    } else {
        summary_table[iteration].entry[7] = 0;
    }

    if (verbose >= 1 && rank == 0) {
        printf("   File creation     : %10.3f sec, %10.3f ops/sec\n",
           t[1] - t[0], summary_table[iteration].entry[4]);
        printf("   File stat         : %10.3f sec, %10.3f ops/sec\n",
           t[2] - t[1], summary_table[iteration].entry[5]);
        printf("   File read         : %10.3f sec, %10.3f ops/sec\n",
           t[3] - t[2], summary_table[iteration].entry[6]);
        printf("   File removal      : %10.3f sec, %10.3f ops/sec\n",
           t[4] - t[3], summary_table[iteration].entry[7]);
        fflush(stdout);
    }
}

void print_help() {
    char * opts[] = {
"Usage: mdtest [-b branching_factor] [-B] [-c] [-C] [-d testdir] [-D] [-e number_of_bytes_to_read]",
"              [-E] [-f first] [-F] [-h] [-i iterations] [-I items_per_dir] [-l last] [-L]",
"              [-n number_of_items] [-N stride_length] [-p seconds] [-r]",
"              [-R[seed]] [-s stride] [-S] [-t] [-T] [-u] [-v]",
"              [-V verbosity_value] [-w number_of_bytes_to_write] [-y] [-z depth]",
"\t-b: branching factor of hierarchical directory structure",
"\t-B: no barriers between phases",
"\t-c: collective creates: task 0 does all creates",
"\t-C: only create files/dirs",
"\t-d: the directory in which the tests will run",
"\t-D: perform test on directories only (no files)",
"\t-e: bytes to read from each file",
"\t-E: only read files/dir",
"\t-f: first number of tasks on which the test will run",
"\t-F: perform test on files only (no directories)",
"\t-h: prints this help message",
"\t-i: number of iterations the test will run",
"\t-I: number of items per directory in tree",
"\t-l: last number of tasks on which the test will run",
"\t-L: files only at leaf level of tree",
"\t-n: every process will creat/stat/read/remove # directories and files",
"\t-N: stride # between neighbor tasks for file/dir operation (local=0)",
"\t-p: pre-iteration delay (in seconds)",
"\t-r: only remove files or directories left behind by previous runs",
"\t-R: randomly stat files (optional argument for random seed)",
"\t-s: stride between the number of tasks for each test",
"\t-S: shared file access (file only, no directories)",
"\t-t: time unique working directory overhead",
"\t-T: only stat files/dirs",
"\t-u: unique working directory for each task",
"\t-v: verbosity (each instance of option increments by one)",
"\t-V: verbosity value",
"\t-w: bytes to write to each file after it is created",
"\t-y: sync file after writing",
"\t-z: depth of hierarchical directory structure",
""
};
    int i, j;

    for (i = 0; strlen(opts[i]) > 0; i++)
       	printf("%s\n", opts[i]);
   	fflush(stdout);

    MPI_Initialized(&j);
    if (j) {
        MPI_Finalize();
    }
    exit(0);
}

void summarize_results(int iterations) {
    char access[MAX_LEN];
    int i, j, k;
    int start, stop, tableSize = 10;
    double min, max, mean, sd, sum = 0, var = 0, curr = 0;

    double all[iterations * size * tableSize];
    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Gather(&summary_table->entry[0], tableSize*iterations, 
               MPI_DOUBLE, all, tableSize*iterations, MPI_DOUBLE, 
               0, MPI_COMM_WORLD);

    if (rank == 0) {

        printf("\nSUMMARY: (of %d iterations)\n", iterations);
        printf(
            "   Operation                  Max        Min       Mean    Std Dev\n");
        printf(
            "   ---------                  ---        ---       ----    -------\n");
        fflush(stdout);
        
        /* if files only access, skip entries 0-3 (the dir tests) */
        if (files_only && !dirs_only) {
            start = 4;
        } else {
            start = 0;
        }
    
        /* if directories only access, skip entries 4-7 (the file tests) */
        if (dirs_only && !files_only) {
            stop = 4;
        } else {
            stop = 8;
        }

        /* special case: if no directory or file tests, skip all */
        if (!dirs_only && !files_only) {
            start = stop = 0;
        }

        /* calculate aggregates */
        if (barriers) {
            double maxes[iterations];
            

            /* Because each proc times itself, in the case of barriers we
             * have to backwards calculate the time to simulate the use
             * of barriers.
             */
            for (i = start; i < stop; i++) {
                for (j=0; j<iterations; j++) {
                    maxes[j] = all[j*tableSize + i];
                    for (k=0; k<size; k++) {                    
                        curr = all[(k*tableSize*iterations) 
                                   + (j*tableSize) + i];
                        if (maxes[j] < curr) {
                            maxes[j] = curr;
                        }
                    }
                }
            
                min = max = maxes[0];
                for (j=0; j<iterations; j++) {
                    if (min > maxes[j]) {
                        min = maxes[j];
                    }
                    if (max < maxes[j]) {
                        max = maxes[j];
                    }
                    sum += maxes[j];
                }
                mean = sum / iterations;
                for (j=0; j<iterations; j++) {
                    var += pow((mean - maxes[j]), 2);
                }
                var = var / iterations;
                sd = sqrt(var);
                switch (i) {
                    case 0: strcpy(access, "Directory creation:"); break;
                    case 1: strcpy(access, "Directory stat    :"); break;
                    /* case 2: strcpy(access, "Directory read    :"); break; */
                    case 2: ;                                      break; /* N/A */
                    case 3: strcpy(access, "Directory removal :"); break;
                    case 4: strcpy(access, "File creation     :"); break;
                    case 5: strcpy(access, "File stat         :"); break;
                    case 6: strcpy(access, "File read         :"); break;
                    case 7: strcpy(access, "File removal      :"); break;
                   default: strcpy(access, "ERR");                 break;
                }
                if (i != 2) {
                    printf("   %s ", access);
                    printf("%10.3f ", max);
                    printf("%10.3f ", min);
                    printf("%10.3f ", mean);
                    printf("%10.3f\n", sd);
                    fflush(stdout);
                }
                sum = var = 0;
                
            }
            
        } else {
            for (i = start; i < stop; i++) {
                min = max = all[i];
                for (k=0; k < size; k++) {
                    for (j = 0; j < iterations; j++) {
                        curr = all[(k*tableSize*iterations) 
                                   + (j*tableSize) + i];
                        if (min > curr) {
                            min = curr;
                        }
                        if (max < curr) {
                            max =  curr;
                        }
                        sum += curr;
                    }
                }
                mean = sum / (iterations * size);
                for (k=0; k<size; k++) {
                    for (j = 0; j < iterations; j++) {
                        var += pow((mean -  all[(k*tableSize*iterations) 
                                                + (j*tableSize) + i]), 2);
                    }
                }
                var = var / (iterations * size);
                sd = sqrt(var);
                switch (i) {
                    case 0: strcpy(access, "Directory creation:"); break;
                    case 1: strcpy(access, "Directory stat    :"); break;
                    /* case 2: strcpy(access, "Directory read    :"); break; */
                    case 2: ;                                      break; /* N/A */
                    case 3: strcpy(access, "Directory removal :"); break;
                    case 4: strcpy(access, "File creation     :"); break;
                    case 5: strcpy(access, "File stat         :"); break;
                    case 6: strcpy(access, "File read         :"); break;
                    case 7: strcpy(access, "File removal      :"); break;
                   default: strcpy(access, "ERR");                 break;
                }
                if (i != 2) {
                    printf("   %s ", access);
                    printf("%10.3f ", max);
                    printf("%10.3f ", min);
                    printf("%10.3f ", mean);
                    printf("%10.3f\n", sd);
                    fflush(stdout);
                }
                sum = var = 0;
                
            }
        }
        
        /* calculate tree create/remove rates */
        for (i = 8; i < tableSize; i++) {
            min = max = all[i];
            for (j = 0; j < iterations; j++) {
                curr = summary_table[j].entry[i];
                if (min > curr) {
                    min = curr;
                }
                if (max < curr) {
                    max =  curr;
                }
                sum += curr;
            }
            mean = sum / (iterations);
            for (j = 0; j < iterations; j++) {
                var += pow((mean -  summary_table[j].entry[i]), 2);
            }
            var = var / (iterations);
            sd = sqrt(var);
            switch (i) {
                case 8: strcpy(access, "Tree creation     :"); break;
                case 9: strcpy(access, "Tree removal      :"); break;
               default: strcpy(access, "ERR");                 break;
            }
            printf("   %s ", access);
            printf("%10.3f ", max);
            printf("%10.3f ", min);
            printf("%10.3f ", mean);
            printf("%10.3f\n", sd);
            fflush(stdout);
            sum = var = 0;
        }
    }
}

/* Checks to see if the test setup is valid.  If it isn't, fail. */
void valid_tests() {

    /* if dirs_only and files_only were both left unset, set both now */
    if (!dirs_only && !files_only) {
        dirs_only = files_only = 1;
    }

    /* if shared file 'S' access, no directory tests */
    if (shared_file) {
        dirs_only = 0;
    }

    /* check for collective_creates incompatibilities */
    if (shared_file && collective_creates && rank == 0) {
        FAIL("-c not compatible with -S");
    }
    if (path_count > 1 && collective_creates && rank == 0) {
        FAIL("-c not compatible with multiple test directories");
    }
    if (collective_creates && !barriers) {
        FAIL("-c not compatible with -B");
    }
    
    /* check for shared file incompatibilities */
    if (unique_dir_per_task && shared_file && rank == 0) {
        FAIL("-u not compatible with -S");
    }

    /* check multiple directory paths and strided option */
    if (path_count > 1 && nstride > 0) {
        FAIL("cannot have multiple directory paths with -N strides between neighbor tasks");
    }
    
    /* check for shared directory and multiple directories incompatibility */
    if (path_count > 1 && unique_dir_per_task != 1) {
        FAIL("shared directory mode is not compatible with multiple directory paths");
    }

    /* check if more directory paths than ranks */
    if (path_count > size) {
        FAIL("cannot have more directory paths than MPI tasks");
    }
    
    /* check depth */
    if (depth < 0) {
    	FAIL("depth must be greater than or equal to zero");
    }
    /* check branch_factor */
    if (branch_factor < 1 && depth > 0) {
    	FAIL("branch factor must be greater than or equal to zero");
    }
    /* check for valid number of items */
    if ((items > 0) && (items_per_dir > 0)) {
    	FAIL("only specify the number of items or the number of items per directory");
    }

}

void show_file_system_size(char *file_system) {
    char          real_path[MAX_LEN];
    char          file_system_unit_str[MAX_LEN] = "GiB";
    char          inode_unit_str[MAX_LEN]       = "Mi";
    long long int file_system_unit_val          = 1024 * 1024 * 1024;
    long long int inode_unit_val                = 1024 * 1024;
    long long int total_file_system_size,
                  free_file_system_size,
                  total_inodes,
                  free_inodes;
    double        total_file_system_size_hr,
                  used_file_system_percentage,
                  used_inode_percentage;
    struct statfs status_buffer;

    if (statfs(file_system, &status_buffer) != 0) {
        FAIL("unable to statfs() file system");
    }

    /* data blocks */
    total_file_system_size = status_buffer.f_blocks * status_buffer.f_bsize;
    free_file_system_size = status_buffer.f_bfree * status_buffer.f_bsize;
    used_file_system_percentage = (1 - ((double)free_file_system_size
                                  / (double)total_file_system_size)) * 100;
    total_file_system_size_hr = (double)total_file_system_size
                                / (double)file_system_unit_val;
    if (total_file_system_size_hr > 1024) {
        total_file_system_size_hr = total_file_system_size_hr / 1024;
        strcpy(file_system_unit_str, "TiB");
    }

    /* inodes */
    total_inodes = status_buffer.f_files;
    free_inodes = status_buffer.f_ffree;
    used_inode_percentage = (1 - ((double)free_inodes/(double)total_inodes))
                            * 100;

    /* show results */
    if (realpath(file_system, real_path) == NULL) {
        FAIL("unable to use realpath()");
    }
    fprintf(stdout, "Path: %s\n", real_path);
    fprintf(stdout, "FS: %.1f %s   Used FS: %2.1f%%   ",
            total_file_system_size_hr, file_system_unit_str,
            used_file_system_percentage);
    fprintf(stdout, "Inodes: %.1f %s   Used Inodes: %2.1f%%\n",
           (double)total_inodes / (double)inode_unit_val,
           inode_unit_str, used_inode_percentage);
    fflush(stdout);

    return;
}

void display_freespace(char *testdirpath)
{
    char dirpath[MAX_LEN] = {0};
    int  i;
    int  directoryFound   = 0;

    strcpy(dirpath, testdirpath);

    /* get directory for outfile */
    i = strlen(dirpath);
    while (i-- > 0) {
        if (dirpath[i] == '/') {
            dirpath[i] = '\0';
            directoryFound = 1;
            break;
        }
    }

    /* if no directory/, use '.' */
    if (directoryFound == 0) {
        strcpy(dirpath, ".");
    }

    show_file_system_size(dirpath);

    return;
}

void create_remove_directory_tree(int create, 
                                  int currDepth, char* path, int dirNum) {

	int i;
	char dir[MAX_LEN];

	if (currDepth == 0) {

	    sprintf(dir, "%s.%d/", base_tree_name, dirNum);

		if (create) {
			if (rank == 0 && verbose >= 2) {
				printf("making: %s\n", dir);
				fflush(stdout);
			}
			if (mkdir(dir, DIRMODE) == -1) {
				FAIL("Unable to create directory");
			}
		}

		create_remove_directory_tree(create, ++currDepth, dir, ++dirNum);

		if (!create) {
			if (rank == 0 && verbose >= 2) {
				printf("remove: %s\n", dir);
				fflush(stdout);
			}
			if (rmdir(dir) == -1) {
				FAIL("Unable to remove directory");
			}
		}

	} else if (currDepth <= depth) {
	
		char temp_path[MAX_LEN];
		strcpy(temp_path, path);
		int currDir = dirNum;

		for (i=0; i<branch_factor; i++) {

			sprintf(dir, "%s.%d/", base_tree_name, currDir);
			strcat(temp_path, dir);

			if (create) {
				if (rank == 0 && verbose >= 2) {
					printf("making: %s\n", temp_path);
					fflush(stdout);
				}
				if (mkdir(temp_path, DIRMODE) == -1) {
					FAIL("Unable to create directory");
				}
			}

			create_remove_directory_tree(create, ++currDepth, 
                                         temp_path, (branch_factor*currDir)+1);
			currDepth--;
			
			if (!create) {
				if (rank == 0 && verbose >= 2) {
					printf("remove: %s\n", temp_path);
					fflush(stdout);
				}
				if (rmdir(temp_path) == -1) {
					FAIL("Unable to remove directory");
				}
			}
			
			strcpy(temp_path, path);
            currDir++;
		}
	}
}

int main(int argc, char **argv) {
    int i, j, c;
    int nodeCount;
    MPI_Group worldgroup, testgroup;
    struct {
        int first;
        int last;
        int stride;
    } range = {0, 0, 1};
    int first = 1;
    int last = 0;
    int stride = 1;
    int iterations = 1;

    /* Check for -h parameter before MPI_Init so the mdtest binary can be
       called directly, without, for instance, mpirun. */
    for (i = 1; i < argc; i++) {
        if (!strcmp(argv[i], "-h") || !strcmp(argv[i], "--help")) {
            print_help();
        }
    }

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    nodeCount = size / count_tasks_per_node();

    if (rank == 0) {
        printf("-- started at %s --\n\n", timestamp());
        printf("mdtest-%s was launched with %d total task(s) on %d nodes\n",
                RELEASE_VERS, size, nodeCount);
        fflush(stdout);
    }

    if (rank == 0) {
        fprintf(stdout, "Command line used:");
        for (i = 0; i < argc; i++) {
            fprintf(stdout, " %s", argv[i]);
        }
        fprintf(stdout, "\n");
        fflush(stdout);
    }

    /* Parse command line options */
    while (1) {
        c = getopt(argc, argv, "b:BcCd:De:Ef:Fhi:I:l:Ln:N:p:rR::s:StTuvV:w:yz:");
        if (c == -1) {
            break;
        }

        switch (c) {
        	case 'b':
        		branch_factor = atoi(optarg); break;
            case 'B':
                barriers = 0;                 break;
            case 'c':
                collective_creates = 1;       break;
            case 'C':
                create_only = 1;              break;
            case 'd':
                parse_dirpath(optarg);        break;
            case 'D':
                dirs_only = 1;                break;
            case 'e':
                read_bytes = atoi(optarg);    break;
            case 'E':
                read_only = 1;                break;
            case 'f':
                first = atoi(optarg);         break;
            case 'F':
                files_only = 1;               break;
            case 'h':
                print_help();                 break;
            case 'i':
                iterations = atoi(optarg);    break;
            case 'I':
            	items_per_dir = atoi(optarg); break;
            case 'l':
                last = atoi(optarg);          break;
            case 'L':
                leaf_only = 1;                break;
            case 'n':
                items = atoi(optarg);         break;
            case 'N':
                nstride = atoi(optarg);       break;
            case 'p':
                pre_delay = atoi(optarg);     break;
            case 'r':
                remove_only = 1;              break;
            case 'R':
                if (optarg == NULL) {
                    random_seed = time(NULL);
                    MPI_Barrier(MPI_COMM_WORLD);
                    MPI_Bcast(&random_seed, 1, MPI_INT, 0, MPI_COMM_WORLD);
                    random_seed += rank;
                } else {
            	    random_seed = atoi(optarg)+rank;
                }
                                        	  break;
            case 's':
                stride = atoi(optarg);        break;
            case 'S':
                shared_file = 1;              break;
            case 't':
                time_unique_dir_overhead = 1; break;
            case 'T':
                stat_only = 1;                break;
            case 'u':
                unique_dir_per_task = 1;      break;
            case 'v':
                verbose += 1;                 break;
            case 'V':
                verbose = atoi(optarg);       break;
            case 'w':
                write_bytes = atoi(optarg);   break;
            case 'y':
                sync_file = 1;                break;
            case 'z':
            	depth = atoi(optarg);		  break;
        }
    }

    if (!create_only && !stat_only && !read_only && !remove_only) {
        create_only = stat_only = read_only = remove_only = 1;
    }
    
    valid_tests();

    /* setup total number of items and number of items per dir */
    if (depth <= 0) {
        num_dirs_in_tree = 1;
    } else {
        if (branch_factor < 1) {
            num_dirs_in_tree = 1;
        } else if (branch_factor == 1) {
            num_dirs_in_tree = depth + 1;
        } else {
            num_dirs_in_tree = 
                (1 - pow(branch_factor, depth+1)) / (1 - branch_factor);
        }
    }
    if (items_per_dir > 0) {
        items = items_per_dir * num_dirs_in_tree;
    } else {
        if (leaf_only) {
            if (branch_factor <= 1) {
                items_per_dir = items;
            } else {
                items_per_dir = items / pow(branch_factor, depth);
                items = items_per_dir * pow(branch_factor, depth);
            }
        } else {
            items_per_dir = items / num_dirs_in_tree;
            items = items_per_dir * num_dirs_in_tree;
        }
    }

    /* initialize rand_array */
    if (random_seed > 0) {
        srand(random_seed);
        
        int stop = 0;
        if (leaf_only) {
            stop = items_per_dir * pow(branch_factor, depth);
        } else {
            stop = items;
        }
        rand_array = (int*) malloc(stop * sizeof(int));
        
        for (i=0; i<stop; i++) {
            rand_array[i] = i;
        }

        /* shuffle list randomly */
        int n = stop;
        while (n>1) {
            n--;
            int k = rand() % (n+1);
            int tmp = rand_array[k];
            rand_array[k] = rand_array[n];
            rand_array[n] = tmp;
        }
    }
	
    /* allocate and initialize write buffer with # */
    if (write_bytes > 0) {
        write_buffer = (char *)malloc(write_bytes);
        if (write_buffer == NULL) {
            FAIL("out of memory");
        }
        memset(write_buffer, 0x23, write_bytes);
    }

    /* setup directory path to work in */
    if (path_count == 0) { /* special case where no directory path provided with '-d' option */
        getcwd(testdirpath, MAX_LEN);
        path_count = 1;
    } else {
        strcpy(testdirpath, filenames[rank%path_count]);
    }

    /* display disk usage */
    if (rank == 0) display_freespace(testdirpath);
    
    if (rank == 0) {
        if (random_seed > 0) {
            printf("random seed: %d\n", random_seed);
        }
    }

    /*   if directory does not exist, create it */
    if ((rank < path_count) && chdir(testdirpath) == -1) {
        if (mkdir(testdirpath, DIRMODE) == - 1) {
            FAIL("Unable to create test directory path");
        }
    }

    if (gethostname(hostname, MAX_LEN) == -1) {
        perror("gethostname");
        MPI_Abort(MPI_COMM_WORLD, 2);
    }
    if (last == 0) {
        first = size;
        last = size;
    }

    /* setup summary table for recording results */
    summary_table = (table_t *)malloc(iterations * sizeof(table_t));
    if (summary_table == NULL) {
        FAIL("out of memory");
    }

    if (unique_dir_per_task) {
        sprintf(base_tree_name, "mdtest_tree.%d", rank);
    } else {
        sprintf(base_tree_name, "mdtest_tree");
    }

    /* start and end times of directory tree create/remove */
    double startCreate, endCreate;

    /* default use shared directory */
    strcpy(mk_name, "mdtest.shared.");
    strcpy(stat_name, "mdtest.shared.");
    strcpy(read_name, "mdtest.shared.");
    strcpy(rm_name, "mdtest.shared.");
    
    MPI_Comm_group(MPI_COMM_WORLD, &worldgroup);
    /* Run the tests */
    for (i = first; i <= last && i <= size; i += stride) {
        range.last = i - 1;
        MPI_Group_range_incl(worldgroup, 1, (void *)&range, &testgroup);
        MPI_Comm_create(MPI_COMM_WORLD, testgroup, &testcomm);
        if (rank == 0) {
            if (files_only && dirs_only) {
                printf("\n%d tasks, %d files/directories\n", i, i * items);
            } else if (files_only) {
                printf("\n%d tasks, %d files\n", i, i * items);
            } else if (dirs_only) {
                printf("\n%d tasks, %d directories\n", i, i * items);
            }
        }
        if (rank == 0 && verbose >= 1) {
            printf("\n");
            printf("   Operation               Duration              Rate\n");
            printf("   ---------               --------              ----\n");
        }
        for (j = 0; j < iterations; j++) {
            if (rank == 0 && verbose >= 1) {
                printf(" * iteration %d *\n", j+1);
                fflush(stdout);
            }
            
            strcpy(testdir, testdirpath);
            strcat(testdir, "/");
            strcat(testdir, TEST_DIR);
            sprintf(testdir, "%s.%d", testdir, j);
            if ((rank < path_count) && chdir(testdir) == -1) {
                if (mkdir(testdir, DIRMODE) == - 1) {
                    FAIL("Unable to create test directory");
                }
            }
            MPI_Barrier(MPI_COMM_WORLD);
            if (chdir(testdir) == -1) {
                FAIL("Unable to change to test directory");
            }
        	/* create hierarchical directory structure */
        	MPI_Barrier(MPI_COMM_WORLD);
            if (create_only) {
                startCreate = MPI_Wtime();
                if (unique_dir_per_task) {
                    if (collective_creates && (rank == 0)) {
                        for (i=0; i<size; i++) {
                            sprintf(base_tree_name, "mdtest_tree.%d", i);
                            create_remove_directory_tree(1, 0, NULL, 0);
                        }
                    } else if (!collective_creates) {
                        create_remove_directory_tree(1, 0, NULL, 0);
                    }
                } else {
                    if (rank == 0) {
                        create_remove_directory_tree(1, 0 , NULL, 0);
                    }
                }
                MPI_Barrier(MPI_COMM_WORLD);
                endCreate = MPI_Wtime();
                summary_table[j].entry[8] = 
                    num_dirs_in_tree / (endCreate - startCreate);
                if (verbose >= 1 && rank == 0) {
                    printf("   Tree creation     : %10.3f sec, %10.3f ops/sec\n",
                        (endCreate - startCreate), summary_table[j].entry[8]);
					fflush(stdout);
                }
            } else {
               summary_table[j].entry[8] = 0;
            }
            sprintf(unique_mk_dir, "%s/%s.0", testdir, base_tree_name);
            sprintf(unique_chdir_dir, "%s/%s.0", testdir, base_tree_name);
            sprintf(unique_stat_dir, "%s/%s.0", testdir, base_tree_name);
            sprintf(unique_read_dir, "%s/%s.0", testdir, base_tree_name);
            sprintf(unique_rm_dir, "%s/%s.0", testdir, base_tree_name);
            sprintf(unique_rm_uni_dir, "%s", testdir);

            if (!unique_dir_per_task) {
                if (chdir(unique_mk_dir) == -1) {
                    FAIL("unable to change to shared tree directory");
                }
            }
            
            if (rank < i) {
                if (!shared_file) {
                    sprintf(mk_name, "mdtest.%d.", (rank+(0*nstride))%i);
                    sprintf(stat_name, "mdtest.%d.", (rank+(1*nstride))%i);
                    sprintf(read_name, "mdtest.%d.", (rank+(2*nstride))%i);
                    sprintf(rm_name, "mdtest.%d.", (rank+(3*nstride))%i);                
                }
                if (unique_dir_per_task) {
                    sprintf(unique_mk_dir, "%s/mdtest_tree.%d.0", testdir,
                            (rank+(0*nstride))%i);
                    sprintf(unique_chdir_dir, "%s/mdtest_tree.%d.0", testdir,
                            (rank+(1*nstride))%i);
                    sprintf(unique_stat_dir, "%s/mdtest_tree.%d.0", testdir,
                            (rank+(2*nstride))%i);
                    sprintf(unique_read_dir, "%s/mdtest_tree.%d.0", testdir,
                            (rank+(3*nstride))%i);
                    sprintf(unique_rm_dir, "%s/mdtest_tree.%d.0", testdir,
                            (rank+(4*nstride))%i);
                    sprintf(unique_rm_uni_dir, "%s", testdir);
                }
                strcpy(top_dir, unique_mk_dir);
                if (dirs_only && !shared_file) {
                    if (pre_delay) {
                        delay_secs(pre_delay);
                    }
                    directory_test(j, i);
                }
                if (files_only) {
                    if (pre_delay) {
                        delay_secs(pre_delay);
                    }
                    file_test(j, i);
                }
            }

        	/* remove directory structure */
            if (!unique_dir_per_task) {
                if (chdir(testdir) == -1) {
                    FAIL("unable to change to tree directory");
                }
            }
            MPI_Barrier(MPI_COMM_WORLD);
            if (remove_only) {
                startCreate = MPI_Wtime();
                if (unique_dir_per_task) {
                    if (collective_creates && (rank == 0)) {
                        for (i=0; i<size; i++) {
                            sprintf(base_tree_name, "mdtest_tree.%d", i);
                            create_remove_directory_tree(0, 0, NULL, 0);
                        }
                    } else if (!collective_creates) {
                        create_remove_directory_tree(0, 0, NULL, 0);
                    }
                } else {
                    if (rank == 0) {
                        create_remove_directory_tree(0, 0 , NULL, 0);
                    }
                }
                MPI_Barrier(MPI_COMM_WORLD);
                endCreate = MPI_Wtime();
                summary_table[j].entry[9] = num_dirs_in_tree 
                    / (endCreate - startCreate);
                if (verbose >= 1 && rank == 0) {
                    printf("   Tree removal      : %10.3f sec, %10.3f ops/sec\n",
                        (endCreate - startCreate), summary_table[j].entry[9]);
					fflush(stdout);
                }                    
            } else {
                summary_table[j].entry[9] = 0;
            }
        }
        summarize_results(iterations);
        if (i == 1 && stride > 1) {
            i = 0;
        }
    }

    if (rank == 0) {
        printf("\n-- finished at %s --\n", timestamp());
        fflush(stdout);
    }
    
    if (random_seed > 0) {
        free(rand_array);
    }

    MPI_Finalize();
    exit(0);
}
