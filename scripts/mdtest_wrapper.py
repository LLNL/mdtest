#! /usr/bin/env python

########################### mdtest_wrapper.py ##############################
#
#This program is a wrapper for mdtest.  It will execute mdtest and parse the 
#output.  The result will then be inserted into a database.  If the database 
#doesn't exist, then the query is written to a file.
#
#To run this program, run the following command:
#python mdtest_wrapper.py mpirun [mpirun args] /path/to/mdtest [mdtest args]
#
#Written by: Ryan Kroiss
#Last modified:  07/24/2009
#
############################################################################

import getopt,sys,os,array,string,time,user
import MySQLdb as db

import sys



def fail(message):
    print message
    sys.exit()

### customized parsing method for mdtest ###
def parseArgs(args, db_dict):

    for i in range(0, len(args)):
        if (args[i].startswith('-')):
            set = False
            o = args[i]
            if (i+1 <= (len(args)-1)):
                if (not args[i+1].startswith('-')):
                    a = args[i+1]
                    set = True

            if o == "-b":
                if (not set):
                    fail("Improperly formatted arguments")
                db_dict['branch_factor'] = a
            elif o == "-B":
                db_dict['no_barriers'] = 1
            elif o == "-c":
                db_dict['collective_creates'] = 1
            elif o == "-C":
                db_dict['create_only'] = 1
            elif o == "-d":
                if (not set):
                    fail("Improperly formatted arguments")
                db_dict['working_directory'] = a
            elif o == '--desc':
                continue
            elif o == "-D":
                db_dict['directories_only'] = 1
            elif o == "-f":
                if (not set):
                    fail("Improperly formatted arguments")
                db_dict['first_task'] = a
            elif o == "-F":
                db_dict['files_only'] = 1
            elif o == "-h":
                continue
            elif o == "-i":
                if (not set):
                    fail("Improperly formatted arguments")
                db_dict['iterations'] = a
            elif o == "-I":
                if (not set):
                    fail("Improperly formatted arguments")
                db_dict['items_per_dir'] = a
            elif o == "-l":
                if (not set):
                    fail("Improperly formatted arguments")
                db_dict['last_task'] = a
            elif o == "-L":
                db_dict['leaf_only'] = 1
            elif o == "-n":
                if (not set):
                    fail("Improperly formatted arguments")
                db_dict['items'] = a
            elif o == "-N":
                if (not set):
                    fail("Improperly formatted arguments")
                db_dict['nstride'] = a
            elif o == "-p":
                if (not set):
                    fail("Improperly formatted arguments")
                db_dict['pre_delay'] = a
            elif o == "-r":
                db_dict['remove_only'] = 1
            #elif o.startswith('-R'):
                #don't do anything here because the random seed is caught in the output of the test
            elif o == "-s":
                if (not set):
                    fail("Improperly formatted arguments")
                db_dict['stride'] = a
            elif o == "-S":
                db_dict['shared_file'] = 1
            elif o == "-t":
                db_dict['time_unique_dir_overhead'] = 1
            elif o == "-T":
                db_dict['stat_only'] = 1
            elif o == "-u":
                db_dict['unique_dir_per_task'] = 1
            elif o == "-v":
                continue
            elif o == "-V":
                continue
            elif o == "-w":
                if (not set):
                    fail("Improperly formatted arguments")
                db_dict['write_bytes'] = a
            elif o == "-y":
                db_dict['sync_file'] = 1
            elif o == "-z":
                if (not set):
                    fail("Improperly formatted arguments")
                db_dict['depth'] = a
            else:
                if (not o.startswith('-R')):
                    print o
                    fail("Incorrect flag - check mdtest usage")

    return db_dict


###### creates db insert query from db_data dictionary
###### then executes query
def db_insert(dbconn, db_data):

    ###### create insert query ######
    query = "INSERT INTO mdtest ("

    count = 0

    ### append column names to query ###
    for key in db_data.keys():
        if (db_data.get(key) != None):
            if (count == 1):
                query += ','
            count = 1
            query += key

    query += ") VALUES ('"
    count = 0

    ### append values to query ###
    for value in db_data.values():
        if (value != None):
            if (count == 1):
                query += "','"
            count = 1
            query += str(value)

    query += "')"

    db_success=False
    try: 
        ### connect to the database ###
        raise SystemError   # don't even bother, just dump to file
        conn = db.connect(host="phpmyadmin",db="mpi_io_test_pro",user="cron",
                          passwd="hpciopwd")
        cursor = conn.cursor()
    
        ### execute the query ###
        cursor.execute(query)

        ### close connection ###
        cursor.close()
        conn.close()
        
        print "Query inserted into database"
        db_success=True

    except:

        sql_file = os.getenv('HOME') + '/mdtest.sql_query'

        ### if unable to connect to db, print query to file sql_query ###
        try:
            f = open(sql_file,'a')
        except:
            f = open(sql_file,'w')
        try:
            f.write(query + ';\n')
            f.close()
            print "Appended query to file: %s" % sql_file
            db_success=True
        except:
            print "Unable to append query to file: %s" % sql_file

    #finally:

    ### when all else fails print query to standard out ###
    if db_success is False: print query
    


def main():
    
    ### check for minimum number of arguments ###
    if (len(sys.argv) < 3):
        print "Your command needs to have more that three arguments."
        print "It should look something like this:"
        print "python mdtest_wrapper.py mpirun..."
        sys.exit()

    command_line =" ".join(sys.argv)
    
    ### find index of first arg of mdtest command ###
    last = len(sys.argv)
    description = None
    env_to_db = None
    last = len(sys.argv)
    for a in sys.argv:
        if (a == '--desc'):
            index = sys.argv.index(a) + 1
            if (index < len(sys.argv)):
                description = sys.argv[index]
                last = last - 2 
        if (a == '--env_to_db'):
            index = sys.argv.index(a) + 1
            if (index < len(sys.argv)):
                env_to_db = sys.argv[index]
                last = last - 2

    ### get command to execute ###
    command = sys.argv[1]
    for s in sys.argv[2:last]:
        command += " " + s


    ### run command and print db_data to standard out ###
    walltime = int(time.time())
    p = os.popen(command)
    mdtest_output = p.read()
    walltime = int(time.time()) - walltime
    print mdtest_output
    
    ###### set up dictionary of values ######
    db_data = dict()

    ###### keys for output #######
    db_data['user'] = None
    db_data['system'] = None
    db_data['date_ts'] = None
    db_data['description'] = description

    ####### initialize mdtest parameters output ########
    db_data['collective_creates'] = None
    db_data['working_directory'] = None
    db_data['directories_only'] = None
    db_data['files_only'] = None
    db_data['first_task'] = None
    db_data['last_task'] = None
    db_data['iterations'] = None
    db_data['items'] = None
    db_data['items_per_dir'] = None
    db_data['nstride'] = None
    db_data['stride'] = None
    db_data['pre_delay'] = None
    db_data['remove_only'] = None
    db_data['shared_file'] = None
    db_data['time_unique_dir_overhead'] = None
    db_data['unique_dir_per_task'] = None
    db_data['write_bytes'] = None
    db_data['sync_file'] = None
    db_data['branch_factor'] = None
    db_data['depth'] = None
    db_data['random_stat'] = None
    db_data['no_barriers'] = None
    db_data['create_only'] = None
    db_data['leaf_level'] = None
    db_data['stat_only'] = None


    ####### initialize mdtest environment output #######
    db_data['mdtest_version'] = None
    db_data['num_tasks'] = None
    db_data['num_nodes'] = None
    db_data['command_line'] = command_line
    db_data['path'] = None
    db_data['fs_size'] = None
    db_data['fs_used_pct'] = None
    db_data['inodes_size'] = None
    db_data['inodes_used_pct'] = None
    db_data['walltime'] = str(walltime)

    ####### initialize mdtest operations output ########
    db_data['dir_create_max'] = None
    db_data['dir_create_min'] = None
    db_data['dir_create_mean'] = None
    db_data['dir_create_stddev'] = None
    db_data['dir_stat_max'] = None
    db_data['dir_stat_min'] = None
    db_data['dir_stat_mean'] = None
    db_data['dir_stat_stddev'] = None
    db_data['dir_remove_max'] = None
    db_data['dir_remove_min'] = None
    db_data['dir_remove_mean'] = None
    db_data['dir_remove_stddev'] = None
    db_data['file_create_max'] = None
    db_data['file_create_min'] = None
    db_data['file_create_mean'] = None
    db_data['file_create_stddev'] = None
    db_data['file_stat_max'] = None
    db_data['file_stat_min'] = None
    db_data['file_stat_mean'] = None
    db_data['file_stat_stddev'] = None
    db_data['file_remove_max'] = None
    db_data['file_remove_min'] = None
    db_data['file_remove_mean'] = None
    db_data['file_remove_stddev'] = None
    db_data['tree_create'] = None
    db_data['tree_remove'] = None

    ######## initialize system output #########
    db_data['mpihome'] = None
    db_data['mpihost'] = None
    db_data['mpi_version'] = None
    db_data['segment'] = None
    db_data['os_version'] = None
    db_data['yyyymmdd'] = None
    db_data['jobid'] = None
    db_data['host_list'] = None
    db_data['panfs'] = None
    db_data['panfs_srv'] = None
    db_data['panfs_type'] = None
    db_data['panfs_stripe'] = None
    db_data['panfs_width'] = None
    db_data['panfs_depth'] = None
    db_data['panfs_comps'] = None
    db_data['panfs_visit'] = None
    db_data['panfs_mnt'] = None
    db_data['panfs_threads'] = None
    db_data['ionodes'] = None
    db_data['num_ionodes'] = None
    db_data['procs_per_node'] = None


    ### set working_directory to cwd if user didn't specify one
    if (db_data['working_directory'] == None):
        db_data['working_directory'] = os.getcwd()

    ####### run env_to_db and parse output ######
    if (env_to_db is not None and os.path.exists(env_to_db)):
        command = "%s %s" % (env_to_db, db_data['working_directory']) 
        p = os.popen(command)
        env_result = p.read()
        lines = env_result.splitlines()
        for line in lines:
            tokens = line.split()
            if (len(tokens) >= 2):
                if (tokens[0] == 'ionodes'):
                    db_data['ionodes'] = tokens[1]
                elif (tokens[0] == 'num_ionodes'):
                    db_data['num_ionodes'] = tokens[1]
                elif (tokens[0] == 'panfs_mnt'):
                    db_data['panfs_mnt'] = tokens[1]
                elif (tokens[0] == 'panfs_type'):
                    db_data['panfs_type'] = tokens[1]
                elif (tokens[0] == 'panfs_comps'):
                    db_data['panfs_comps'] = tokens[1]
                elif (tokens[0] == 'panfs_stripe'):
                    db_data['panfs_stripe'] = tokens[1]
                elif (tokens[0] == 'panfs_width'):
                    db_data['panfs_width'] = tokens[1]
                elif (tokens[0] == 'panfs_depth'):
                    db_data['panfs_depth'] = tokens[1]
                elif (tokens[0] == 'panfs_visit'):
                    db_data['panfs_visit'] = tokens[1]
                elif (tokens[0] == 'mpihome'):
                    db_data['mpihome'] = tokens[1]
                elif (tokens[0] == 'segment'):
                    db_data['segment'] = tokens[1]
                elif (tokens[0] == 'user'):
                    db_data['user'] = tokens[1]
                elif (tokens[0] == 'system'):
                    db_data['system'] = tokens[1]
                elif (tokens[0] == 'date_ts'):
                    db_data['date_ts'] = tokens[1]
                elif (tokens[0] == 'mpihost'):
                    db_data['mpihost'] = tokens[1]
                elif (tokens[0] == 'os_version'):
                    db_data['os_version'] = tokens[1]
                elif (tokens[0] == 'yyyymmdd'):
                    db_data['yyyymmdd'] = tokens[1]
                elif (tokens[0] == 'jobid'):
                    db_data['jobid'] = tokens[1]
                elif (tokens[0] == 'mpi_version'):
                    db_data['mpi_version'] = tokens[1]
                elif (tokens[0] == 'host_list'):
                    db_data['host_list'] = tokens[1]
                elif (tokens[0] == 'procs_per_node'):
                    db_data['procs_per_node'] = tokens[1]
                elif (tokens[0] == 'panfs_threads'):
                    db_data['panfs_threads'] = tokens[1]
                elif (tokens[0] == 'panfs'):
                    db_data['panfs'] = tokens[1]
                    for i in range(len(tokens)-2):
                        db_data['panfs'] += " " + tokens[i+2]

    ###### get fs stats ######
    ### NOTE: this info could obtained by parsing output from mdtest
    ### but it's both easier and more accurate to do it here
    stats = os.statvfs(db_data['working_directory'])

    ### data blocks
    total_fs_size = stats.f_blocks * stats.f_bsize
    free_fs_size = stats.f_bfree * stats.f_bsize
    used_fs_pct = (1 - (float(free_fs_size)/float(total_fs_size))) * 100
    db_data['fs_size'] = total_fs_size
    db_data['fs_used_pct'] = used_fs_pct

    ### inodes
    total_inodes = stats.f_files
    free_inodes = stats.f_ffree
    used_inodes_pct = (1 - (float(free_inodes)/float(total_inodes))) * 100
    db_data['inodes_size'] = total_inodes
    db_data['inodes_used_pct'] = used_inodes_pct

    ###### parse output from mdtest and put in db_data dictionary ######
    lines = mdtest_output.splitlines()
    for line in lines:
        if (line.startswith('mdtest')):
            line_toks = line.split(' ')
            db_data['mdtest_version'] = line_toks[0]
            first = True
            for l in line_toks:
                if (l.isdigit() and first):
                    db_data['num_tasks'] = l
                    first = False
                elif (l.isdigit()):
                    db_data['num_nodes'] = l
        elif (line.startswith('Path:')):
            line_toks = line.split(':')
            db_data['path'] = line_toks[1].strip()
        elif (line.startswith('random')):
            line_toks = line.split(':')
            db_data['random_stat'] = line_toks[1].strip()
        elif (line.startswith('tree creation rate')):
            line_toks = line.split(':')
            db_data['tree_create'] = line_toks[1].strip()
        elif (line.startswith("   Directory creation:")):
            line_toks = line.split()
            length = len(line_toks)
            for i in range(length):
                if (i==(length-4)):
                    db_data['dir_create_max'] = line_toks[i]
                elif (i==(length-3)):
                    db_data['dir_create_min'] = line_toks[i]
                elif (i==(length-2)):
                    db_data['dir_create_mean'] = line_toks[i]
                elif (i==(length-1)):
                    db_data['dir_create_stddev'] = line_toks[i]
        elif (line.startswith("   Directory stat")):
            line_toks = line.split()
            length = len(line_toks)
            for i in range(length):
                if (i==(length-4)):
                    db_data['dir_stat_max'] = line_toks[i]
                elif (i==(length-3)):
                    db_data['dir_stat_min'] = line_toks[i]
                elif (i==(length-2)):
                    db_data['dir_stat_mean'] = line_toks[i]
                elif (i==(length-1)):
                    db_data['dir_stat_stddev'] = line_toks[i]
        elif (line.startswith("   Directory removal")):
            line_toks = line.split()
            length = len(line_toks)
            for i in range(length):
                if (i==(length-4)):
                    db_data['dir_remove_max'] = line_toks[i]
                elif (i==(length-3)):
                    db_data['dir_remove_min'] = line_toks[i]
                elif (i==(length-2)):
                    db_data['dir_remove_mean'] = line_toks[i]
                elif (i==(length-1)):
                    db_data['dir_remove_stddev'] = line_toks[i]
        elif (line.startswith("   File creation")):
            line_toks = line.split()
            length = len(line_toks)
            for i in range(length):
                if (i==(length-4)):
                    db_data['file_create_max'] = line_toks[i]
                elif (i==(length-3)):
                    db_data['file_create_min'] = line_toks[i]
                elif (i==(length-2)):
                    db_data['file_create_mean'] = line_toks[i]
                elif (i==(length-1)):
                    db_data['file_create_stddev'] = line_toks[i]
        elif (line.startswith("   File stat")):
            line_toks = line.split()
            length = len(line_toks)
            for i in range(length):
                if (i==(length-4)):
                    db_data['file_stat_max'] = line_toks[i]
                elif (i==(length-3)):
                    db_data['file_stat_min'] = line_toks[i]
                elif (i==(length-2)):
                    db_data['file_stat_mean'] = line_toks[i]
                elif (i==(length-1)):
                    db_data['file_stat_stddev'] = line_toks[i]
        elif (line.startswith("   File removal")):
            line_toks = line.split()
            length = len(line_toks)
            for i in range(length):
                if (i==(length-4)):
                    db_data['file_remove_max'] = line_toks[i]
                elif (i==(length-3)):
                    db_data['file_remove_min'] = line_toks[i]
                elif (i==(length-2)):
                    db_data['file_remove_mean'] = line_toks[i]
                elif (i==(length-1)):
                    db_data['file_remove_stddev'] = line_toks[i]
                elif (line.startswith('tree removal rate')):
                    line_toks = line.split(':')
                    db_data['tree_remove'] = line_toks[1].strip()


    
    db_insert(db,db_data)
    

    
    
if __name__ == "__main__":
    main()


