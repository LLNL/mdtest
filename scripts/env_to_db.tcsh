#! /bin/tcsh 
# whatever this spits out in the form of # key val
# can be parsed by the fs_test and will be
# injected into the DB (so long as the key exists in the schema)
# the format is key [space] val
# currently the val can't have spaces in it...
# to pull this into the DB through the fs_test, set the
# FS_TEST_EXTRA environment variable to point to this file


# set up
set target = $1

# if the user specified an fs:/ notation for MPI-IO, then strip it
set target = `echo $target | sed 's/.*://g'`
set target_dir = $target:h
set tpf = $HOME/Testing/tpf/src/tpf_panfs.x

# mpihome
echo "mpihome $MPIHOME"

# segment
echo "segment $HOSTNAME"

# user
echo "user $USER"

# system
echo "system $HOSTNAME"

# date_ts
set date_ts = `date +%s`
echo "date_ts $date_ts"

# mpihost
if ( $?MY_MPI_HOST ) then
	echo "mpihost $MY_MPI_HOST"
endif

# os_version
set os_version = `uname -r`
echo "os_version $os_version"s

# yyyymmdd
set yyyymmdd = `date +%F`
echo "yyyymmdd $yyyymmdd"

# jobid
if ( $?PBS_JOBID ) then
	echo "jobid $PBS_JOBID"	
else if ( $?LFS_JOBID ) then
	echo "jobid $LFS_JOB"
endif

# mpi_version
echo "mpi_version $MPI_VERSION"

# host list 
#env | grep -i node
if ( $?PBS_NODEFILE ) then
	set host_list = `cat $PBS_NODEFILE | tr '\n' ','`
	echo "host_list $host_list"
endif

# procs_per_node
if ( $?PBS_NODEFILE ) then
    set shortname = `hostname -s`
	set procs_per_node = `cat $PBS_NODEFILE | grep $shortname | wc -l`
	echo "procs_per_node $procs_per_node"
endif

# grab the ionode list
set ionodes = `/sbin/ip route | awk '/nexthop/ {print $3}' | sort | uniq`
set num_ionodes = `echo $ionodes | wc -w`
set ionodes = `echo "$ionodes" | tr ' ' ','`
echo "ionodes $ionodes"
echo "num_ionodes $num_ionodes"

# grab the panfs mount options
# if panfs has multiple mounts, this might get the wrong one...
set panfs_mnt = `mount -t panfs | tr '\n' '|' | tr ' ' '_'`
echo "panfs_mnt $panfs_mnt"

# get panfs client version
set panfs_trace1 = /usr/sbin/panfs_trace
set panfs_trace2 = /usr/local/sbin/panfs_trace
if ( -x $panfs_trace1 ) then
	set client_version = `$panfs_trace1 --version $target_dir | awk '{print $4$5}' | head -1`
	echo "panfs $client_version"
else if ( -x $panfs_trace2 ) then
	set client_version = `$panfs_trace2 --version $target_dir | awk '{print $4$5}' | head -1`
	echo "panfs $client_version"
else
    echo "error couldnt_discover_panfs_version"
endif

# get thread count
set thread_count = `ps auxw | grep kpanfs_thpool | grep -v grep | wc -l`
echo "panfs_threads $thread_count"

# get df numbers
set df_perc = `df $target_dir -t panfs -P | tail -1 | awk '{print $5}' | sed s/%//`
set df_tot  = `df $target_dir -t panfs -P | tail -1 | awk '{print $2}'`
echo "df_perc_before $df_perc"
echo "df_tot_before $df_tot"

# grab tpf info
if ( "X$target" != "X" ) then
    if ( -d $target_dir ) then
        if ( -x $tpf ) then
            $tpf default $target_dir |& awk \
                '/Components/ {print "panfs_comps "$5} \
                 /RAID width/ {print "panfs_width "$3} \
                 /Depth/      {print "panfs_depth "$2} \
                 /Stride/     {print "panfs_stripe "$3} \
                 /Layout Policy/ {print "panfs_visit "$3} \
                 /Layout Type/     {print "panfs_type "$3}  \
                '
        else
            echo "error no_valid_tpf_executable"
        endif
    else
        echo "error no_valid_target_dir_$target_dir"
    endif
else 
    echo "error no_valid_target"
endif
