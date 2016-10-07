import MySQLdb as db,string

def main():

    ### change these variables to update the desired field ###
	param = "-I"
	field = "items_per_dir"
	
	print "Initiating database connection..."
	d = db.connect(host="tangerine.lanl.gov", db="mpi_io_test")
	print "Connected to database!"
	cursor = d.cursor()
	
	print "Querying database..."
	sql = "SELECT command_line,user,system,date_ts FROM mdtest WHERE !isnull(command_line)"
	cursor.execute(sql)
	print "Completed SELECT query!"

	data = cursor.fetchone()

	f = open("temp_query","w")
	
	print "Parsing query results..."
	while (data != None):
		cmd = data[0]

		if (cmd.find(param) != -1):
			list = cmd.split()
			index = list.index(param)
			sql = "UPDATE mdtest SET "+field+"="+list.pop(index+1)
			sql += " WHERE user like '"+data[1]+"'"
			sql += " && system like '"+data[2]+"'"
			sql += " && date_ts="+str(data[3])+"; "
			f.write(sql)

		data = cursor.fetchone()

	d.close()
	f.close()
	
	print "Done parsing query results!  Update queries located in sql_query."



if __name__ == "__main__":
	main()
