#!/usr/bin/python
import psycopg2
List1=[]
List2=[]
List3=[]
List4=[]
def count(database_name,List):
	conn=psycopg2.connect(database="%s" %database_name,user="postgres",password="qhy85246",host="localhost",port="5432")
	cur=conn.cursor()
	for i in range(1,288):
		cur.execute("select count(*) from real_road_time_slice_%s_2010_04_08 where is_sensed='t'" %i)
		rows=cur.fetchall()
		List.append(rows)
	conn.commit()
	cur.close()
	conn.close()
def getcount():
	count("routing",List1)
	count("routing1",List2)
	for a,b in zip(List1,List2):
		print a,b
#getcount()

def compare(database_name,List):
	conn=psycopg2.connect(database="%s" %database_name,user="postgres",password="qhy85246",host="localhost",port="5432")
	cur=conn.cursor()
	cur.execute("select gid,average_speed from real_road_time_slice_%s_2010_04_08 where is_sensed='t' limit 1000" %timeslice)
	rows1=cur.fetchall()
	for i in rows1:
		List.append(i)
	conn.commit()
	cur.close()
	conn.close()
def compare_speed():
	compare("routing",List3)
	compare("routing1",List4)
	for i,j in List3:
		for k,l in List4:
			if i==k:
				print i,"	",j,"	",l,"	",j-l
timeslice=1
compare_speed()

