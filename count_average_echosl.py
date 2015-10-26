#!/usr/bin/python2.4
#
#

import psycopg2
import psycopg2.extras
import sys
import re 
import collections



#try to connect
try:
    conn = psycopg2.connect("dbname='MIMIC' user='postgres' host='localhost' port='5432' password='mimic'")
except:
    print "I am unable to connect to the database"
	
cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
try:
    cur.execute(""" 
	select * 
	from mimiciii.noteevents ne
	where ne.category = 'Echo'
	""")
except:
    print "I can't SELECT from notesevent"
rows = cur.fetchall()


#my hash table with subject_id as keys
#this will be a double hash, the inner table will contain number of echos with and without associated hospital ids 
sub_hash = {}
# each row contains an echo with 
# row_id
# record_id
# subject_id
# hadm_id
# chart_date
# category (Echo)
# description (Report)
# cgid
# iserror
# text (tshe body of the echo)


#qualtiy
total_echos = 0
total_no = 0
total_people_yes = 0
total_yes = 0
total_people_no = 0
more_than_one = 0
total_echos2=0
for row in rows:

	#if it is a new subject ID, then we add the inner hash, no_hid means there is no hospital id associated.  yes_hid means there is a hospital id associated
	if row['subject_id'] not in sub_hash:
		sub_hash[row['subject_id']] = {'no_hid':0,'yes_hid':0}
	if (row['hadm_id'] is None):
		sub_hash[row['subject_id']]['no_hid'] = sub_hash[row['subject_id']]['no_hid']+1
	else:
		sub_hash[row['subject_id']]['yes_hid'] = sub_hash[row['subject_id']]['yes_hid']+1	
	total_echos2 += 1
#for keys,values in sub_hash.items():
#	print"%s: %s" % (keys, values)
	
for x in sub_hash:
#    for y in sub_hash[x]:
#		print "%s, %s, %s" % (x,sub_hash[x]['yes_hid'], sub_hash[x]['no_hid'])
	total_echo_for_person = sub_hash[x]['yes_hid'] + sub_hash[x]['no_hid']
	total_echos += total_echo_for_person
	if sub_hash[x]['no_hid'] > 0: 
		total_people_no += 1
		total_no += sub_hash[x]['no_hid']
	if sub_hash[x]['yes_hid'] > 0:
		total_people_yes += 1
		total_yes += sub_hash[x]['yes_hid']
	if total_echo_for_person > 1:
		more_than_one += 1
#        print (y,':',sub_hash[x][y])	

print "total echos: %s" % total_echos
print "total_echos without Hospital Admin Ids: %s" % total_no
print "total people with at least one echo without a hospital Id: %s" % total_people_no
print "total echos with hospital Ids: %s" % total_yes
print "total people with at least one echo with a hospital ID: %s" % total_people_yes
print "total_echos2: %s" % total_echos2
print "total people with more than 1 echo: %s" % more_than_one