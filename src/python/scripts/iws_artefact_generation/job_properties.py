"""
This program converts the job xml file to plain text and extracts the job properties in a separate file
usage: python job_properties.py <your txt file> <your properties file>
"""

import lxml.etree as ET
import ibm_db as db2
import sys
sys.path.append("..")
from wrapper_scripts.connect import connect_db02

con = connect_db02()
xml_file = db2.exec_immediate(con,"SELECT XML_JOB FROM JOB_REPO.V_IWS_JOB_CHAIN_TENANT_NEU where tenant_name ='r1i5v1_dummy'")
res = db2.fetch_assoc(xml_file)
xml = res['XML_JOB']
def xml_to_plain_text(xml,file_name):
	dom = ET.fromstring(xml)
	transform = ET.XSLT(ET.parse('xsl_file.xslt'))
	result = transform(dom)
	with open(file_name,"w") as f:
		f.write(str(result))
		f.close()

	return file_name

def iws_properties(file_name,iws_file):
	with open(iws_file,"w") as f:
		with open(file_name,"r") as r:
			seen = set()
			count, count1 = True, True
			while True:
				line = r.readline()
				if "FOLDER_" in line and line.count('#') == 0 and count1 == True:
					folder = line[:-2].replace("$","").rstrip('\n')+"="+line[line.find("/"):len(line)-3]
					count1 = False
				if "JOB_" in line and line.count('#')==0 and not line in seen:
					f.write(line.replace("$","").rstrip('\n')+"=")
					seen.add(line)
				if ".sh" in line and not line in seen:
					f.write(line[line.rfind('/')+1:len(line)-4]+"\n")
					seen.add(line)
				if "WORKSTATION_" in line and line.count('#') == 0 and count == True:
					workstation = line.replace("$","").rstrip('\n')+"="+line[line.rfind("_")+1:len(line)-2]
					count = False
				if "JOBSTREAM_" in line:
					f.write(line.replace("$","").rstrip('\n')+"="+line[line.find("_")+1:len(line)-2]+"\n")
				if not line:
					break
		f.write(folder + '\n' + workstation)
	return iws_file

try:
	file_name = sys.argv[1]
	xml_to_plain_text(xml,file_name)
	iws_file = sys.argv[2]
	iws_properties(file_name,iws_file)
except:
	print("Insufficient number of arguments! Check the usage for correct format.")
