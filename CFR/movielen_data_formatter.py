import re
import sys

if len(sys.argv)<3:
	print("usage: python movielen_data_formatter [input] [delimiter] > [output]\n")
	exit(1)

count=0
with open(sys.argv[1],'r') as csv_file:
   content = csv_file.readlines()
   for line in content:
        fixed = re.sub(sys.argv[2], "\t", line).rstrip()
        splitted = fixed.split("\t")
        if splitted[0]<>"userId":
            print '%s' % fixed
