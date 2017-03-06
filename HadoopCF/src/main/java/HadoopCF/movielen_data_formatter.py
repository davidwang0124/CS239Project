import re
count=0
with open('ratings.csv','r') as csv_file:
   content = csv_file.readlines()
   for line in content:
        fixed = re.sub(",", "\t", line).rstrip()
        splitted = fixed.split("\t")
        if splitted[0]<>"userId":
            print '%s' % fixed
