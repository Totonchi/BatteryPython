import sys

can_list = []
with open('display_can.csv', 'r') as f:
   for line in f:
      split = line.split(',')
      if len(split) < 5:
         continue
      tmp = {}
      tmp['CAN'] = split[0]
      tmp['start'] = split[1]
      tmp['length'] = split[2]
      tmp['scale'] = split[3]
      tmp['offset'] = split[4]
      can_list.append(tmp)
print can_list
