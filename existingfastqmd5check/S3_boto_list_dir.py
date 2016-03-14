import boto

s3 = boto.connect_s3()
bucket = s3.lookup('cgl-sgdd-work')

mylist = bucket.list(prefix = 'sequence-sorted/')

#print mylist

for line in mylist:
    print line
