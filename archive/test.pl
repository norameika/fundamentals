/usr/local/bin/python 
2.7.5 

#!/usr/local/bin/python 
# -*- coding: utf-8 -*- 

print "Content-Type: text/html\n" 
print "Hello world!"
for f in os.listdir('./db/temp'):
    print f
