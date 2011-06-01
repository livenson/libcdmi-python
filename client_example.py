# sample client of a CDMI service
import tempfile
import os

from libcdmi import cdmi


endpoint = "http://localhost:2364/"
credentials = {'user': 'aaa',
               'password': 'aaa'}

lf, localfile = tempfile.mkstemp()
os.write(lf, "# Test data #")
os.close(lf)

remoteblob = 'test_file.txt'
remoteblob2 = '/mydata/text_file.txt'

remote_container = '/mydata'
remote_container2 = '/mydata/more'

conn = cdmi.CDMIConnection(endpoint, credentials)

# blob operations
conn.blob_proxy.create_from_file(localfile, remoteblob, mimetype='text/plain')
conn.blob_proxy.create_from_file(localfile, remoteblob + "_nocdmi", )

value = conn.blob_proxy.read(remoteblob)
print "=== Value ==\n%s\n" % value

conn.blob_proxy.delete(remoteblob)

# container operations
conn.container_proxy.create(remote_container)
print conn.container_proxy.read('/')
conn.container_proxy.delete(remote_container)
print conn.container_proxy.read('/')

# cleanup 
os.unlink(localfile)
