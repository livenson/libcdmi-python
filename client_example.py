# sample client of a CDMI service

from libcdmi import cdmi

endpoint = "http://localhost:2364/"
credentials = {'user': 'aaa',
               'password': 'aaa'}

localfile = 'test_file.txt'
remoteblob = 'test_file.txt'
remoteblob2 = '/mydata/text_file.txt'

remote_container = '/mydata'
remote_container2 = '/mydata/more'

conn = cdmi.CDMIConnection(endpoint, credentials)

# blob operations
conn.blob_proxy.create_blob_from_file(localfile, remoteblob, mimetype='text/plain')
conn.blob_proxy.update_blob_from_file(localfile, remoteblob, mimetype='text/plain')

value = conn.blob_proxy.read_blob(remoteblob)
print "=== Value ==\n%s\n" % value

conn.blob_proxy.delete_blob(remoteblob)

# container operations
conn.container_proxy.create_container(remote_container)
print conn.container_proxy.read_container('/')
conn.container_proxy.delete_container(remote_container)
print conn.container_proxy.read_container('/')
