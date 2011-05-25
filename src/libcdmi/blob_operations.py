import urllib2
from libcdmi.common import CDMIRequestWithMethod, CDMI_DATA, CDMI_OBJECT
   
try:
    import json
except ImportError:
    import simplejson as json
   
class BlobOperations():
    endpoint = None
    
    def __init__(self, endpoint):
        self.endpoint = endpoint
        
    def create_from_file(self, localfile, remoteblob, mimetype='text/plain', cdmi_object=True, metadata={}):
        if cdmi_object:
            self.create(open(localfile, "rb"), remoteblob, mimetype, metadata)
        else:
            self.create_noncdmi(open(localfile, "rb"), remoteblob, mimetype)
            
    def create(self, content_object, remoteblob, mimetype='text/plain', metadata={}):
        """Create a new blob from a file object, e.g. file or StringIO. """
        
        # put relevant headers
        headers = {
                   'Accept': CDMI_DATA,
                   'Content-Type': CDMI_DATA,
                   }
        
        # read-in the value        
        content = content_object.read()
        content_object.close()
        
        data = {'value': content,
                'mimetype': mimetype,
                'metadata': metadata                
                }
        
        req = CDMIRequestWithMethod(self.endpoint + remoteblob, 'PUT', json.dumps(data), headers)
        try:            
            f = urllib2.urlopen(req)
            return f.read()
        except urllib2.HTTPError, e:
            # urllib2 throws error if the code is 201 CREATED, which is a normal thing in CDMI
            if e.code == 201:
                return f.read()
            else:
                raise e
    
    def update(self, localfile, remoteblob , mimetype=None, metadata={}):
        """Update a remote blob with new data."""
        # XXX for now we don't differentiate between update and create
        return self.create(localfile, remoteblob, mimetype, metadata)
    
    def read(self, remoteblob):
        """Read contents of a blob"""
        # put relevant headers
        headers = {
                   'Accept': CDMI_DATA,
                   'Content-Type': CDMI_OBJECT,
                   }        
        
        req = CDMIRequestWithMethod(self.endpoint + remoteblob, 'GET', None, headers)
        res = urllib2.urlopen(req)
        
        return json.loads(res.read())['value']
        
    
    def delete(self, remoteblob):
        """Delete specified blob"""
        headers = {
                   'Accept': CDMI_DATA,
                   'Content-Type': CDMI_DATA,
                   }
        
        req = CDMIRequestWithMethod(self.endpoint + remoteblob, 'DELETE', None, headers)
        urllib2.urlopen(req).read()
    
    #------ Non-CMDI operations ------#  
    
    def create_noncdmi(self, content_object, remoteblob, mimetype='text/plain'):
        content = content_object.read()
        content_object.close()
        headers = {
                   'Content-Type': mimetype,
                   }  
        req = CDMIRequestWithMethod(self.endpoint + remoteblob, 'GET', content, 
                                    cdmi_object=False, headers=headers)
        try:            
            f = urllib2.urlopen(req)
            return f.read()
        except urllib2.HTTPError, e:
            # urllib2 throws error if the code is 201 CREATED, which is a normal thing in CDMI
            if e.code == 201:
                return f.read()
            else:
                raise e

    def update_noncdmi(self, content_object, remoteblob, mimetype='text/plain'):
        return self.create_noncdmi(content_object, remoteblob, mimetype)
    
    def read_noncdmi(self):
        pass
    
    def delete_noncdmi(self):
        pass 
    
    
