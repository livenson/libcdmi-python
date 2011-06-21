import urllib2
from libcdmi.common import CDMIRequestWithMethod, CDMI_OBJECT
   
try:
    import json
except ImportError:
    import simplejson as json
   
class BlobOperations():
    endpoint = None
    
    def __init__(self, endpoint):
        self.endpoint = endpoint
        
    def head(self, remoteblob, cdmi_object=True):
        # Logic: GET - body
        if cdmi_object:
            return self.read_cdmi(remoteblob, return_body=False)
        else:
            return self.read_noncdmi(remoteblob, return_body=False)
        
        
    def create_from_file(self, localfile, remoteblob, mimetype='text/plain', cdmi_object=True, metadata={}):
        return self.create(open(localfile, "rb"), remoteblob, mimetype, cdmi_object, metadata)       
        
    def create(self, content_object, remoteblob, mimetype='text/plain', cdmi_object=True, metadata={}):
        if cdmi_object:
            return self.create_cdmi(content_object, remoteblob, mimetype, metadata)
        else:
            return self.create_noncdmi(content_object, remoteblob, mimetype)
    
    def update_from_file(self, localfile, remoteblob , mimetype=None, cdmi_object=True, metadata={}):
        return self.update(open(localfile, "rb"), remoteblob, mimetype, cdmi_object, metadata)
    
    def update(self, content_object, remoteblob , mimetype=None, cdmi_object=True, metadata={}):
        if cdmi_object: 
            return self.update_cdmi(content_object, remoteblob , mimetype, metadata)
        else:
            # XXX Warning - metadata will not be forwarded 
            return self.update_noncdmi(content_object, remoteblob , mimetype)
    
    def read(self, remoteblob, cdmi_object=True):
        if cdmi_object:
            return self.read_cdmi(remoteblob)
        else:
            return self.read_noncdmi(remoteblob) 
        
    def delete(self, remoteblob, cdmi_object=True):
        if cdmi_object: 
            return self.delete_cdmi(remoteblob)
        else:
            return self.delete_noncdmi(remoteblob)
    
    #------ CDMI operations ------#  
    
    def create_cdmi(self, content_object, remoteblob, mimetype='text/plain', metadata={}):
        """Create a new blob from a file object, e.g. file or StringIO. """
        # put relevant headers
        headers = {
                   'Accept': CDMI_OBJECT,
                   'Content-Type': CDMI_OBJECT,
                   }
        
        # read-in the value        
        content = content_object.read()
        content_object.close()
        
        data = {
                'value': content,
                'mimetype': mimetype,
                'metadata': metadata                
                }
        
        req = CDMIRequestWithMethod(self.endpoint + remoteblob, 'PUT', json.dumps(data), headers=headers)
        try:
            f = urllib2.urlopen(req)
            return json.loads(f.read())
        except urllib2.HTTPError, e:
            # urllib2 throws error if the code is 201 CREATED, which is a normal thing in CDMI
            if e.code == 201:
                return json.loads(f.read())
            else:
                raise e
    
    def update_cdmi(self, localfile, remoteblob, mimetype=None, metadata={}):
        """Update a remote blob with new data."""
        # XXX for now we don't differentiate between update and create
        return self.create_cdmi(localfile, remoteblob, mimetype, metadata)
    
    def read_cdmi(self, remoteblob, return_body = True):
        """Read contents of a blob. Returns JSON-encoded metadata and data."""
        # put relevant headers
        headers = {
                   'Accept': CDMI_OBJECT,
                   }
        method = 'GET' if return_body else 'HEAD'
        req = CDMIRequestWithMethod(self.endpoint + remoteblob, method, headers=headers)
        res = urllib2.urlopen(req)
        if return_body:            
            return json.loads(res.read())
        else:
            return res.info()
        
    
    def delete_cdmi(self, remoteblob):
        """Delete specified blob"""        
        req = CDMIRequestWithMethod(self.endpoint + remoteblob, 'DELETE')
        f = urllib2.urlopen(req)
        f.close()
    
    #------ Non-CDMI operations ------#  
    
    def create_noncdmi(self, content_object, remoteblob, mimetype='text/plain'):
        content = content_object.read()
        content_object.close()
        headers = {
                   'Content-Type': mimetype,
                   }  
        req = CDMIRequestWithMethod(self.endpoint + remoteblob, 'PUT', content, 
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
    
    def read_noncdmi(self, remoteblob, return_body=True):
        """Read contents of a blob. Returns contents of the remote blob."""
        # put relevant headers
        headers = {
                   'Accept': CDMI_OBJECT,
                   }
        method = 'GET' if return_body else 'HEAD'
        req = CDMIRequestWithMethod(self.endpoint + remoteblob, method, cdmi_object=False, headers=headers)
        res = urllib2.urlopen(req)
        if return_body:            
            return res.read()
        else:
            return res.info()
    
    def delete_noncdmi(self, remoteblob, return_body=True):
        """Delete specified blob"""
        req = CDMIRequestWithMethod(self.endpoint + remoteblob, 'DELETE', cdmi_object=False)
        f = urllib2.urlopen(req)
        f.close()