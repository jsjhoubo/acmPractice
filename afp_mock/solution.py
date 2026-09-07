class InMemoryDB:
    def __init__(self):
        # TODO: your storage here
        self._cache={}
        self._backup ={}

    
    # ---- Level 1 ----
    def set(self, key: str, field: str, value: str) -> None:
        """Set field=value in record `key` (create the record if missing)."""
        if self._cache.get(key) is None:
            self._cache[key] ={}
        
        self._cache[key][field]=(value, None, None)
        

    def get(self, key: str, field: str):
        """Return the value, or None if the key or field does not exist."""
        if self._cache.get(key) is None:
            return None
        if self._cache[key].get(field) is None:
            return None
        return self._cache[key][field][0]

    def delete(self, key: str, field: str) -> bool:
        """Delete the field. Return True if it existed and was deleted, else False."""
        if self._cache.get(key) is None:
            return False
        if self._cache[key].get(field) is None :
            return False
        self._cache[key].pop(field)
        return True

    # ---- Level 2+ : add methods as you unlock each level (see LEVELn.md) ----
    def scan(self, key: str):
        if self._cache.get(key) is None or len(self._cache[key]) ==0:
            return ''
        ret =''
        
        for field, value in sorted(self._cache[key].items()):
            ret =ret + field+'('+value[0]+'), '
        ret =ret.rstrip()
        return ret[:-1];

    def scan_by_prefix(self, key: str, prefix: str):
        if self._cache.get(key) is None or len(self._cache[key]) ==0:
            return ''
        ret =''
        find =False
        for field, value in sorted(self._cache[key].items()):
            if field.startswith(prefix):
                ret =ret + field+'('+value[0]+'), '
                find =True
        if find ==True:
            ret =ret.rstrip()
            ret = ret[:-1]
        return ret;

    def set_at(self, key, field, value, timestamp):
        if self._cache.get(key) is None:
            self._cache[key] ={}
        if self._cache[key].get(field) is None:
            self._cache[key][field] ={}
        self._cache[key][field] =(value, timestamp, None)
        
    def set_at_with_ttl(self, key, field, value, timestamp, ttl):
        if self._cache.get(key) is None:
            self._cache[key] ={}
        if self._cache[key].get(field) is None:
            self._cache[key][field] ={}
        self._cache[key][field] =(value, timestamp, ttl)
                
    def delete_at(self, key, field, timestamp):
        if self._cache.get(key) is None:
            return False
        if self._cache[key].get(field) is None:
            return False

        if self._cache[key][field][2] is None:
            if  self._cache[key][field][1] > timestamp:
                return False
        if self._cache[key][field][2] is not None:
            if  not (self._cache[key][field][1] <= timestamp and timestamp <self._cache[key][field][1] + self._cache[key][field][2]):
                return False
        self._cache[key].pop(field)
        return True

    def get_at(self, key, field, timestamp):
        if self._cache.get(key) is None:
            return None
        if self._cache[key].get(field) is None:
            return None
        all_time =[] 
        if self._cache[key][field][2] is None:
            if  self._cache[key][field][1] > timestamp:
                return None
        if self._cache[key][field][2] is not None:
            if  not (self._cache[key][field][1] <= timestamp and timestamp <self._cache[key][field][1] + self._cache[key][field][2]):
                return None
        return self._cache[key][field][0]
        

    def scan_at(self, key, timestamp):
        if self._cache.get(key) is None or len(self._cache[key]) ==0:
            return ''
        ret =''
        
        for field, value in sorted(self._cache[key].items()):
            if value[2] is None and value[1]> timestamp:
                continue
            if value[2] is not None and not (value[1]<= timestamp and timestamp <value[1] + value[2]):
                continue
                        
            ret =ret + field+'('+value[0]+'), '
        ret =ret.rstrip()
        return ret[:-1];
        
    def scan_by_prefix_at(self, key, prefix, timestamp):
        if self._cache.get(key) is None or len(self._cache[key]) ==0:
            return ''
        ret =''
        find =False
        for field, value in sorted(self._cache[key].items()):
            if not field.startswith(prefix):
                continue
            if value[2] is None and value[1]> timestamp:
                continue
            if value[2] is not None and not (value[1]<= timestamp and timestamp <value[1] + value[2]):
                continue
            find =True              
            ret =ret + field+'('+value[0]+'), '
        if find ==True:
            ret =ret.rstrip()
            ret = ret[:-1]
        ret =ret.rstrip()
        return  ret

    def backup(self, timestamp):
        self._backup[timestamp] ={}
        cnt =0
        for key, data1 in self._cache.items():
            self._backup[timestamp][key] ={}
            for field, data2 in data1.items():
                start =data2[1]
                end = data2[2]
                if end is None and start <= timestamp:
                    self._backup[timestamp][key][field] =data2 
                    
                elif end is not None and start <= timestamp and timestamp < start + end:
                    self._backup[timestamp][key][field] =data2
            if len(self._backup[timestamp][key]) >0:
                cnt =cnt +1
            else:
                self._backup[timestamp].pop(key)
        return cnt

    def restore(self, timestamp):
        maxt =-1
        for time, data in self._backup.items():
            if time <=timestamp and time >maxt:
                maxt = time
        if maxt ==-1:
            return
        self._cache ={}
        for key, data1 in self._backup[maxt].items():
            self._cache[key] ={}
            for field, data2 in data1.items():    
                            
                if data2[2] is not None:
                    self._cache[key][field] =(data2[0], timestamp, data2[1] + data2[2] -maxt)
                else:
                    self._cache[key][field] =data2
                                    
                                