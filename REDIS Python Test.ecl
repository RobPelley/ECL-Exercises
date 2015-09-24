IMPORT Python;

INTEGER SetKeyValue(STRING k, STRING v) := EMBED(Python)

import redis

r = redis.StrictRedis(host='127.0.0.1', port=6379, db=0)

r.set(k, v)
  
return 0;
                                        
ENDEMBED;

i := SetKeyValue('Rob','Pelley');

OUTPUT(i, NAMED('SetResult'));

STRING GetKeyValue(STRING k) := EMBED(Python)

import redis

r = redis.StrictRedis(host='127.0.0.1', port=6379, db=0)

return r.get(k)
  
ENDEMBED;

res := GetKeyValue('Rob');

OUTPUT(res, NAMED('KeyValue'));

