# Author    kim kiogora <kimkiogora@gmail.com>
# Usage     Redis Class
# Version   1.0
# Since     11 Nov 2016

import redis

class RCache:
    # Define a constructor
    def __init__(self, host, port):
        self.redis_obj = redis.StrictRedis(host=host, port=port, db=0)

    # Set the element in the stack
    def push(self, key, data):
        self.redis_obj.set(key, data, 3600*24) #defaul is 24 hour

    # Get the element saved in stack
    def pop(self, key):
        data = self.redis_obj.get(key)
        return data
