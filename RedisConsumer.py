import time
import redis
import threading
import sys

class Listener(threading.Thread):
    redis = None
    pubsub = None
    channels = None

    def __init__(self, r, channels):
        threading.Thread.__init__(self)
        self.redis = r
        self.channels = channels
        self.pubsub = self.redis.pubsub()
        self.pubsub.subscribe(channels)

    def work(self, item):
        if item['data']==1:
            pass
        else:
            print "Got - ", item['channel'], ":", item['data']
    
    def run(self):
        try:
            for item in self.pubsub.listen():
                if item['data'] == "KILL":
                    print self, "unsubscribed and finished"
                    self.pubsub.unsubscribe()
                    break
                else:
                    #if item['data']==1:
                    #    return
                    self.work(item)
        except:
            print 'Redis went away, but we will ping for a connection '
            while 1:
                print 'wait for a bit xxx'
                time.sleep(2)
                try:
                    print 'Trying to reconnect xxxx'
                    self.redis = redis.StrictRedis(host='localhost', port=6379, db=0)
                    self.__init__(self.redis, self.channels)
                    print 'Reconnected :)'
                    self.start()
                    break
                except:
                    error = str(sys.exc_info()[1])
                    print 'still no connection , wait xxx %s' % error


'''
TEST
'''
if __name__ == "__main__":
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    try:
        client = Listener(r, ['test'])
        client.start()
    except:
        print 'Connection refused'
        while 1:
            try:
                r = redis.StrictRedis(host='localhost', port=6379, db=0)
                client = Listener(r, ['test', 'test2'])
                client.start()
                print 'Connection is back!'
                break
            except:
                print 'No connection!'
            time.sleep(5)
