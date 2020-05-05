import os, sys
sys.path.append(os.path.abspath("."))
import constant

init_hit_dict = {'hit':0, 'miss':0}
class Analyzer():
    def __init__(self):
        super().__init__()
        self.hit_rate = {}
        self.curStep = 0
        self.log = {}

    def start(self):
        self.hit_rate[self.curStep] = init_hit_dict.copy()
        self.log[self.curStep] = []
    
    def addRecord(self, key, cache_server_id, isHit):
        if isHit == constant.STATUS_CODE.CACHE_HIT:
            self.hit_rate[self.curStep]['hit'] += 1
        else:
            self.hit_rate[self.curStep]['miss'] += 1

        self.log[self.curStep].append({ 'key': key,
                                        'cache_id': cache_server_id,
                                        'is_hit': isHit})

    def addStep(self):
        self.curStep += 1
        self.hit_rate[self.curStep] = init_hit_dict.copy()
        self.log[self.curStep] = []


    def getPrintHitRate(self, step):
        if self.hit_rate[step]['hit'] + self.hit_rate[step]['miss'] == 0:
            return 'The hit rate in step {0} is inf \n'.format(step)
        else:
            return 'The hit rate in step {0} is {1} \n'.format(
                        step, self.hit_rate[step]['hit']/(self.hit_rate[step]['hit'] + self.hit_rate[step]['miss']))

    def getPrintLog(self, step):
        out = ''
        for r in self.log[step]:
            if r['is_hit'] == constant.STATUS_CODE.CACHE_HIT:
                out += 'Cache hit for key "{0}" in server {1}\n'.format(r['key'], r['cache_id'])
            elif r['is_hit'] == constant.STATUS_CODE.CACHE_MISS:
                out += 'Cache miss for key "{0}" in server {1}\n'.format(r['key'], r['cache_id'])
            elif r['is_hit'] == constant.STATUS_CODE.CONNECTION_CACHE_SERVER_FAILED:
                out += 'Cache fail for key "{0}" in server {1}\n'.format(r['key'], r['cache_id'])

        return out

    def printAnalytics(self, filename=None):
        if filename:
            with open(filename, 'w') as f:
                for i in range(self.curStep+1):
                    f.write(self.getPrintHitRate(i))
                f.write('=======================Detail Log============================\n')
                for i in range(self.curStep + 1):
                    f.write('In step {0}\n'.format(i))
                    f.write(self.getPrintLog(i))
        else:
            for i in range(self.curStep+1):
                print(self.getPrintHitRate(i))
            print('=======================Detail Log=============================')
            for i in range(self.curStep+1):
                print('In step {0}'.format(i))
                print(self.getPrintLog(i))
    
