import abc
from strategies.manager_file import ManagerFile
import json


class JsonStrategy(ManagerFile):

    __metaclass__ = abc.ABCMeta
    
    def __init__(self, fname, encoding):
        super(JsonStrategy, self).__init__(fname, encoding, None, None)#'utf-8'


    @abc.abstractmethod
    def parser(self, f, line):
        f.write(line+ '\n')
        if len(line )>0:
            data = json.loads(line.rstrip('\n|\r'))
            return data
            #json_record = json.dumps(data, ensure_ascii=False)
        
            

