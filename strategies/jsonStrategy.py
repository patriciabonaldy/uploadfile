import logging
import abc
from manager_file import ManagerFile
import json


logger = logging.getLogger()
class JsonStrategy(ManagerFile):

    __metaclass__ = abc.ABCMeta
    
    def __init__(self, fname, encoding, typeExtension, checkHeader):
        super(JsonStrategy, self).__init__(fname, encoding, typeExtension, checkHeader, None, None)#'utf-8'


    @abc.abstractmethod
    def parser(self, line):
        try:
            data = json.loads(line.rstrip('\n|\r'))
            if 'site' in data and  'id' in data:
                return [data["site"], data["id"]]
            else:
                logger.error('Exception JsonStrategy[parser]: formato linea invalido -'+line )    
            #json_record = json.dumps(data, ensure_ascii=False)
        except Exception as e:  
            logger.error('Exception JsonStrategy[parser]: formato linea invalido -'+line ) 
        
            

