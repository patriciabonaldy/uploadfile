import logging
import abc
from manager_file import ManagerFile


logger = logging.getLogger()
class GenericStrategy(ManagerFile):

    __metaclass__ = abc.ABCMeta
    
    def __init__(self, fname, encoding, typeExtension, checkHeader, limitedLine, limitedColumn):
        super(GenericStrategy, self).__init__(fname, encoding, typeExtension, checkHeader, limitedLine, limitedColumn)#'utf-8'


    @abc.abstractmethod
    def parser(self, line):
        try:
            value = line.split(self.limitedColumn)
            site, id_item = value[0], int(value[1])
            return value
        except Exception as e:  
            logger.error('Exception GenericStrategy[parser]: formato linea invalido -'+line )    

