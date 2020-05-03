import abc
from strategies.manager_file import ManagerFile


class GenericStrategy(ManagerFile):

    __metaclass__ = abc.ABCMeta
    
    def __init__(self, fname, encoding, limitedLine, limitedColumn):
        super(GenericStrategy, self).__init__(fname, encoding, limitedLine, limitedColumn)#'utf-8'


    @abc.abstractmethod
    def parser(self, f, line):
        value = line.split(self.limitedColumn)
        #site, id_item = value[0], value[1]
        f.write(line+'\n')
        return value

