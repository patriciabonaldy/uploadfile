from strategies.genericStrategy import GenericStrategy
from strategies.jsonStrategy import JsonStrategy
import json


class Strategy:

    
    def __init__(self, checkHeader,fname, encoding, typeExtension, limitedLine, limitedColumn):
        self.encoding = encoding
        self.typeExtension = typeExtension
        self.limitedLine = limitedLine
        self.limitedColumn = limitedColumn 
        self.fname = fname
        self.checkHeader = checkHeader

    
    def get_strategy(self):
        strategy = None
        if self.typeExtension == 'JSON': 
            strategy = JsonStrategy(self.fname, self.encoding, self.typeExtension, self.checkHeader)
        else: 
            strategy = GenericStrategy(self.fname, self.encoding, self.typeExtension, self.checkHeader, self.limitedLine, self.limitedColumn)
              
        return strategy
