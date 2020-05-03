import logging
import abc
import os
from file_item import FileItem
from manager_worker import ManagerWorker
from oracle_driver import save_dataset_to_oracle_bulk, insertar_header_lote


logger = logging.getLogger()

class ManagerFile(FileItem):

    __metaclass__ = abc.ABCMeta
    
    def __init__(self, fname, encoding, typeExtension, checkHeader, limitedLine, limitedColumn):
        super(ManagerFile, self).__init__(fname, encoding, typeExtension, checkHeader, limitedLine, limitedColumn)
        self.id_lote = 0


    @abc.abstractmethod
    def parser(self, line):
        """Required Method"""    

    
    def save_list(self, lines):
        df = list(self.process_sublist(lines))
        save_dataset_to_oracle_bulk(self.id_lote,df)


    def process_lines(self,start_block, end_block):
        workers2 = ManagerWorker()                    
        for lines in self.process_wrapper(start_block, end_block):
            if len(lines) > 0 :                
                for s_ln in self.sub_list(lines):                    
                    workers2.create_jobs(self.save_list,(s_ln, ))
        workers2.execute_job()
                

    def procesar_file(self):  
        workers = ManagerWorker()
        df = (1,self.fname, 
                self.encoding, 
                self.typeExtension, 
                self.limitedColumn,'PE')
        self.id_lote = insertar_header_lote(df)
        for start_block,end_block in self.get_blocks_file(self.fname,self.slices_file):
            workers.create_jobs(self.process_lines, (start_block, end_block))
        workers.execute_job()
