import logging
import abc
import os
from file_item import FileItem
from manager_worker import ManagerWorker
from oracle_driver import save_dataset_to_oracle_bulk


logger = logging.getLogger()

class ManagerFile(FileItem):

    __metaclass__ = abc.ABCMeta
    
    def __init__(self, fname, encoding, limitedLine, limitedColumn):
        super(ManagerFile, self).__init__(fname, encoding, limitedLine, limitedColumn)


    @abc.abstractmethod
    def parser(self,f, line):
        """Required Method"""    

    
    def save_list(self, lines):
        df = list(self.process_sublist(lines))
        #save_dataset_to_oracle_bulk(df)


    def process_lines(self,start_block, end_block):
        workers2 = ManagerWorker()                    
        for lines in self.process_wrapper(start_block, end_block):
            if len(lines) > 0 :                
                for s_ln in self.sub_list(lines):
                    workers2.create_jobs(self.save_list,(s_ln, ))
        workers2.execute_job()


    def get_blocks_file(self,fname, size):
        fileEnd = os.path.getsize(fname)
        with open(fname, 'rb') as f:
            end_block = f.tell()
            while True:
                start_block = end_block
                f.seek(size,1)
                line =f.readline()
                end_block = f.tell()  
                end_block = end_block - start_block   
                if end_block > fileEnd or start_block==fileEnd:
                    break           
                yield start_block, end_block
                

    def procesar_file(self):  
        workers = ManagerWorker()
        for start_block,end_block in self.get_blocks_file(self.fname,self.slices_file):
            workers.create_jobs(self.process_lines, (start_block, end_block))
        workers.execute_job()
