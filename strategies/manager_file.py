import logging
import abc
import os
from manager_worker import ManagerWorker
from oracle_driver import save_dataset_to_oracle_bulk

MEGABYTES =1024*1024
logger = logging.getLogger()

class ManagerFile(object):

    __metaclass__ = abc.ABCMeta
    
    def __init__(self, fname, encoding, limitedLine, limitedColumn):
        self.encoding = encoding #'utf-8' 
        self.limitedLine = limitedLine
        self.limitedColumn = limitedColumn 
        self.fname = fname      
        self.size = os.path.getsize(self.fname)
        self.cant_cpu = 3
        self.slices_file = self.size
        if self.size > MEGABYTES:
            self.slices_file = round(self.size/self.cant_cpu)



    @abc.abstractmethod
    def parser(self,f, line):
        """Required Method"""    

    
    def save_list(self, lines):
        df = list(self.process_sublist(lines))
        #save_dataset_to_oracle_bulk(df)



    def process_sublist(self, lines): 
        with open("./resource/demofile.txt", "a") as f:
            for ln in lines:
                yield self.parser(f, ln)



    def process_wrapper(self, start_block, end_block):
        with open(self.fname, 'r', encoding=self.encoding) as f:
            f.seek(start_block)
            bloque = f.read(end_block)
            yield bloque.splitlines() 

    #todo mejorar
    def sub_list(self, lines):
        tope = round(len(lines)/3)
        if len(lines) <=3:
            yield lines
        for n in range(3):
            x = (tope*n)
            y = ((n+1)*tope)
            if n==2:
                yield lines[x:]
            else:
                yield lines[x:y]


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
