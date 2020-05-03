import multiprocessing as mp,os
from monitor import harward_info, watch_memory
import logging
import time


logger = logging.getLogger()
info_pc = harward_info()


class ManagerWorker:

    def __init__(self):     
        self.started_at = time.time()         
        self.jobs = []

    
    def create_jobs(self, process_wrapper, args_job): #(self.fname, start_block,end_block)
        p = mp.Process(target=process_wrapper, args=args_job )   
        self.jobs.append(p)


    def wait_jobs(self):
        for proc in self.jobs:
            proc.join()  


    def execute_job(self):        
        for proc in self.jobs:
            watch_memory()
            logger.info("procesando job - {}".format(proc.name))    
            proc.start()
        self.wait_jobs()    



    