import multiprocessing as mp,os
from monitor import harward_info, memory_use, config_logging, profile
from strategies.strategy import Strategy
import logging
import psutil
import time


""" Logging """
config_logging('./logs', 'app', 'INFO')
logger = logging.getLogger()

@profile
def main():
    strategy = Strategy('./resource/technical_challenge_data.csv','utf-8','csv', ';', ',')  
    file_builder = strategy.get_strategy()
    file_builder.procesar_file()

if __name__ == '__main__':  
    main()
    