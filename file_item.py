import os

MEGABYTES =1024*1024

class FileItem():

    
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
        if len(lines) <=500:
            yield lines
        for n in range(3):
            x = (tope*n)
            y = ((n+1)*tope)
            if n==2:
                yield lines[x:]
            else:
                yield lines[x:y]
