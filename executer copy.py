import multiprocessing as mp,os
from monitor import harward_info, memory_use


def process(line):
    f = open("demofile.txt", "a")
    f.write(line)


def process_wrapper(start_block, end_block):
    with open("technical_challenge_data.csv") as f:
        f.seek(start_block)
        lines = f.read(end_block).splitlines()
        for line in lines:
            process(line)

def chunkify(fname,size):
    fileEnd = os.path.getsize(fname)
    with open(fname,'rb') as f:
        end_block = f.tell()
        while True:
            start_block = end_block
            f.seek(size,1)
            line =f.readline()
            end_block = f.tell()
            end_block = end_block - start_block
            yield start_block, end_block
            if end_block > fileEnd:
                break

#init objects
info_pc = harward_info()
cant_cpu = info_pc['processor']
pool = mp.Pool(cant_cpu)
jobs = []
size = os.path.getsize("technical_challenge_data.csv")
slices_file = round(size/cant_cpu)
bloques = chunkify("technical_challenge_data.csv", slices_file)

#create jobs
cnt = 0
for start_block,end_block in bloques:
    p = mp.Process(name="{0}".format(cnt), target=process_wrapper, args=(start_block,end_block))  
    jobs.append(p)
    cnt = cnt +1

# Arrancamos a todos los hijos
print("PADRE: arrancando hijos")
for proceso in jobs:
    memory = memory_use()
    if memory < 90 :
        print("rango  {} - {}".format(start_block, end_block))                
    else:
        print("memory al  {} - esperando a liberar".format(memory))
        esperar = True
        while esperar:
            memory = memory_use()
            if memory < 70:
                    esperar = False
        print("memory al  {} - retomando actividad".format(memory))   
    proceso.start()

print("PADRE: esperando a que los procesos hijos hagan su trabajo")
for proceso in jobs:
    proceso.join()
