
from DIRAC.TransformationSystem.Client.TransformationClient   import TransformationClient

import random
import time
import threading



class stepOne(threading.Thread):
    # Costruttore
    def __init__(self, id):
        threading.Thread.__init__(self)
        self.id = id

    # Ridefinizione del metono run del thread
    def run(self):
        #
        # Si aggiunge una Trasformazione
        #
        self.pos_time = 0
        self.time = [None]*3
        self.transName = "transName" + str(self.id)
        self.time_start = time.time()
        error = 0
        deadlock = 1
        while (deadlock == 1):
            res = transClient.addTransformation( self.transName, 'description', 'longDescription', 'MCSimulation', 'Standard','Manual', '' )
            if res['OK']==True:
                print "((( THREAD ",self.id,"))) **** Trasformazione Creata ****"
                deadlock = 0
            else:
                if (res["Message"] == "Execution failed.: ( 1213: Deadlock found when trying to get lock; try restarting transaction )"):
                    deadlock = 1
                else:
                    deadlock = 0
                    error = 1
        self.time_end = time.time()
        self.time[self.pos_time] =  self.time_end -  self.time_start
        self.pos_time = self.pos_time +1
        if (error == 1):
            print "((( THREAD ",self.id,"))) \033[1;31m#### ERROR: Trasformazione non creata ####\033[1;m"
            print res['Message']
        self.transID = res['Value']
        # Si mette il valore del TransID in coda alla lista transID del main 
        self.n = self.id-1
        transID[self.n] = self.transID
    
        #
        # Si aggiungono rand File alla Trasformazione
        #
        self.n_file = random.randint(1000,2000)
        print "((( THREAD ",self.id,"))) Sto creando ",self.n_file," File .... "
        self.lfns=[]
        # Si assembla il nome del file per generarli con nomi diversi
        for n in range( self.n_file ):
            self.lfns.append( "/aa/lfn." + str(n) + "." + str(self.id) + ".txt" )
        self.time_start = time.time()
        error = 0
        deadlock = 1
        while (deadlock == 1):
            res = transClient.addFilesToTransformation( self.transID, self.lfns )
            if res['OK']==True:
                print "((( THREAD ",self.id,"))) **** Creati ",self.n_file," File  ****"
                deadlock = 0
            else:
                if (res["Message"] == "Execution failed.: ( 1213: Deadlock found when trying to get lock; try restarting transaction )"):
                    deadlock = 1
                else:
                    deadlock = 0
                    error = 1
        self.time_end = time.time()
        self.time[self.pos_time] =  self.time_end -  self.time_start
        self.pos_time = self.pos_time +1
        if (error == 1):
            print "((( THREAD ",self.id,"))) \033[1;31m#### ERROR: File non creati ####\033[1;m"
            print res['Message']
        # Si mette la lista dei nomi dei file in coda alla lista lfns del main
        lfns[self.n] = self.lfns

        #
        # Si aggiunge un Task ogni rand(1,100) file
        #
        print "((( THREAD ",self.id,"))) Sto creando i Task .... "
        self.min = 0
        self.max = random.randint(1,100)
        self.taskID = []
        self.n_task = 0
        self.time_start = time.time()
        while( self.max <= self.n_file ):
            self.lfns_tmp = self.lfns[self.min:self.max]
            error = 0
            deadlock = 1
            while (deadlock == 1):
                res = transClient.addTaskForTransformation( self.transID, self.lfns_tmp )
                if res['OK']==True:
                    deadlock = 0
                    self.n_task = self.n_task+1
                else:
                    if (res["Message"] == "Execution failed.: ( 1213: Deadlock found when trying to get lock; try restarting transaction )"):
                        deadlock = 1
                    else:
                        deadlock = 0
                        error = 1
            if (error == 1):
                print "((( THREAD ",self.id,"))) \033[1;31m#### ERROR: Task non creato ####\033[1;m"
                print res['Message']
            self.taskID.append(res['Value'])
            self.rand = random.randint(0,30)
            self.min = self.max
            self.max = self.max + self.rand
        self.time_end = time.time()
        self.time[self.pos_time] =  self.time_end -  self.time_start
        self.pos_time = self.pos_time +1
        print "((( THREAD ",self.id,"))) **** Creati ",self.n_task," Tasks ****"
        # Si mettono in coda alle rispettive liste del main i valori di taskID, n_task (numero tot di task creati), min e n_file (numero tot di file)
        taskID[self.n] = self.taskID
        n_task[self.n] = self.n_task
        min[self.n] = self.min
        n_file[self.n] = self.n_file
        times_s1[self.n] = self.time



class stepTwo(threading.Thread):
    def __init__(self, id, transID, taskID, lfns, n_task):
        threading.Thread.__init__(self)
        self.id = id
        self.transID = transID
        self.taskID = taskID
        self.lfns = lfns
        self.n_task = n_task

    def run(self):
        #
        # Si setta lo stato dei primi rand(1,100) Task a 'Status_Modified'
        #
        self.time = [None]*2
        self.pos_time = 0
        self.rand = random.randint(1,100)
        # Si controlla che il numero random sia minore del numero dei Task, altrimenti se ne genera uno diverso
        while( self.rand > self.n_task ):
            self.rand = random.randint(1,100)
        self.status = "Status_Modified_TH"+str(self.id)
        self.taskID_tmp = self.taskID[0:self.rand]
        error = 0
        deadlock = 1
        self.time_start = time.time()
        while (deadlock == 1):
            res = transClient.setTaskStatus(self.transID, self.taskID_tmp, self.status)
            if res['OK']==True:
                print "((( THREAD ",self.id,"))) **** Stato di ",self.rand," Task settato a '",self.status,"' ****"
                deadlock = 0
            else:
                if (res["Message"] == "Execution failed.: ( 1213: Deadlock found when trying to get lock; try restarting transaction )"):
                    deadlock = 1
                else:
                    deadlock = 0
                    error = 1
        self.time_end = time.time()
        self.time[self.pos_time] =  self.time_end -  self.time_start
        self.pos_time = self.pos_time +1
        if (error == 1):
            print "((( THREAD ",self.id,"))) \033[1;31m#### ERROR: Stato del Task non settato ####\033[1;m"
            print res['Message']

        #
        # Si setta lo stato dei primi rand(1,20) File a 'Status_Modified'
        #
        self.rand = random.randint(1,20)
        self.newLFNsStatus = "Status_Modified_TH"+str(self.id)
        self.lfns_tmp = self.lfns[0:self.rand]
        self.time_start = time.time()
        error = 0
        deadlock = 1
        while (deadlock == 1):
            res = transClient.setFileStatusForTransformation( self.transID, self.newLFNsStatus, self.lfns_tmp )
            if res['OK']==True:
                print "((( THREAD ",self.id,"))) **** Stato dei primi ",self.rand," File settato a '",self.newLFNsStatus,"' ****"
                deadlock = 0
            else:
                if (res["Message"] == "Execution failed.: ( 1213: Deadlock found when trying to get lock; try restarting transaction )"):
                    deadlock = 1
                else:
                    deadlock = 0
                    error = 1
        self.time_end = time.time()
        self.time[self.pos_time] =  self.time_end -  self.time_start
        if (error == 1):
            print "((( THREAD ",self.id,"))) \033[1;31m#### ERROR: Stato dei File non modificati ####\033[1;m"
            print res['Message']
        self.n = self.id - 1
        times_s2[self.n] = self.time


    
class stepThree(threading.Thread):
    def __init__(self, id, transID, lfns, f,  min, n_file):
        threading.Thread.__init__(self)
        self.id = id
        self.transID = transID
        self.lfns = lfns
        self.f = [f]
        self.min = min
        self.n_file = n_file
    
    def run(self):
        #
        # get dello stato della Trasformazione e del Task
        #
        self.time = [None]*4
        self.pos_time = 0
        self.time_start = time.time()
        error = 0
        deadlock = 1
        while (deadlock == 1):
            res = transClient.getTransformationStats(self.transID)
            if res['OK']==True:
                self.statusTransf = res['Value']
                print "((( THREAD ",self.id,"))) **** Stato della Trasformazione recuperato ****"
                deadlock = 0
            else:
                if (res["Message"] == "Execution failed.: ( 1213: Deadlock found when trying to get lock; try restarting transaction )"):
                    deadlock = 1
                else:
                    deadlock = 0
                    error = 1
        self.time_end = time.time()
        self.time[self.pos_time] =  self.time_end -  self.time_start
        self.pos_time = self.pos_time +1
        if (error == 1):
            print "((( THREAD ",self.id,"))) \033[1;31m#### ERROR: Stato della Trasformazione impossibile da recuperare ####\033[1;m"
            print res['Message']

        self.time_start = time.time()
        error = 0
        deadlock = 1
        while (deadlock == 1):
            res = transClient.getTransformationTaskStats(self.transID)
            if res['OK']==True:
                self.statusTask = res['Value']
                print "((( THREAD ",self.id,"))) **** Stato del Task recuperato ****"
                deadlock = 0
            else:
                if (res["Message"] == "Execution failed.: ( 1213: Deadlock found when trying to get lock; try restarting transaction )"):
                    deadlock = 1
                else:
                    deadlock = 0
                    error = 1
        self.time_end = time.time()
        self.time[self.pos_time] =  self.time_end -  self.time_start
        self.pos_time = self.pos_time +1
        if (error == 1):
            print "((( THREAD ",self.id,"))) \033[1;31m#### ERROR: Stato del Task impossibile da recuperare ####\033[1;m"
            print res['Message']
        self.n = self.id - 1
        statusTransf[self.n] = self.statusTransf
        statusTask[self.n] = self.statusTask
      
        #
        # Si cambia ancora lo stato di un File e si aggiunge una Task
        #
        self.newLFNsStatus = "Second_Modified"
        self.lfns_tmp = self.lfns[0:1]
        self.time_start = time.time()
        error = 0
        deadlock = 1
        while (deadlock == 1):
            res = transClient.setFileStatusForTransformation( self.transID, self.newLFNsStatus, self.lfns_tmp )
            if res['OK']==True:
                print "((( THREAD ",self.id,"))) **** Stato di un File settato a '",self.newLFNsStatus,"' ****"
                deadlock = 0
            else:
                if (res["Message"] == "Execution failed.: ( 1213: Deadlock found when trying to get lock; try restarting transaction )"):
                    deadlock = 1
                else:
                    deadlock = 0
                    error = 1
        self.time_end = time.time()
        self.time[self.pos_time] =  self.time_end -  self.time_start
        self.pos_time = self.pos_time +1
        if (error == 1):
            print "((( THREAD ",self.id,"))) \033[1;31m#### ERROR: Stato del File non modificato per la seconda volta ####\033[1;m"
            print res['Message']

        self.time_start = time.time()
        error = 0
        deadlock = 1
        while (deadlock == 1):
            res = transClient.addTaskForTransformation( self.transID, self.f[0] )
            if res['OK']==True:
                print "((( THREAD ",self.id,"))) **** Aggiunto un Task ****"
                deadlock = 0
            else:
                if (res["Message"] == "Execution failed.: ( 1213: Deadlock found when trying to get lock; try restarting transaction )"):
                    deadlock = 1
                else:
                    deadlock = 0
                    error = 1
        self.time_end = time.time()
        self.time[self.pos_time] =  self.time_end -  self.time_start
        self.pos_time = self.pos_time +1
        if (error == 1):
            print "((( THREAD ",self.id,"))) \033[1;31m#### ERROR: Task non aggiunto ####\033[1;m"
            print res['Message']
        times_s3[self.n] = self.time



class stepFour(threading.Thread):
    def __init__( self, id, transID ):
        threading.Thread.__init__(self)
        self.id = id
        self.transID = transID
 
    def run(self):
        self.time = [None]*4
        self.time_start = time.time()
        error = 0
        deadlock = 1
        while (deadlock == 1):
            res = transClient.cleanTransformation( self.transID )
            if res['OK']==True:
                print "((( THREAD ",self.id,"))) **** Clean Transformation ****"
                deadlock = 0
            else:
                if (res["Message"] == "Execution failed.: ( 1213: Deadlock found when trying to get lock; try restarting transaction )"):
                    deadlock = 1
                else:
                    deadlock = 0
                    error = 1
        self.time_end = time.time()
        self.time = self.time_end -  self.time_start
        if (error == 1):
            print "((( THREAD ",self.id,"))) \033[1;31m#### ERROR: Clean Transformation ####\033[1;m"
            print res['Message']
        self.n = self.id - 1
        times_s4[self.n] = self.time



class stepFive(threading.Thread):
    def __init__( self, id, transID ):
        threading.Thread.__init__(self)
        self.id = id
        self.transID = transID

    def run(self):
        #
        # Si elimina una trasformazione
        #
        error = 0
        deadlock = 1
        while (deadlock == 1):
            res = transClient.deleteTransformation( self.transID )
            if res['OK']==True:
                print "((( THREAD ",self.id,"))) **** Trasformazione eliminata ****"
                deadlock = 0
            else:
                if (res["Message"] == "Execution failed.: ( 1213: Deadlock found when trying to get lock; try restarting transaction )"):
                    deadlock = 1
                else:
                    deadlock = 0
                    error = 1
        if (error == 1):  
            print "((( THREAD ",self.id,"))) \033[1;31m#### ERROR: Trasformazione non eliminata ####\033[1;m"
            print res['Message']



####MAIN####
if __name__ == "__main__":
    transClient = TransformationClient()
    
    print "\n"
    print " \033[1;31m|========================================|\033[1;m "
    print " \033[1;31m|    TEST JOB   -   TRANSFORMATION DB    |\033[1;m "
    print " \033[1;31m|========================================|\033[1;m "
    print "\n"

    time_tot=0

    print "\033[1;34m    STEP 1\033[1;m"
    print "\033[1;34m--------------\033[1;m"
    N_TH = 3
    th = []
    transID = [None]*N_TH
    lfns = [None]*N_TH
    taskID = [None]*N_TH
    n_task = [None]*N_TH
    min = [None]*N_TH
    n_file = [None]*N_TH
    times_s1 = [None]*N_TH
    # Instanziazione dei thread
    for i in range(1, N_TH+1):
        t = stepOne(i)
        th.append(t)
    # Si fanno partire i thread
    for i in range(1, N_TH+1):
        th[i-1].start()
    # Il main aspetta la fine di tutti i thread per proseguire la sua exec
    for i in range(1, N_TH+1):
        th[i-1].join()
    for i in range(1,N_TH+1):
        print "\n"
        print "((( THREAD ",i,")))"
        print "TaskID: ",transID[i-1]
        print "Numero File creati: ",n_file[i-1]
        print "Numero Task creati: ",n_task[i-1]
        print "Time [insert T, add F, insert TT]: ",times_s1[i-1]," secondi"
    print "\n"
    print "TaskID: ",taskID
    print " \n"
    time_tot_s = 0
    for i in range(0,len(times_s1)):
        for j in range(0,len(times_s1[i])):
            time_tot_s = time_tot_s + times_s1[i][j]
    print "-------------------------------------------------------------"
    print "Impiegati", time_tot_s, "secondi per eseguire lo step 1"
    print "-------------------------------------------------------------"
    time_tot = time_tot + time_tot_s
    print "\n"

    print "\033[1;34m    STEP 2\033[1;m"
    print "\033[1;34m--------------\033[1;m"
    th = []
    N_TH = 5
    times_s2 = [None]*N_TH
    r = random.randint(0,2)
    print "Operazioni eseguite sulla ",r+1," Trasformazione con ID: ",transID[r]
    # Instanziazione dei thread
    for i in range(1, N_TH+1):
        t = stepTwo(i, transID[r], taskID[r], lfns[r], n_task[r])
        th.append(t)
    # Si fanno partire i thread
    for i in range(1, N_TH+1):
        th[i-1].start()
    # Il main aspetta la fine di tutti i thread per proseguire la sua exec
    for i in range(1, N_TH+1):
        th[i-1].join()
    for i in range (1,N_TH+1):
        print "\n"
        print "((( THREAD ",i,")))"
        print "Time [change status TT, change status F]: ",times_s2[i-1]," secondi"
    time_tot_s = 0
    for i in range(0,len(times_s2)):
        for j in range(0,len(times_s2[i])):
            time_tot_s = time_tot_s + times_s2[i][j]
    print "\n"
    print "-------------------------------------------------------------"
    print "Impiegati", time_tot_s, "secondi per eseguire lo step 2"
    print "-------------------------------------------------------------"
    time_tot = time_tot + time_tot_s
    print "\n"

    print "\033[1;34m    STEP 3\033[1;m"
    print "\033[1;34m--------------\033[1;m"
    th = []
    N_TH = 4
    times_s3 = [None]*N_TH
    statusTransf = [None]*N_TH
    statusTask = [None]*N_TH
    r = random.randint(0,2)
    print "Operazioni eseguite sulla ",r+1," Trasformazione con ID: ",transID[r]
    min_tmp = min[r]
    if ((min_tmp + N_TH) >= n_file[r]):
        # Si crea File nuovi per creare un Task
        diff_file = ((min_tmp + N_TH) - n_file[r]) +1 
        # Si crea il numero giusto di file
        for i in range(0,diff_file):
            min_tmp = min_tmp + 1
            lfns_tmp = "/aa/lfn." + str(min_tmp) + "." + str(r) + ".txt"
            res = transClient.addFilesToTransformation( transID[r], [lfns_tmp] )
            if res['OK']==True:
                print "**** Creato un File per creare un nuovo Task ****"
                lfns[r].append(lfns_tmp)
            else:
                print "\033[1;31m#### ERROR: File per aggiungere i Task non creati ####\033[1;m"
                print res['Message']
    min_tmp = min[r]
    f = []
    for i in range(0,N_TH):
        min_tmp = min_tmp + 1
        print "min_tmp: ",min_tmp
        print "len(lfns[r])",len(lfns[r])
        f.append([lfns[r][min_tmp]])
    print "f: ",f
    # Instanziazione dei thread
    for i in range(1, N_TH+1):
        t = stepThree(i, transID[r], lfns[r], f[i-1], min[r], n_file[r])
        th.append(t)
    # Si fanno partire i thread
    for i in range(1, N_TH+1):
        th[i-1].start()
    # Il main aspetta la fine di tutti i thread per proseguire la sua exec
    for i in range(1, N_TH+1):
        th[i-1].join()
    for i in range (1,N_TH+1):
        print "\n"
        print "((( THREAD ",i,")))"
        print "Stato della Trasformazione: ",statusTransf[i-1]
        print "Stato del Task: ",statusTask[i-1]
        print "Time [Get transStatus, Get taskStatus, change status F, add TT]: ",times_s3[i-1]," secondi"
    time_tot_s = 0
    for i in range(0,len(times_s3)):
        for j in range(0,len(times_s3[i])):
            time_tot_s = time_tot_s + times_s3[i][j]
    print "\n"
    print "-------------------------------------------------------------"
    print "Impiegati", time_tot_s, "secondi per eseguire lo step 3"
    print "-------------------------------------------------------------"
    time_tot = time_tot + time_tot_s
    print "\n"

    print "\033[1;34m    STEP 4\033[1;m"
    print "\033[1;34m--------------\033[1;m"
    th = []
    N_TH = 3
    times_s4 = [None]*N_TH
    # Instanziazione dei thread
    for i in range(1, N_TH+1):
        t = stepFour(i, transID[i-1])
        th.append(t)
    # Si fanno partire i thread
    for i in range(1, N_TH+1):
        th[i-1].start()
    # Il main aspetta la fine di tutti i thread per proseguire la sua exec
    for i in range(1, N_TH+1):
        th[i-1].join()
    for i in range (1,N_TH+1):
        print "\n"
        print "((( THREAD ",i,")))"
        print "Time [Clear]: ",times_s4[i-1]," secondi"
    time_tot_s = 0
    for i in range(0,len(times_s4)):
        time_tot_s = time_tot_s + times_s4[i]
    print "\n"
    print "-------------------------------------------------------------"
    print "Impiegati", time_tot_s, "secondi per eseguire lo step 4"
    print "-------------------------------------------------------------"
    time_tot = time_tot + time_tot_s
    print "\n"

    print "\033[1;34m    Elliminazione Transformation\033[1;m"
    print "\033[1;34m------------------------------------\033[1;m"
    th = []
    N_TH = 3
    # Instanziazione dei thread
    for i in range(1, N_TH+1):
        t = stepFive(i, transID[i-1])
        th.append(t)
    # Si fanno partire i thread
    for i in range(1, N_TH+1):
        th[i-1].start()
    # Il main aspetta la fine di tutti i thread per proseguire la sua exec
    for i in range(1, N_TH+1):
        th[i-1].join()
    
    print "\n"
    print "\033[1;32m-------------------------------------------------------------\033[1;m"
    print "\033[1;32mImpiegati " + str(time_tot) + " secondi per eseguire il job\033[1;m"
    print "\033[1;32m-------------------------------------------------------------\033[1;m"
