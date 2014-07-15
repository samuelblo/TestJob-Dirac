
from DIRAC.TransformationSystem.Client.TransformationClient   import TransformationClient


import random
import time
import threading
import uuid




class stepOne(threading.Thread):
    # Costruttore
    def __init__(self, id, str_uuid):
        threading.Thread.__init__(self)
        self.id = id
        self.uuid_str = str_uuid

    # Ridefinizione del metodo run del thread
    def run(self):
        # -------------------------------
        # Si aggiunge una Trasformazione
        # -------------------------------
        self.time = []
        self.transName = "transName" + str(self.id) + str(self.uuid_str)
        
        self.time_start = time.time()
        self.dl_count = 0
        self.deadlock = 1
        while (self.deadlock == 1):
            self.res = transClient.addTransformation( self.transName, 'description', 'longDescription', 'MCSimulation', 'Standard','Manual', '' )
            if self.res['OK']==True:
                print "((( THREAD ",self.id,"))) **** Trasformazione Creata ****"
                self.deadlock = 0
            else:
                if (self.res["Message"] == "Execution failed.: ( 1213: Deadlock found when trying to get lock; try restarting transaction )"):
                    self.deadlock = 1
                    self.dl_count += 1
                else:
                    self.deadlock = 0
                    print "((( THREAD ",self.id,"))) \033[1;31m#### ERROR: Trasformazione non creata ####\033[1;m"
                    print self.res['Message']
        self.time.append( time.time() -  self.time_start )
        
        self.transID = self.res['Value']
        # Si mette il valore del TransID in coda alla lista transID del main 
        transID[self.id-1] = self.transID
    

        # -------------------------------------------------------------------
        # Si aggiungono [random(MIN_FILE,MAX_FILE)] File alla Trasformazione
        # ------------------------------------------------------------------- 
        self.n_file = random.randint(MIN_FILE,MAX_FILE)
        print "((( THREAD ",self.id,"))) Sto creando ",self.n_file," File .... "
        self.lfns = []
        # Si genera la lista con i nomi dei File da inserire
        for n in range( self.n_file ):
            #self.lfns.append( "/aa/lfn." + str(n) + "." + str(self.id) + ".txt" )
            self.lfns.append( "/aa/lfn." + str(n) + "." + str(self.id) + "." + str(self.uuid_str) + ".txt" )

        self.time_start = time.time()
        self.index_min = 0
        self.index_max = BLOCK_ADD_FILE
        # Si controlla se BLOCK_ADD_FILE e' minore della grandella della lista dei nomi dei File
        if (self.index_max < (len(self.lfns)-1)):
            # si entra nel ciclo while
            self.exit = 0
        else:
            # si salta il ciclo while
            self.exit = 1
        # Si divide la lista con i nomi dei File in blocchi da BLOCK_ADD_FILE
        while(self.exit == 0):
            self.deadlock = 1
            while (self.deadlock == 1):
                self.lfns_temp = []
                self.lfns_temp = self.lfns[self.index_min:self.index_max]
                self.res = transClient.addFilesToTransformation( self.transID, self.lfns_temp )
                if self.res['OK']==True:
                    self.deadlock = 0
                else:
                    if (self.res["Message"] == "Execution failed.: ( 1213: Deadlock found when trying to get lock; try restarting transaction )"):
                        self.deadlock = 1
                        self.dl_count += 1
                    else:
                        self.deadlock = 0
                        print "((( THREAD ",self.id,"))) \033[1;31m#### ERROR: File non creati ####\033[1;m"
                        print self.res['Message']
            self.index_min = self.index_max
            if ((self.index_max + BLOCK_ADD_FILE)<(len(self.lfns)-1)):
                self.index_max += BLOCK_ADD_FILE
            else:
                self.exit = 1
        self.deadlock = 1
        while (self.deadlock == 1):
            self.lfns_temp = []
            self.lfns_temp = self.lfns[self.index_min:(len(self.lfns))]
            self.res = transClient.addFilesToTransformation( self.transID, self.lfns_temp )
            if self.res['OK']==True:
                self.deadlock = 0
            else:
                if (self.res["Message"] == "Execution failed.: ( 1213: Deadlock found when trying to get lock; try restarting transaction )"):
                    self.deadlock = 1
                    self.dl_count += 1
                else:
                    self.deadlock = 0
                    print "((( THREAD ",self.id,"))) \033[1;31m#### ERROR: File non creati ####\033[1;m"
                    print self.res['Message']
        self.time.append(time.time() -  self.time_start)

        print "((( THREAD ",self.id,"))) **** Creati ",self.n_file," File  ****"
        # Si mette la lista dei nomi dei file in coda alla lista lfns del main
        lfns[self.id-1] = self.lfns


        # --------------------------------------------------------------------------
        # Si aggiunge un Task ogni [random(MIN_FILE_TO_TASK,MAX_FILE_TO_TASK)] File
        # --------------------------------------------------------------------------
        print "((( THREAD ",self.id,"))) Sto creando i Task .... "
        self.min = 0
        self.max = random.randint(MIN_FILE_TO_TASK,MAX_FILE_TO_TASK)
        self.taskID = []
        self.n_task = 0

        self.time_start = time.time()
        while( self.max <= self.n_file ):
            self.lfns_tmp = self.lfns[self.min:self.max]
            self.deadlock = 1
            while (self.deadlock == 1):
                self.res = transClient.addTaskForTransformation( self.transID, self.lfns_tmp )
                if self.res['OK']==True:
                    self.deadlock = 0
                    self.n_task += 1
                else:
                    if (self.res["Message"] == "Execution failed.: ( 1213: Deadlock found when trying to get lock; try restarting transaction )"):
                        self.deadlock = 1
                        self.dl_count += 1
                    else:
                        self.deadlock = 0
                        print "((( THREAD ",self.id,"))) \033[1;31m#### ERROR: Task non creato ####\033[1;m"
                        print self.res['Message']
            self.taskID.append(self.res['Value'])
            self.rand = random.randint(MIN_FILE_TO_TASK,MAX_FILE_TO_TASK)
            self.min = self.max
            self.max = self.max + self.rand
        self.time.append(time.time() -  self.time_start)

        print "((( THREAD ",self.id,"))) **** Creati ",self.n_task," Tasks ****"
        # Si mettono in coda alle rispettive liste del main i valori di taskID, n_task (numero tot di task creati), min e n_file (numero tot di file)
        taskID[self.id-1] = self.taskID
        n_task[self.id-1] = self.n_task
        min[self.id-1] = self.min
        n_file[self.id-1] = self.n_file
        times_s1[self.id-1] = self.time
        dl_count_step1[self.id-1] = self.dl_count




class stepTwo(threading.Thread):
    def __init__(self, id, transID, taskID, lfns, n_task):
        threading.Thread.__init__(self)
        self.id = id
        self.transID = transID
        self.taskID = taskID
        self.lfns = lfns
        self.n_task = n_task

    def run(self):
        # ---------------------------------------------------------------------------------------------------------
        # Si setta lo stato dei primi [random(MIN_CHANGE_STATUS_TT,MAX_CHANGE_STATUS_TT)] Task a 'Status_Modified'
        # ---------------------------------------------------------------------------------------------------------
        self.time = []
        self.rand = random.randint(MIN_CHANGE_STATUS_TT,MAX_CHANGE_STATUS_TT)
        # Si controlla che il numero random sia minore del numero dei Task, altrimenti se ne genera uno diverso
        while( self.rand > self.n_task ):
            self.rand = random.randint(MIN_CHANGE_STATUS_TT,MAX_CHANGE_STATUS_TT)
        self.status = "Status_Modified_TH"+str(self.id)
        self.taskID_tmp = self.taskID[0:self.rand]
        self.dl_count = 0
        self.deadlock = 1

        self.time_start = time.time()
        while (self.deadlock == 1):
            self.res = transClient.setTaskStatus(self.transID, self.taskID_tmp, self.status)
            if self.res['OK']==True:
                print "((( THREAD ",self.id,"))) **** Stato di ",self.rand," Task settato a '",self.status,"' ****"
                self.deadlock = 0
            else:
                if (self.res["Message"] == "Execution failed.: ( 1213: Deadlock found when trying to get lock; try restarting transaction )"):
                    self.deadlock = 1
                    self.dl_count += 1
                else:
                    self.deadlock = 0
                    print "((( THREAD ",self.id,"))) \033[1;31m#### ERROR: Stato del Task non settato ####\033[1;m"
                    print self.res['Message']
        self.time.append(time.time() -  self.time_start)


        # -------------------------------------------------------------------------------------------------------------
        # Si setta lo stato dei primi [random(MIN_CHANGE_STATUS_FILE,MAX_CHANGE_STATUS_FILE)] File a 'Status_Modified'
        # -------------------------------------------------------------------------------------------------------------
        self.rand = random.randint(MIN_CHANGE_STATUS_FILE,MAX_CHANGE_STATUS_FILE)
        self.newLFNsStatus = "Status_Modified_TH"+str(self.id)
        self.lfns_tmp = self.lfns[0:self.rand]

        self.time_start = time.time()
        self.deadlock = 1
        while (self.deadlock == 1):
            self.res = transClient.setFileStatusForTransformation( self.transID, self.newLFNsStatus, self.lfns_tmp )
            if self.res['OK']==True:
                print "((( THREAD ",self.id,"))) **** Stato dei primi ",self.rand," File settato a '",self.newLFNsStatus,"' ****"
                self.deadlock = 0
            else:
                if (self.res["Message"] == "Execution failed.: ( 1213: Deadlock found when trying to get lock; try restarting transaction )"):
                    self.deadlock = 1
                    self.dl_count += 1
                else:
                    self.deadlock = 0
                    print "((( THREAD ",self.id,"))) \033[1;31m#### ERROR: Stato dei File non modificati ####\033[1;m"
                    print self.res['Message']
        self.time.append(time.time() -  self.time_start)

        times_s2[self.id-1] = self.time
        dl_count_step2[self.id-1] = self.dl_count



    
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
        # ----------------------------------------
        # get stato della Trasformazione
        #
        # get stato dei Task di una Trasformazione
        # -----------------------------------------
        self.time = []

        self.time_start = time.time()
        self.dl_count = 0
        self.deadlock = 1
        while (self.deadlock == 1):
            self.res = transClient.getTransformationStats(self.transID)
            if self.res['OK']==True:
                self.statusTransf = self.res['Value']
                print "((( THREAD ",self.id,"))) **** Stato della Trasformazione recuperato ****"
                self.deadlock = 0
            else:
                if (self.res["Message"] == "Execution failed.: ( 1213: Deadlock found when trying to get lock; try restarting transaction )"):
                    self.deadlock = 1
                    self.dl_count += 1
                else:
                    self.deadlock = 0
                    print "((( THREAD ",self.id,"))) \033[1;31m#### ERROR: Stato della Trasformazione impossibile da recuperare ####\033[1;m"
                    print self.res['Message']
        self.time.append(time.time() -  self.time_start)

        self.time_start = time.time()
        self.deadlock = 1
        while (self.deadlock == 1):
            self.res = transClient.getTransformationTaskStats(self.transID)
            if self.res['OK']==True:
                self.statusTask = self.res['Value']
                print "((( THREAD ",self.id,"))) **** Stato del Task recuperato ****"
                self.deadlock = 0
            else:
                if (self.res["Message"] == "Execution failed.: ( 1213: Deadlock found when trying to get lock; try restarting transaction )"):
                    self.deadlock = 1
                    self.dl_count += 1
                else:
                    self.deadlock = 0
                    print "((( THREAD ",self.id,"))) \033[1;31m#### ERROR: Stato del Task impossibile da recuperare ####\033[1;m"
                    print self.res['Message']
        self.time.append(time.time() -  self.time_start)
      

        # ---------------------------------
        # Modifica dello stato di un File
        #
        # Aggiunta di un Task
        # ---------------------------------
        self.newLFNsStatus = "Second_Modified"
        self.lfns_tmp = self.lfns[0:1]

        self.time_start = time.time()
        self.deadlock = 1
        while (self.deadlock == 1):
            self.res = transClient.setFileStatusForTransformation( self.transID, self.newLFNsStatus, self.lfns_tmp )
            if self.res['OK']==True:
                print "((( THREAD ",self.id,"))) **** Stato di un File settato a '",self.newLFNsStatus,"' ****"
                self.deadlock = 0
            else:
                if (self.res["Message"] == "Execution failed.: ( 1213: Deadlock found when trying to get lock; try restarting transaction )"):
                    self.deadlock = 1
                    self.dl_count += 1
                else:
                    self.deadlock = 0
                    print "((( THREAD ",self.id,"))) \033[1;31m#### ERROR: Stato del File non modificato per la seconda volta ####\033[1;m"
                    print self.res['Message']
        self.time.append(time.time() -  self.time_start)

        self.time_start = time.time()
        self.deadlock = 1
        while (self.deadlock == 1):
            self.res = transClient.addTaskForTransformation( self.transID, self.f[0] )
            if self.res['OK']==True:
                print "((( THREAD ",self.id,"))) **** Aggiunto un Task ****"
                self.deadlock = 0
            else:
                if (self.res["Message"] == "Execution failed.: ( 1213: Deadlock found when trying to get lock; try restarting transaction )"):
                    self.deadlock = 1
                    self.dl_count += 1
                else:
                    self.deadlock = 0
                    print "((( THREAD ",self.id,"))) \033[1;31m#### ERROR: Task non aggiunto ####\033[1;m"
                    print self.res['Message']
        self.time.append(time.time() -  self.time_start)
        
        times_s3[self.id-1] = self.time
        dl_count_step3[self.id-1] = self.dl_count




class stepFour(threading.Thread):
    def __init__( self, id, transID, n_task ):
        threading.Thread.__init__(self)
        self.id = id
        self.transID = transID
        self.n_task = n_task
 
    def run(self):
        # ------------------------------------------------------------------------------
        # get dello stato di una Trasformazione (per [READ_TRANS_STATUS] volte)
        #
        # get dello stato dei Task di una Transformation (per [READ_TASK_STATUS] volte)
        # ------------------------------------------------------------------------------
        self.time = []
        self.dl_count = 0
        self.c = 0
        
        self.time_start = time.time()
        for i in range (READ_TRANS_STATUS):
            self.deadlock = 1
            while (self.deadlock == 1):
                self.res = transClient.getTransformationStats(self.transID)
                if self.res['OK']==True:
                    self.statusTransf = self.res['Value']
                    self.c += 1
                    self.deadlock = 0
                else:
                    if (self.res["Message"] == "Execution failed.: ( 1213: Deadlock found when trying to get lock; try restarting transaction )"):
                        self.deadlock = 1
                        self.dl_count += 1
                    else:
                        self.deadlock = 0
                        print "((( THREAD ",self.id,"))) \033[1;31m#### ERROR: Stato della Trasformazione impossibile da recuperare ####\033[1;m"
                        print self.res['Message']
        self.time.append(time.time() -  self.time_start)
        
        print "((( THREAD ",self.id,"))) **** Stato della Trasformazione recuperato (x",self.c,") ****"
        
        self.c = 0
        
        self.time_start = time.time()
        for i in range (READ_TASK_STATUS):
            self.deadlock = 1
            while (self.deadlock == 1):
                self.res = transClient.getTransformationTaskStats(self.transID)
                if self.res['OK']==True:
                    self.statusTask = self.res['Value']
                    self.c += 1
                    self.deadlock = 0
                else:
                    if (self.res["Message"] == "Execution failed.: ( 1213: Deadlock found when trying to get lock; try restarting transaction )"):
                        self.deadlock = 1
                        self.dl_count += 1
                    else:
                        self.deadlock = 0
                        print "((( THREAD ",self.id,"))) \033[1;31m#### ERROR: Stato del Task impossibile da recuperare ####\033[1;m"
                        print self.res['Message']
        self.time.append(time.time() -  self.time_start)
        
        print "((( THREAD ",self.id,"))) **** Stato dei Task recuperati (x",self.c,") ****"
        times_s4[self.id-1] = self.time
        dl_count_step4[self.id-1] = self.dl_count




class stepFive(threading.Thread):
    def __init__( self, id, transID ):
        threading.Thread.__init__(self)
        self.id = id
        self.transID = transID

    def run(self):
        #
        # Si elimina una trasformazione
        #
        self.deadlock = 1
        while (self.deadlock == 1):
            self.res = transClient.deleteTransformation( self.transID )
            if self.res['OK']==True:
                print "((( THREAD ",self.id,"))) **** Trasformazione eliminata ****"
                self.deadlock = 0
            else:
                if (self.res["Message"] == "Execution failed.: ( 1213: Deadlock found when trying to get lock; try restarting transaction )"):
                    self.deadlock = 1
                else:
                    self.deadlock = 0
                    print "((( THREAD ",self.id,"))) \033[1;31m#### ERROR: Trasformazione non eliminata ####\033[1;m"
                    print self.res['Message']




##################
####   MAIN   ####
##################
if __name__ == "__main__":
    transClient = TransformationClient()

    # ----------------------------
    #           COSTANTI         
    # ----------------------------
    MIN_FILE = 100
    MAX_FILE = 100
    BLOCK_ADD_FILE = 10000

    MIN_FILE_TO_TASK = 10
    MAX_FILE_TO_TASK = 10

    MIN_CHANGE_STATUS_TT = 2
    MAX_CHANGE_STATUS_TT = 2

    MIN_CHANGE_STATUS_FILE = 3
    MAX_CHANGE_STATUS_FILE = 3

    READ_TRANS_STATUS = 6
    READ_TASK_STATUS = 7

    N_TH_STEP_1 = 3
    N_TH_STEP_2 = 4
    N_TH_STEP_3 = 5
    N_TH_STEP_4 = 2
    N_TH_STEP_5 = N_TH_STEP_1 
    
    print "\n"
    print " \033[1;31m|========================================|\033[1;m "
    print " \033[1;31m|    TEST JOB   -   TRANSFORMATION DB    |\033[1;m "
    print " \033[1;31m|========================================|\033[1;m "
    print "\n"

    time_tot=0



    print "\033[1;34m    STEP 1\033[1;m"
    print "\033[1;34m--------------\033[1;m"
    th = []
    transID = [None]*N_TH_STEP_1
    lfns = [None]*N_TH_STEP_1
    taskID = [None]*N_TH_STEP_1
    n_task = [None]*N_TH_STEP_1
    min = [None]*N_TH_STEP_1
    n_file = [None]*N_TH_STEP_1
    times_s1 = [None]*N_TH_STEP_1
    dl_count_step1 = [None]*N_TH_STEP_1
    uuid_str = uuid.uuid1()
    # Instanziazione dei thread
    for i in range(1, N_TH_STEP_1+1):
        t = stepOne(i, uuid_str)
        th.append(t)
    # Si fanno partire i thread
    for i in range(1, N_TH_STEP_1+1):
        th[i-1].start()
    # Il main aspetta la fine di tutti i thread per proseguire la sua exec
    for i in range(1, N_TH_STEP_1+1):
        th[i-1].join()
    print " \n"
    time_tot_s = 0
    t_tmp = 0
    list_media_t = [None]*len(times_s1[0])
    indice = 0
    for j in range(0,len(times_s1[indice])):    # si scorrono le colonne
        for i in range(0,len(times_s1)):        # si scorrono le righe
            t_tmp = t_tmp + times_s1[i][j]      # si sommano gli elementi della stessa colonna
        list_media_t[indice] = t_tmp / len(times_s1)          # si salva il valore della media degli elemeti della stessa colonna in una lista
        t_tmp = 0
        indice += 1
    for i in range(0,len(list_media_t)):
        time_tot_s = time_tot_s + list_media_t[i]
    for i in range(1,N_TH_STEP_1+1):
        print "\n"
        print "((( THREAD ",i,")))"
        print "TransID: ",transID[i-1]
        print "Numero File creati: ",n_file[i-1]
        print "Numero Task creati: ",n_task[i-1]
        print "Numero di deadlock generati: ",dl_count_step1[i-1]
        print "Time [insert T, add F, insert TT]: ",times_s1[i-1]," secondi"
    print "\n"
    print "Time medio [insert T, add F, insert TT]: ",list_media_t," secondi"
    print "\n"
    print "-------------------------------------------------------------"
    print "Impiegati", time_tot_s, "secondi per eseguire lo step 1"
    print "-------------------------------------------------------------"
    time_tot = time_tot + time_tot_s
    print "\n"
 

 
    print "\033[1;34m    STEP 2 + STEP 3\033[1;m"
    print "\033[1;34m----------------------\033[1;m"
    th = []
    N_TH = N_TH_STEP_2 + N_TH_STEP_3
    times_s2 = [None]*N_TH_STEP_2
    times_s3 = [None]*N_TH_STEP_3
    dl_count_step2 = [None]*N_TH_STEP_2
    dl_count_step3 = [None]*N_TH_STEP_3
    f = []
    for r in range(0,N_TH_STEP_1):
        min_tmp = min[r]
        if ((min_tmp + N_TH_STEP_3) >= n_file[r]):
            # Si verifica se (e quanti) File bisogna creare per aggiungere un Task per una specifica Trasformazione
            diff_file = ((min_tmp + N_TH_STEP_3) - n_file[r]) +1 
            # Si crea il numero giusto di File guardando se il numero di file da creare e' positivo
            if (diff_file > 0):
                for i in range(0,diff_file):
                    min_tmp += 1
                    #lfns_tmp = "/aa/lfn." + str(min_tmp) + "." + str(r) + ".txt"
                    lfns_tmp = "/aa/lfn." + str(uuid_str) + "." + str(i) + ".txt"
                    res = transClient.addFilesToTransformation( transID[r], [lfns_tmp] )
                    if res['OK']==True:
                        print "**** Creato un File per creare un nuovo Task (per il thread ",r+1,") ****"
                        lfns[r].append(lfns_tmp)
                    else:
                        print "\033[1;31m#### ERROR: File per aggiungere i Task non creati ####\033[1;m"
                        print res['Message']
        min_tmp = min[r]
        f_tmp = []
        for i in range(0,N_TH_STEP_3):
            min_tmp = min_tmp + 1
            f_tmp.append([lfns[r][min_tmp]])
        f.append(f_tmp)

    for i in range(1, N_TH_STEP_2 + 1):
        r = random.randint(0, N_TH_STEP_1 - 1)
        t = stepTwo(i, transID[r], taskID[r], lfns[r], n_task[r])
        th.append(t)
    for i in range(1, N_TH_STEP_3 + 1):
        r = random.randint(0, N_TH_STEP_1 - 1)  
        t = stepThree(i, transID[r], lfns[r], f[r][i-1], min[r], n_file[r])
        th.append(t)
    # Si fanno partire i thread
    for i in range(1, N_TH + 1):
        th[i-1].start()
    # Il main aspetta la fine di tutti i thread per proseguire la sua exec
    for i in range(1, N_TH + 1):
        th[i-1].join()

    print "\n"
    print "STEP 2:"
    time_tot_s = 0
    t_tmp = 0
    list_media_t = [None]*len(times_s2[0])
    indice = 0
    for j in range(0,len(times_s2[indice])):    # si scorrono le colonne
        for i in range(0,len(times_s2)):        # si scorrono le righe
            t_tmp = t_tmp + times_s2[i][j]      # si sommano gli elementi della stessa colonna
        list_media_t[indice] = t_tmp / len(times_s2)          # si salva il valore della media degli elemeti della stessa colonna in una lista
        t_tmp = 0
        indice += 1
    for i in range(0,len(list_media_t)):
        time_tot_s = time_tot_s + list_media_t[i]
    for i in range (1,N_TH_STEP_2 + 1):
        print "((( THREAD ",i,")))"
        print "Numero di deadlock generati: ",dl_count_step2[i-1]
        print "Time [change status TT, change status F]: ",times_s2[i-1]," secondi"
    print "\n"
    print "Time medio [change status TT, change status F]: ",list_media_t," secondi"
    print "\n"
    print "-------------------------------------------------------------"
    print "Impiegati", time_tot_s, "secondi per eseguire lo step 2"
    print "-------------------------------------------------------------"
    time_tot = time_tot + time_tot_s
    
    print "\n"
    print "STEP 3:"
    time_tot_s = 0
    t_tmp = 0
    list_media_t = [None]*len(times_s3[0])
    indice = 0
    for j in range(0,len(times_s3[indice])):    # si scorrono le colonne
        for i in range(0,len(times_s3)):        # si scorrono le righe
            t_tmp = t_tmp + times_s3[i][j]      # si sommano gli elementi della stessa colonna
        list_media_t[indice] = t_tmp / len(times_s3)          # si salva il valore della media degli elemeti della stessa colonna in una lista
        t_tmp = 0
        indice += 1
    for i in range(0,len(list_media_t)):
        time_tot_s = time_tot_s + list_media_t[i]
    for i in range (1,N_TH_STEP_3 + 1):
        print "((( THREAD ",i,")))"
        print "Numero di deadlock generati: ",dl_count_step3[i-1]
        print "Time [Get transStatus, Get taskStatus, change status F, add TT]: ",times_s3[i-1]," secondi"
    print "\n"
    print "Time medio [Get transStatus, Get taskStatus, change status F, add TT]: ",list_media_t," secondi"
    print "\n"
    print "-------------------------------------------------------------"
    print "Impiegati", time_tot_s, "secondi per eseguire lo step 3"
    print "-------------------------------------------------------------"
    time_tot = time_tot + time_tot_s
    print "\n"



    print "\033[1;34m    STEP 4\033[1;m"
    print "\033[1;34m--------------\033[1;m"
    th = []
    times_s4 = [None]*N_TH_STEP_4
    dl_count_step4 = [None]*N_TH_STEP_4
    # Instanziazione dei thread
    for i in range(1, N_TH_STEP_4 + 1):
        r = random.randint(0, N_TH_STEP_1 - 1)
        t = stepFour(i, transID[r], n_task[r])
        th.append(t)
    # Si fanno partire i thread
    for i in range(1, N_TH_STEP_4 + 1):
        th[i-1].start()
    # Il main aspetta la fine di tutti i thread per proseguire la sua exec
    for i in range(1, N_TH_STEP_4 + 1):
        th[i-1].join()
    time_tot_s = 0
    t_tmp = 0
    list_media_t = [None]*len(times_s4[0])
    indice = 0
    for j in range(0,len(times_s4[indice])):    # si scorrono le colonne
        for i in range(0,len(times_s4)):        # si scorrono le righe
            t_tmp = t_tmp + times_s4[i][j]      # si sommano gli elementi della stessa colonna
        list_media_t[indice] = t_tmp / len(times_s4)          # si salva il valore della media degli elemeti della stessa colonna in una lista
        t_tmp = 0
        indice += 1
    for i in range(0,len(list_media_t)):
        time_tot_s = time_tot_s + list_media_t[i]
    for i in range (1,N_TH_STEP_4 + 1):
        print "\n"
        print "((( THREAD ",i,")))"
        print "Numero di deadlock generati: ",dl_count_step4[i-1]
        print "Time [getTransStatus, getTaskStatus]: ",times_s4[i-1]," secondi"
    print "\n"
    print "Time medio [getTransStatus, getTaskStatus]: ",list_media_t," secondi"
    print "\n"
    print "-------------------------------------------------------------"
    print "Impiegati", time_tot_s, "secondi per eseguire lo step 4"
    print "-------------------------------------------------------------"
    time_tot = time_tot + time_tot_s
    print "\n"



    ext = 0
    while (ext != 1):
        r = -1
        r = input("SI VOGLIONO CANCELLARE LE TRANSFORMATION, FILE, TASK APPENA CREATI? [SI->1, NO->0] >> ")
        print "\n"
        if (r == 1):
            print "\033[1;34m    Elliminazione Transformation\033[1;m"
            print "\033[1;34m------------------------------------\033[1;m"
            th = []
            # Instanziazione dei thread
            for i in range(1, N_TH_STEP_5 + 1):
                t = stepFive(i, transID[i-1])
                th.append(t)
            # Si fanno partire i thread
            for i in range(1, N_TH_STEP_5 + 1):
                th[i-1].start()
            # Il main aspetta la fine di tutti i thread per proseguire la sua exec
            for i in range(1, N_TH_STEP_5 + 1):
                th[i-1].join()
            ext = 1
        else:
            if (r != 0):
                print "\033[1;34mScelta non valida.. Prego, inserire nuovamente l'opzione\033[1;m"
            else:
                ext = 1
            
    print "\n" 
    
    print "\033[1;32m-------------------------------------------------------------\033[1;m"
    print "\033[1;32mImpiegati " + str(time_tot) + " secondi per eseguire il job\033[1;m"
    print "\033[1;32m-------------------------------------------------------------\033[1;m"
