from DIRAC.TransformationSystem.Client.TransformationClient   import TransformationClient

import random
import time

class testJob:
    def stepOne( self ):
        #
        # Si aggiunge una Trasformazione
        #
        res = transClient.addTransformation( 'transName', 'description', 'longDescription', 'MCSimulation', 'Standard','Manual', '' )
        if res['OK']==True:
            print "**** Trasformazione creata ****"
        else:
            print "#### ERROR: Trasformazione non creata ####"
            print res['Message']
        transID = res['Value']
    
        #
        # Si aggiungono rand File alla Trasformazione
        #
        n_file = random.randint(1000,2000)
        print "Sto creando ",n_file," File .... "
        lfns=[]
        # Si assembla il nome del file per generarli con nomi diversi
        for n in range( n_file ):
            st = str( n )
            lfns.append( "/aa/lfn." + st + ".txt" )
        res = transClient.addFilesToTransformation( transID, lfns )
        if res['OK']==True:
            print "**** Creati ",n_file," File  ****"
        else:
            print "#### ERROR: File non creati ####"
            print res['Message']

        #
        # Si aggiunge un Task ogni rand(1,100) file
        #
        print "Sto creando i Task .... "
        min = 0
        max = random.randint(1,100)
        taskID = []
        n_task = 0
        while( max <= n_file ):
            lfns_tmp = lfns[min:max]
            res = transClient.addTaskForTransformation( transID, lfns_tmp )
            if res['OK']==False:
                print "#### ERROR: Task non creato ####"
                print res['Message']
            else:
                n_task = n_task+1
            taskID.append( res['Value'] )
            rand = random.randint(0,30)
            min = max
            max = max + rand
        print "**** Creati ",n_task," Tasks ****"
    
        return (transID, lfns, taskID, n_task, min, n_file)



    def stepTwo( self, transID, taskID, lfns, n_task ):
        #
        # Si setta lo stato dei primi rand(1,100) Task a 'Status_Modified'
        #
        rand = random.randint(1,100)
        # Si controlla che il numero random sia minore del numero dei Task, altrimenti se ne genera uno diverso
        while( rand > n_task ):
            rand = random.randint(1,100)
        status = "Status_Modified"
        taskID_tmp = taskID[0:rand] 
        res = transClient.setTaskStatus(transID, taskID_tmp, status)
        if res['OK']==True:
            print "**** Stato di ",rand," Task settato a '",status,"' ****"
        else:
            print "#### ERROR: Stato del Task non settato ####"
            print res['Message']

        #
        # Si setta lo stato dei primi rand(1,20) File a 'Status_Modified'
        #
        rand = random.randint(1,20)
        newLFNsStatus = "Status_Modified"
        lfns_tmp = lfns[0:rand]
        res = transClient.setFileStatusForTransformation( transID, newLFNsStatus, lfns_tmp )
        if res['OK']==True:
            print "**** Stato dei primi ",rand," File settato a '",newLFNsStatus,"' ****"
        else:
            print "#### ERROR: Stato dei File non modificati ####"
            print res['Message']


    
    def stepThree( self, transID, lfns, min, n_file ):
        #
        # get dello stato della Trasformazione e del Task
        #
        res = transClient.getTransformationStats(transID)
        if res['OK']==True:
            statusTransf = res['Value']
        else:
            print "#### ERROR: Stato della Trasformazione impossibile da recuperare ####"
            print res['Message']

        res = transClient.getTransformationTaskStats(transID)
        if res['OK']==True:
            statusTask = res['Value']
        else:
            print "#### ERROR: Stato del Task impossibile da recuperare ####"
            print res['Message']
        print "Stato Trasformazione: ",statusTransf
        print "Stato Task: ",statusTask

        #
        # Si cambia ancora lo stato di un File e si aggiunge una Task
        #
        newLFNsStatus = "Second_Modified"
        lfns_tmp = lfns[0:1]
        res = transClient.setFileStatusForTransformation( transID, newLFNsStatus, lfns_tmp )
        if res['OK']==True:
            print "**** Stato di un File settato a '",newLFNsStatus,"' ****"
        else:
            print "#### ERROR: Stato del File non modificato per la seconda volta ####"
            print res['Message']
        min = min + 1
        max = min + 1
        if (max <= n_file):
            lfns_tmp = lfns[min:max]
            res = transClient.addTaskForTransformation( transID, lfns_tmp )
        else:
            # Si crea un File nuovo per creare un Task
            st = str( max )
            lfns_tmp = ["/aa/lfn." + st + ".txt"]
            res = transClient.addFilesToTransformation( transID, lfns_tmp )
            if res['OK']==True:
                print "**** Creato un nuovo File perche' non erano disponibili file per creare un nuovo Task ****"
            else:
                print "#### ERROR: File per aggiungere il Task non creato ####"
                print res['Message']
            res = transClient.addTaskForTransformation( transID, lfns_tmp )
        if res['OK']==True:
            print "**** Aggiunto un Task ****"
        else:
            print "#### ERROR: Task non aggiunto ####"
            print res['Message']

    def stepFour( self, transID ):
        res = transClient.cleanTransformation( transID )
        if res['OK']==True:
            print "**** Clean Transformation ****"
        else:
            print "#### ERROR: Clean Transformation ####"
            print res['Message']



    def stepFive( self, transID ):
        #
        # Si elimina una trasformazione
        #
        res = transClient.deleteTransformation( transID )
        if res['OK']==True:
            print "**** Trasformazione eliminata ****"
        else:
            print "#### ERROR: Trasformazione non eliminata ####"
            print res['Message']



####MAIN####
if __name__ == "__main__":
    transClient = TransformationClient()
    
    test = testJob()

    print "\n"
    print " |========================================|  "
    print " |    TEST JOB   -   TRANSFORMATION DB    | "
    print " |========================================| "
    print "\n"

    print "STEP 1"
    tempo_iniziale = time.time()
    [transID, lfns, taskID, n_task, min, n_file] = test.stepOne( )
    print "TransID: ",transID
    print "TaskID: ", taskID
    print "\n"

    print "STEP 2"
    test.stepTwo( transID, taskID, lfns, n_task )
    print "\n"

    print "STEP 3"
    test.stepThree( transID, lfns, min, n_file )
    print "\n"

    print "STEP 4"
    test.stepFour( transID )
    tempo_finale = time.time()
    print "\n"
    
    test.stepFive( transID )
    
    print "\n"
    print "-------------------------------------------------------------"
    print "Impiegati", str(tempo_finale - tempo_iniziale), "secondi per eseguire il job"
    print "-------------------------------------------------------------"
