
m DIRAC.TransformationSystem.Client.TransformationClient   import TransformationClient

import random

class testJob:
    def stepOne( self ):
        #
        # Si aggiunge una trasformazione
        #
        res = transClient.addTransformation( 'transName', 'description', 'longDescription', 'MCSimulation', 'Standard','Manual', '' )
        if res['OK']==True:
            print "**** Trasformazione creata ****"
        else:
            print "#### ERROR: Trasformazione non creata ####"
        transID = res['Value']
    
        #
        # Si aggiungono rand(30,100) file alla trasformazione
        #
        n_file = random.randint(1000,2000)
        lfns=[]
        # Si assembla il nome del file per generarli con nomi diversi
        for n in range( n_file ):
            st = str( n )
            lfns.append( "/aa/lfn." + st + ".txt" )
        res = transClient.addFilesToTransformation( transID, lfns )
        #print res['Message']
        if res['OK']==True:
            print "**** Creati ",n_file," Files  ****"
        else:
            print "#### ERROR: File non creati ####"
            print res['Message']

        #
        # Si aggiunge un task ogni rand(1,30) file
        #
        min = 0
        max = random.randint(1,100)
        taskID = []
        n_task = 0
        while( max <= n_file ):
            lfns_tmp = lfns[min:max]
            res = transClient.addTaskForTransformation( transID, lfns_tmp )
            if res['OK']==False:
                print "#### ERROR: Task non creato ####"
            else:
                n_task = n_task+1
            taskID.append( res['Value'] )
            rand = random.randint(0,30)
            min = max
            max = max + rand
        print "**** Creati ",n_task," tasks ****"
    
        return (transID, lfns, taskID, n_task, min, n_file)



    def stepTwo( self, transID, taskID, lfns, n_task ):
        #
        # Si setta lo stato dei primi rand(1,10) task a 'Status_Modified'
        #
        rand = random.randint(1,100)
        # Si controlla che il numero random sia minore del numero dei task, altrimenti se ne genera uno diverso
        while( rand > n_task ):
            rand = random.randint(1,10)
        status = "Status_Modified"
        taskID_tmp = taskID[0:rand] 
        res = transClient.setTaskStatus(transID, taskID_tmp, status)
        if res['OK']==True:
            print "**** Stato di ",rand," task settato a '",status,"' ****"
        else:
            print "#### ERROR: Stato del task non settato ####"

        #
        # Si setta lo stato dei primi rand(1,20) file a 'Status_Modified'
        #
        rand = random.randint(1,20)
        newLFNsStatus = "Status_Modified"
        lfns_tmp = lfns[0:rand]
        res = transClient.setFileStatusForTransformation( transID, newLFNsStatus, lfns_tmp )
        if res['OK']==True:
            print "**** Stato dei primi ",rand," file settato a '",newLFNsStatus,"' ****"
        else:
            print "#### ERROR: Stato dei file non modificati ####"


    
    def stepThree( self, transID, lfns, min, n_file ):
        #
        # get dello stato della Trasformazione e del Task
        #
        res = transClient.getTransformationStats(transID)
        if res['OK']==True:
            statusTransf = res['Value']
        else:
            print "#### ERROR: Stato della trasformazione impossibile da recuperare ####"

        res = transClient.getTransformationTaskStats(transID)
        if res['OK']==True:
            statusTask = res['Value']
        else:
            print "#### ERROR: Stato del task impossibile da recuperare ####"
        print "Stato Trasformazione: ",statusTransf
        print "Stato Task: ",statusTask

        #
        # Si cambia ancora lo stato di un file e si aggiunge una task
        #
        newLFNsStatus = "Second_Modified"
        lfns_tmp = lfns[0:1]
        res = transClient.setFileStatusForTransformation( transID, newLFNsStatus, lfns_tmp )
        if res['OK']==True:
            print "**** Stato di un file settato a '",newLFNsStatus,"' ****"
        else:
            print "#### ERROR: Stato del file non modificato per la seconda volta ####"
        min = min + 1
        max = min + 1
        if (max <= n_file):
            lfns_tmp = lfns[min:max]
            res = transClient.addTaskForTransformation( transID, lfns_tmp )
        else:
            # Si crea un file nuovo per creare la task
            st = str( max )
            lfns_tmp = ["/aa/lfn." + st + ".txt"]
            res = transClient.addFilesToTransformation( transID, lfns_tmp )
            if res['OK']==True:
                print "**** Creato un nuovo file perche' non erano disponibili file per creare un nuovo task ****"
            else:
                print "#### ERROR: File per aggiungere il task non creato ####"
            res = transClient.addTaskForTransformation( transID, lfns_tmp )
        if res['OK']==True:
            print "**** Aggiunto un task ****"
        else:
            print "#### ERROR: Task non aggiunto ####"

    def stepFour( self, transID ):
        res = transClient.cleanTransformation( transID )
        if res['OK']==True:
            print "**** Clean Transformation ****"
        else:
            print "#### ERROR: Clean Transformation ####"



    def stepFive( self, transID ):
        #
        # Si elimina una trasformazione
        #
        res = transClient.deleteTransformation( transID )
        if res['OK']==True:
            print "**** Trasformazione eliminata ****"
        else:
            print "#### ERROR: Trasformazione non eliminata ####"



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
    print "\n"
    
    test.stepFive( transID )
