/**********************************************************************************************
 Name: BatchDaemon.cls
===============================================================================================
Purpose:  Abstract class to standardize batch daemon jobs going forward

Version:  1.2

Test class:  BatchDaemonTest.cls

Notes:
    - See BatchDaemonExample.cls for an example of how to implement a BatchDaemon class.
    - The processRecords() method is currently the only thing that MUST be implemented,
      but setRunForever() really should be called as well. Preferably from inside a
      constructor that gives you the option of setting it on or off.
      Otherwise the daemon will default to running once and you essentially just have a batch job.
    - The queueForProcessing() static method can be used in triggers and other processes
      to queue up records that your daemon will process.
    - getScheduledName() can be overridden if you wish to control what name your daemon is
      scheduled under (default is your class name).
    - Daemon can automatically attempt to reduce scope size when it encounters governor limit
      errors and restore scope size to normal after records are processed successfully.
      However, LimitExceptions cannot be caught directly. Therefore, if you wish to take advantage
      of this functionality you'll need to anticipate limit errors and throw an exception
      that includes the text "GovernorLimitCheck". Check Salesforce docs on the Limits Class
      for information on anticipating a limit error and see BatchDaemonExample for an example.


Maintenance
===============================================================================================
VER  AUTHOR              DATE       DETAIL                              
---  ------------------  ---------  -----------------------------------------------------------
1.0  Jason Coumbe        20-APR-21  Initial Version
1.1  Pamela Murillo      14-JUL-21  Added filter to only update to Processed, BatchQueueRecords 
                                    last modified before BatchDaemon started
1.2  Jason Coumbe        15-SEP-21  Delay processing of BatchQueueRecords
***********************************************************************************************/
public abstract class BatchDaemon implements Database.Batchable<sObject>, Database.Stateful{
    // Custom exception for this class
    private class BatchDaemonException extends Exception {}

    // Instance variables
    @TestVisible private Boolean hadLimitFailure    = false;
    @TestVisible private Boolean runForever         = false;
    @TestVisible private Integer delayBeforeNextRun = 5;
    @TestVisible private Integer scopeSize          = 200;
    @TestVisible private Integer origScopeSize;
    
    
    // Must be implemented - Code that does the actual daemon's work
    public abstract void processRecords(Set<Id> recordIds);


    // Setters
    public void setRunForever(Boolean runForever) {
        if(runForever == null)
            throw new BatchDaemonException('runForever cannot be set to null');

        this.runForever = runForever;
    }
    public void setDelayBeforeNextRun(Integer delayBeforeNextRun) {
        if(delayBeforeNextRun == null)
            throw new BatchDaemonException('delayBeforeNextRun cannot be set to null');
        
        this.delayBeforeNextRun = delayBeforeNextRun;
        if(delayBeforeNextRun < 1) {
            this.delayBeforeNextRun = 1;
        }
    }
    public void setScopeSize(Integer scopeSize) {
        if(scopeSize == null)
            throw new BatchDaemonException('Scope size cannot be null');
        
        // Set scopeSize for current instance
        this.scopeSize = scopeSize;
        if(scopeSize < 1) {
            this.scopeSize = 1;
        }
    }
    private void setOrigScopeSize(Integer origScopeSize) {
        this.origScopeSize = origScopeSize;
    }

    // Getters
    public Boolean getRunForever()         { return this.runForever; }
    public Integer getDelayBeforeNextRun() { return this.delayBeforeNextRun; }
    public Integer getOrigScopeSize()      { return this.origScopeSize; }
    public Integer getScopeSize()          { return this.scopeSize; }


    // Returns the name of the bottom most class (in other words the child of BatchDaemon)
    @TestVisible
    private String getClassName() {
        return String.valueOf(this).substring(0,String.valueOf(this).indexOf(':'));
    }

    // Can be overridden - Returns the name of the daemon when scheduled (default is classname)
    public virtual String getScheduledName() {
        return getClassName();
    }

    // Start method of batch class
    public Database.QueryLocator start(Database.BatchableContext bc) {
        System.debug('***** Startup of ' + getClassName());
        
        // If original scope size was never set then set it to current scope size
        if(this.origScopeSize == null) this.origScopeSize = this.scopeSize;
        
        System.debug('***** runForever: ' + getRunForever());
        System.debug('***** delayBeforeNextRun: ' + getDelayBeforeNextRun());
        System.debug('***** origScopSize: ' + getOrigScopeSize());
        System.debug('***** scopeSize: ' + getScopeSize());
        

        // Delay before BatchQueueRecords are processed
        Integer processingDelay = -2;

        // Set delay of processing to zero for test classes
        if( Test.isRunningTest() ) {
            processingDelay = 0;
        }

        // Get all the BatchQueueRecord__c records we'll be working on
        return Database.getQueryLocator([
            SELECT Id, Record_Id__c, Processed__c
              FROM BatchQueueRecord__c
             WHERE Batch_Job__c = :getClassName()
               AND Processed__c = false
               AND LastModifiedDate <= :Datetime.now().addMinutes(processingDelay)
        ]);
    }

    // Excute method of batch class
    public void execute(Database.BatchableContext bc, List<BatchQueueRecord__c> recs) {
        Datetime startTime = Datetime.now();
        
        // Get record id's
        Set<Id> recIds = new Set<Id>();
        for( BatchQueueRecord__c r : recs ) {
            recIds.add(r.Record_Id__c);
        }
        
        try {
            // Test exceptions
            ExceptionTesting.test('Record Processing Exception Test');

            // Do the real work
            processRecords(recIds);
        }
        catch(Exception e) {
            if( e.getMessage().contains('GovernorLimitCheck') ) {
                hadLimitFailure = true;
            }
            // We only catch errors so we can drop scope size after limit errors, so rethrow error
            throw e;
        }
        
        // Check if we're about to violate a limit
        if( (Limits.getDmlRows() + recs.size()) > Limits.getLimitDmlRows() ) {
            hadLimitFailure = true;
            throw new BatchDaemonException('GovernorLimitCheck DmlRows limit exceeded');
        }
        
        List<BatchQueueRecord__c> recsToUpdate = [SELECT Id, LastModifiedDate, Processed__c
                                                FROM BatchQueueRecord__c
                                                WHERE Id IN :recs
                                                AND LastModifiedDate <= :startTime];
        
        for (BatchQueueRecord__c rec : recsToUpdate){
            rec.Processed__c  = true;
        }
        
        // Set this batch of records to processed
        update recsToUpdate;
    }

    // Finish method of batch class
    public void finish(Database.BatchableContext bc) {
        System.debug('***** Finishing up ' + getClassName());
        
        // As long as we're under limits, delete old Batch queue records
        if (
            ( Limits.getQueries() < 95) &&
            ( Limits.getQueryRows() + 1000 < Limits.getLimitQueryRows() ) &&
            ( Limits.getDmlRows() + 1000 < Limits.getLimitDmlRows() ) )
        {
            // Delete old batch queue records (1000 at a time)
            Database.delete([SELECT Id
                               FROM BatchQueueRecord__c
                              WHERE Batch_Job__c = :getClassName()
                                AND Processed__c = true
                                AND LastModifiedDate < LAST_N_DAYS:30
                             LIMIT 1000]);
        }

        // If we're not running again just exit
        if(!runForever) return;

                
        // Halve the scope size if there was a dml failure to (hopefully) avoid it in the next run
        if( this.hadLimitFailure == true && this.scopeSize > 25) {
            this.scopeSize = this.scopeSize / 2;
            System.debug('There was a governor limit error. Reducing scope size to ' + this.scopeSize);
        }
        // If we had no dml failures raise scope size back to normal
        else if( this.hadLimitFailure == false ) {
            System.debug('No DML failures. Setting scope to original scope');
            this.scopeSize = this.origScopeSize;
        }


        // Name of job when we schedule it
        String jobName = getScheduledName();

        // For test classes rename job and do not run forever
        if(Test.isRunningTest()) {
            this.runForever = false;
            jobName += System.now();
        }

        // Create new instance of batch job
        BatchDaemon newJob = (BatchDaemon) Type.forName( getClassName() ).newInstance();
        newJob.setRunForever(this.runForever);
        newJob.setDelayBeforeNextRun(this.delayBeforeNextRun);
        newJob.setOrigScopeSize(this.origScopeSize);
        newJob.setScopeSize(this.scopeSize);

        // Schedule new instance
        System.scheduleBatch( newJob, jobName, this.delayBeforeNextRun, this.scopeSize );
    }


    // Static method that can be used to queue records for daemon processing
    public static void queueForProcessing( String jobName, Set<Id> recordIds ) {
        if(String.isBlank(jobName) || recordIds.size() < 1) return;

        List<BatchQueueRecord__c> recList = new List<BatchQueueRecord__c>();
        for( Id recId : recordIds ) {
            recList.add( new BatchQueueRecord__c(
                Batch_Job__c = jobName,
                Record_Id__c = recId,
                Unique_Id__c = jobName + '-' + recId,
                Processed__c = false
            ));
        }
        upsert recList Unique_Id__c;
    }
}