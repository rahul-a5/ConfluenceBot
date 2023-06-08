from transformers import pipeline
nlp = pipeline("question-answering")

# context = "Once upon a time, in a quaint little village nestled deep in the forest, there lived a young girl named Lily. Lily was known for her kind heart and unwavering curiosity. One day, while exploring the woods, she stumbled upon a hidden trail. Intrigued, Lily followed the path until she reached a clearing where a magnificent tree stood tall. Its branches were adorned with sparkling lights, and underneath it, a wondrous sight awaited her. A group of woodland creatures, led by a wise old owl, had gathered for a secret celebration. They invited Lily to join their festivities and share her stories of the outside world. From that day forward, Lily's bond with nature grew stronger, and she cherished the memories of her enchanted encounters."
# question = "Who invited Lily to join the secret celebration in the forest clearing?"
# question = "Who was the leader of woodland creatures?"
context="""Introduction
Here is a list of features that team Seal has worked on:

Reverse Sync
WAPMSTR7
OMS, Plutus and RAP jobs
I0481 FTP
I0089 date standardization
Reverse Sync
Reverse Sync is the macro process that allows reprocessing data that has had an issue in the normal data flow during the execution of batch format on different stages in CILL, this data with errors would be pushed by the UI Reprocessing tool that has been designed to business users can fix this errors updating, inserting or deleting data.



For additional detail of architecture in high level refers to: https://collaboration.wal-mart.com/pages/viewpage.action?spaceKey=FINTECHMIG&title=99.8+Reverse+Sync

Reverse Sync Jobs
Reverse Sync process is composed by five Jobs developed in Scala and Spark, this jobs allow reprocess and interact the data between UI + MySQL + GCS.

CillErrorCancellation
CillErrorWithTransform
SAPAckManualUpdate
SAPAckWithOutTranfRevSync
SAPAckWithTransfErroredIdoc


The code of this jobs are in the repo cill-reprocessing-ui-reverse-sync

All Jobs are extended from abstract class BaseJob and includes a override function called def executeJob where the logic of extract, transform and load is developed.

CillErrorCancellation Job
This job cancels all records that want to be applicable for reverse process to do so UI Summary table and CILL Error, also this process maintains the log updated and inserts new records on Cill Error Log.

Extract data from MySQL
This step extracts data from MySQL of CILL database from the next tables:

cill.UI_ERROR_HEADER
cill.JOURNAL_ERROR_DETAIL
cill.COGS_ERROR_DETAIL
This tables are joined by:

UI_ERROR_HEADER inner join JOURNAL_ERROR_DETAIL on errorHeader.header_id = journalError.header_id

union

UI_ERROR_HEADER inner join COGS_ERROR_DETAIL on errorHeader.header_id = cogsError.header_id



Filtering conditions

| errorHeader.submission_status in ('X', 'S')
| and errorHeader.error_status_cd = 'O'
| and errorHeader.sync_status_ind is NULL
| and errorHeader.error_type = 'CILL Error'
| and errorHeader.source_path LIKE '${basePath}/%'
Insert new status on audit cill error log
This step inserts the new records with their correspond status ('X' or 'S'), appending new status records for every partition in cill error log on GSC.

We have developed the object InsertNewStatusErrorLog with apply method, here this method execute the following steps:

Obtain different values from configuration and set cill error schema
Get current partition from current timestamp in different formats. (DAY_FORMAT, TIME_FORMAT_IN_MILLIS) This values will be used  to add new columns to updatedStatusErrors dataframe.
Append new status from error dataframe that has been read from different paths of ORC files in GCS, here, the new columns (errorStatusCd, partnKeyYrHr, createdTimestamp) are included to form updatedStatusErrors dataframe. 
errorStatusCd column will take the correspond value ('X' or 'S') that is sent as parameter of apply method.
Extract data from cill error log on GCS
This step forms the path where record were inserted in Cill Error Log on GCS, this path allows extract data from ORC files in errorsDf dataframe filtering by the correspond error_status_cd.

Identify records to be updated
This records are identified as result to join errorsDf  and UISummaryRegionDF, this dataframe contains data from MySQL query.

Update status on MySQL 
To update status on MySQL we use an object UpdateStatusErrorLog with apply method, here a connection to MySQL is established to map an collect the records that we need to update. Here a trait class JobHelpers is used with method getUpdatesStatusWithErrorQuery. This functions execute SQL DML to update cillUiErrorHeaderTable with their correspond error status ('X' or 'S').

MySQL Queries


CillErrorWithTransform Job
This job corrects and applies transformations to the source data base regarding to correction changes specified on the UI Tool.

The business rules are described in the bellow diagram



Data preparation
This stage is previous to execute the set of business rules.

Data preparation

To prepare data is necessary to extract data from MySQL CILL database, the tables in the main query are:

cill.UI_ERROR_HEADER
cill.AUDIT_LOG_HIST
cill.JOURNAL_ERROR_DETAIL
cill.COGS_ERROR_DETAIL
This tables are joined in the next way.

UI_ERROR_HEADER ueh inner join AUDIT_LOG_HIST alh on ueh.auto_id = alh.audit_log_id and ueh.error_type = 'CILL Error' and ueh.detail_table_ind = 'JE' and ueh.submission_status = 'C' and ueh.sync_status_ind is null

AUDIT_LOG_HIST alh left outer join JOURNAL_ERROR_DETAIL jed on alh.detail_level_auto_id = jed.journal_error_detail_auto_id

UI_ERROR_HEADER idoc left outer join UI_ERROR_HEADER ueh ueh.batch_id = idoc.batch_id and ueh.doc_id = idoc.doc_id and idoc.error_type = 'Errored IDoc' and idoc.submission_status = 'C' and idoc.sync_status_ind = 'C'

where: fnl.rnumber = 1 and error_log_id is not null and fnl.source_path like '$basePath/%'

union

UI_ERROR_HEADER ueh inner join AUDIT_LOG_HIST alh on alh on ueh.auto_id = alh.audit_log_id and ueh.error_type = 'CILL Error' and ueh.detail_table_ind = 'COGS' and ueh.submission_status = 'C' and ueh.sync_status_ind is null

AUDIT_LOG_HIST alh  left outer join COGS_ERROR_DETAIL ced on alh.detail_level_auto_id = ced.cogs_error_detail_auto_id

where: fnl.rnumber = 1 and error_log_id is not null and fnl.source_path like '$basePath/%').stripMargin



Then the query is read as spark dataframe filtering for the column activeIndicator === "Y"

Editable Fields

Also, we have to extract data from MySQL CILL Data Base from EDITABLE_FIELDS table. We need to use group_concat function to obtain all original names by interface_name, source_system_name, processing_stage_name. We need filter data where scope_indicator = 'CILL Error' AND active_indicator = 'Y'

This query must be read as spark dataframe.

Join Data

The conditions para join dataframes are stablished using two collections of Seq, They are based at:

Join condition 1: errorTypeId, processingStageName, sourceSystemName

Join condition 2: interfaceName, sourceSystemName, processingStageName

With the conditions described above the spark dataframes are joined to form cillErrorLog dataframe (correction dataset), here, columns are stablished to struct the main dataframe for then clean and apply the business rules to transform data.

Splitting data

The correction dataset is splitted using the GetDatasetBySplittingKey function and splitting key list, this is a list of objects composed of source path, error type, grouping column names, record id column name, event id.

Applying Business Rules
To apply business rules is necessary iterates collection splitted in value correctionDatasetBySplittingKeySeqMap into for comprehension loop, it is used to map dataset elements and apply every business rule.

for {
  (errorPath, correctionDatasetBySplittingKeySeq) <- correctionDatasetBySplittingKeySeqMap
  errorDataSet <- List(GetSourceDataset.getErrorDataSet(spark, errorPath))
  correctionDatasetBySplittingKey: (SplittingKey, Dataset[Row]) <- correctionDatasetBySplittingKeySeq
  sourceDataSet: (SplittingKey, (Dataset[Row], Dataset[Row])) <- GetSourceDataset(correctionDatasetBySplittingKey, spark, errorDataSet, config)
  correctedDataSet <- GetCorrectedDataset(sourceDataSet, spark, config)
  insertedDataSet <- InsertCorrectedData(correctedDataSet, spark, config, MYSQLReader)
} yield insertedDataSet
MySQL Queries


Unit Test Cases
The new reverse sync structure uses 5 different methods to process records. A different unit test class has been created for each method (listed below), and have data needed for each method saved in a directory titled cillErrorCancelationTestResources.

 

Name of unit test Class

Data used in test

PartitionReaderTest

MySQL dataframe

FilteringSourceDataTest

MySQL dataframe, source dataframe

ReprocessingRulesTest

MySQL dataframe, filteredSource dataframe

LoadReprocessingTest

MySQL dataframe, reprocessed dataframe

AuditEntryTest

MySQL dataframe, reprocessed dataframe

 For each unit test class, the data gets loaded into dataframe. Then we call the job, and the resulting dataframe counts should match the expected number. Also, columns added in the methods are tested to match the expected behavior.

The unit test cases scenarios have been developed in the next story.

FINTWO-34754 - Reverse Sync - Implement unit test on restructure CILLErrorWithTransf Job DONE
SAPAckManualUpdate Job
SAPAckManualUpdate Job is defined as the entry point of manual update for CILL SAP acknowledgement  where all business rules are applied by the calling and execution of functions in this job.


Extract records from SAP Acknowledgement and Audit log Hist
To extract records from mySQL database SAPAckCreateDataframe singleton object is created to connect and apply query, the data is read from CILL.UI_ERROR_HEADER with the next filtering conditions:

where:
ueh.ERROR_TYPE = 'Errored IDoc'
and ueh.SUBMISSION_STATUS = 'C'
and ueh.SYNC_STATUS_IND is null
and ueh.MARK_AS_COMPLETE_IND = 1
and ueh.SAP_REPROCESS_IND is null

Then the query is read as spark dataframe using the MYSQLReader function.

Update cillSAPAckAudit
To update and write the results on cillSAPAckAudit on GCS is necessary transform data to map data for each field, this action is performed by SAPAckDataFieldMapping, then ORC file is written in specific path in GCS by Region specifying a sequence of version, source name and partition key.

Update Sync Status CD
The records should be sent as spark dataframe using UpdateStatusSAPAck method, where the dataset with their correspond syncStatusInd is updated using MySQLUtility.updateStatement.

MySQL Queries


Unit Test Cases
Five different scenarios have been created to apply test unit cases and these are described below.

SAPackManualUpdateMySQLUnitTest	This unit test is a generic case, here, the functionality of Manual Update Job is tested in general way. It reads MySQL dataframe and data source from GCS, to filter the source dataset and transform it. In manual update job MySQL dataset is transformed for then to be loaded into GSC.
PartitionReaderTest	This is an individual unit test where only the reading of source dataset is tested, this will be read from its correspond path and partition.
FilteringSourceDataTest	This is an individual unit test where only the filtering of source dataset is tested, this happens once the source dataset has been red.
ReprocessingRulesTest	This case tests the transformation on source dataset that leave ready to be load on GCS.
LoadReprocessingRulesTest	This unit case tests the writing of the data transformed to be load on GCS, also it tests the status that should be updated according the indicators to set the record as reprocessed.


The unit test cases scenarios have been developed in the next story.

FINTWO-34757 - Reverse Sync - Implement Restructure unit test on Sap Manual Update Job DONE
SAPAckWithOutTransfRevSync Job
This job allows to extract data from UI Error Header Table and insert this data to SAPACK Resubmit for then update status into MySQL


 draw.io

Diagram attachment access error: cannot display diagram
Extract data from UI MySQL
This step allows to extract data from CILL database, UI_ERROR_HEADER table whit the next filtering conditions:

where:



UIERR.ERROR_TYPE = 'Errored IDoc'
and UIERR.SUBMISSION_STATUS = 'C'
and UIERR.SYNC_STATUS_IND is null
and UIERR.RETRAN_IND = 'N'
and trim(both \'/\' from UIERR.sapgateway_source_path) LIKE '$basePath/%'.stripMargin

The query is read as spark dataframe using MYSQLReader function and validate if there are available records to start processing.

Insert Resubmit IDOCS

To process the records is necessary to apply GetDatasetByPath method to get datasets for each path to be filter, this datasets will be split by journal entry path for then obtain a result map object with source path and correspond dataset.

Once the datasets with correspond path have been obtained InsertResubmitIDocs apply method is applied, here data is read from correspond source path. Two main dataframes are processed, first one is jsonDF is read from sap gateway source path and second one is mysqlDS, this is sent as parameter of apply method. These dataframes are joined into temp_df by "batch_id" and "doc_id" for then form the resubmitDF.

Several transformations are performed before starting process and write operations to source path and audit tables, here two steps are done: Write to SAPACK Resubmit and Write to audit tables.



MySQL Queries


SAPAckWithTransfErroredIdoc Job
This job allows to extract data from UI Error Header Table and Audit Log History Table where business users make corrections to cill errored data for then modify journal entry data an finally insert this correct data into MySQL UI Summary table and Cill error log on GCS.



Extract data from MySQL
To extract data from mySQL is necessary apply query to the main tables: UI_ERROR_HEADER and AUDIT_LOG_HIST. This tables are joined by UI_ERROR_HEADER.auto_id = AUDIT_LOG_HIST.audit_log_id.

Filtering Conditions:



WHERE
HIST.audit_type_cd = '${config.getString("auditTypeCdSap")}'
AND UET.sync_status_ind IS NULL
AND HIST.audit_status_cd = 'C'
AND UET.retran_ind = 'Y'
AND HIST.seq_nbr = (SELECT MAX(HIST1.seq_nbr) FROM ${config.getString("cillAuditLogHistTable")} HIST1
WHERE HIST.audit_log_id = HIST1.audit_log_id
AND HIST.audit_type_cd = HIST1.audit_type_cd
AND HIST.acct_doc_line_item_nbr = HIST1.acct_doc_line_item_nbr)
AND trim(both \'/\' from UET.source_path) LIKE '$basePath/%'.stripMargin

Steps to transform data:

for {
  jeSourcePath <- jeSourcePathList
  datasetByPath <- GetDatasetByPath(jeSourcePath, docReprocessDF, "je_source_path", config)
  journalEntryDataset <- GetJournalEntryDataset(datasetByPath._1, datasetByPath._2, spark, config)
  modifiedJournalEntryDataset <- ModifyJournalEntryDataset(journalEntryDataset._1, journalEntryDataset._2._1, journalEntryDataset._2._2, spark, config)
  insertedCorrectedDataset <- InsertCorrectedDataset(modifiedJournalEntryDataset._1, modifiedJournalEntryDataset._2._1, modifiedJournalEntryDataset._2._2, modifiedJournalEntryDataset._2._3, spark, config)
} yield insertedCorrectedDataset
Get Dataset by Path

To obtain dataset that will be processed we have to use GetDatasetByPath apply method calling this into for comprehension loop (view code block above). We need send as parameters:

source Path to be filter
dataset Dataset with complete information
columnPath Column to be used as predicate filter
config Config file of job

The result of this method is a map object with source path and correspond dataset.

Modify Journal Entry

This step performs modifications on journal entry dataset for path of source. We need to send the next parameters to the apply method.

dataset Where it will take the type for each column in the columns names array
columnNames Array of column names that will be added in the StructType

The result of this method is a StructType with all the columns included in the columnNames array and with the types matching the dataset column types.

Furthermore, Editable Fields table for MySQL is read as spark dataframe to map payload with the fields that will be updated. Once payload structure has been formed it is used to star with modification of the fields in the correspond levels and layers. 

Insert Correct Dataset
This step corrects and inserts records in journal entry or in their correspond previous layer. We need to send the next parameters to the apply method.

source: Journal Entry path
mysqlDataset: MySQL Dataset
JEDataset: Original Journal Entry Data at header level
lineLevelCorrectedDs: Corrected Journal Entry Data at line level
spark: Spark session
config: Config file

The result of this method is a map object with source path as key and value with dataset that has been inserted in MySQL.

MySQL Queries




CI/CD Process
For more detail follow the link CI/CD Process"""

question1= "How to Extract records from SAP Acknowledgement, give me the exact query?"

def get_answer(question):
    
    result = nlp(question=question, context=context)
    return result["answer"]

