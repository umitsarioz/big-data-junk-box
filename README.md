# 🧰 Big Data Module Box 
I created a big data module box. It consists of some modules about datasource operations, pyspark operations, io operations etc.I will try to update and add new codes in time.
Check readme.md and codes for details.

> <b> In this module all submodules need to logger object. You can create logger object easily using with utils.logger.LogHelper submodule.</b>

## Databases

You can use Cassandra, Mongo and PostgreSQL modules in db folder.


<table>
    <thead>
        <tr>
            <th> Database</th>
            <th> Module Filepath</th>
            <th> Object Name</th>
            <th> Functions</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td rowspan=12>Cassandra</td>
            <td rowspan=12>db.cassandra.py</td>
            <td rowspan=12>CassandraHelper</td>
            <td>Set Connection Parameters</td>
        </tr>
        <tr>
            <td>Get Connection Parameters</td>
        </tr>
        <tr>
            <td>Connect Cassandra</td>
        </tr>
        <tr>
            <td>Disconnect Cassandra</td>
        </tr>
        <tr>
            <td>Execute query</td>
        </tr>
        <tr>
            <td>Select data from table</td>
        </tr>
        <tr>
            <td>Delete a table</td>
        </tr>
        <tr>
            <td>Truncate a table</td>
        </tr>
        <tr>
            <td>Write data to table by PySpark</td>
        </tr>
        <tr>
            <td>Read data from table by PySpark</td>
        </tr>
        <tr>
            <td>Check rows for given datetime</td>
        </tr>
 <tr>
            <td>Set SparkSession</td>
        </tr>
    <tr>    <td class="bg" colspan=4>
    </td></tr>
    <tr>
            <td rowspan=18>Mongo</td>
            <td rowspan=18>db.mongo.py</td>
            <td rowspan=18>MongoHelper</td>
            <td>Set Connection Parameters</td>
        </tr>
        <tr>
            <td>Get Connection Parameters</td>
        </tr>
        <tr>
            <td>Connect Mongo</td>
        </tr>
        <tr>
            <td>Disconnect Mongo</td>
        </tr>
        <tr>
            <td>Insert one record</td>
        </tr>
        <tr>
            <td>Insert many records</td>
        </tr>
        <tr>
            <td>Find one record</td>
        </tr>
        <tr>
            <td>Find many records</td>
        </tr>
        <tr>
            <td>Get count of records</td>
        </tr>
        <tr>
            <td>Check a record exist</td>
        </tr>
        <tr>
            <td>Delete one record</td>
        </tr>
        <tr>
            <td>Delete many records</td>
        </tr>
        <tr>
            <td>Delete one record</td>
        </tr>
        <tr>
            <td>Update one record</td>
        </tr>
        <tr>
            <td>Update many records</td>
        </tr>
        <tr>
            <td>Drop table</td>
        </tr>
        <tr>
            <td>Create table</td>
        </tr>
        <tr>
            <td>Truncate table</td>
        </tr>
    <tr>   
    <td class="bg" colspan=4>
    </td>
    </tr>
    <tr>
            <td rowspan=16>PostgreSQL</td>
            <td rowspan=16>db.postgres.py</td>
            <td rowspan=16>PostgresHelper</td>
            <td>Set Connection Parameters</td>
        </tr>
        <tr>
            <td>Get Connection Parameters</td>
        </tr>
         <tr>
            <td>Connect PostgreSQL</td>
        </tr>
     <tr>
            <td>Disconnect PostgreSQL</td>
        </tr>
 <tr>
            <td>Commit</td>
        </tr>
 <tr>
            <td>Select data from table</td>
        </tr>
 <tr>
            <td>Select distinct values</td>
        </tr>
 <tr>
            <td>Insert a row</td>
        </tr>
 <tr>
            <td>Insert rows</td>
        </tr>
 <tr>
            <td>Remove a row</td>
        </tr>
 <tr>
            <td>Drop table</td>
        </tr>
 <tr>
            <td>Update a row</td>
        </tr>
 <tr>
            <td>Update rows</td>
        </tr>
 <tr>
            <td>Truncate table</td>
        </tr>
 <tr>
            <td>Read data by PySpark</td>
        </tr>
 <tr>
            <td>Set SparkSession</td>
        </tr>
    </tbody>
</table>

## IO : HDFS and SFTP 

<table>
  <thead>
        <tr>
            <th> Datasource</th>
            <th> Module Filepath</th>
            <th> Object Name</th>
            <th> Functions</th>
        </tr>
    </thead>
    <tbody>
        <tr>
            <td rowspan=15>HDFS</td>
            <td rowspan=15>io.hdfs.py</td>
            <td rowspan=15>HDFSHelper</td>
            <td>Set SparkSession Object</td>
        </tr>
        <tr>
            <td>Set Connection Parameters for HDFS Client</td>
        </tr>
       <tr>
            <td>Set Connection Parameters for PySpark HDFS Gateway URI</td>
        </tr>
        <tr>
            <td>Check file exist</td>
        </tr>
        <tr>
            <td>Delete file</td>
        </tr>
        <tr>
            <td>Get all file details in a directory</td>
        </tr>
        <tr>
            <td>Get all filenames in a directory</td>
        </tr>
        <tr>
            <td>Read file using PySpark</td>
        </tr>
       <tr>
            <td>Write file using PySpark</td>
        </tr>
       <tr>
            <td>Delete old files for given datetime range</td>
        </tr>
       <tr>
            <td>Create a Directory using HDFSClient</td>
        </tr>
       <tr>
            <td>Upload file using HDFSClient</td>
        </tr>
               <tr>
            <td>Download file using HDFSClient</td>
        </tr>
                <tr>
            <td>Read file using HDFSClient</td>
        </tr>
       <tr>
            <td>Write file using HDFSClient</td>
        </tr>
<tr><td colspan="4"></td> </tr>
        <tr>
            <td rowspan=15>SFTP</td>
            <td rowspan=15>io.sftp.py</td>
            <td rowspan=15>SFTPHelper</td>
            <td>Set Connection Parameters</td>
        </tr>
        <tr>
            <td>Get Connection Parameters</td>
        </tr>
       <tr>
            <td>Get Transport Object</td>
        </tr>
       <tr>
            <td>Get SFTP Object</td>
        </tr>
       <tr>
            <td>Open SFTP Connection</td>
        </tr>
       <tr>
            <td>Close SFTP Connection</td>
        </tr>
       <tr>
            <td>Connect SFTP</td>
        </tr>
       <tr>
            <td>Disconnect SFTP</td>
        </tr>
       <tr>
            <td>Get files details for given filepath</td>
        </tr>
       <tr>
            <td>Read file </td>
        </tr>

</table>

## Utils

In this directory, there are some useful modules for connect spark, logging, decorators, mail operations etc. 

<table>
    <thead>
        <tr>
            <th> Filesource</th>
            <th> Module Filepath</th>
            <th> Object Name</th>
            <th> Functions</th>
        </tr>
    </thead>
    <tbody>
        <tr><td colspan="4"></td> </tr>
        <tr>
            <td rowspan=4>Log Operations</td>
            <td rowspan=4>utils.logger.py</td>
            <td rowspan=4>LogHelper</td>
            <td>Create logger object</td>
        </tr>
        <tr>
            <td>Change log level</td>
        </tr>
        <tr>
            <td>Get log level</td>
        </tr>
        <tr>
            <td>Get logger object</td>
        </tr>
    <tr><td colspan="4"></td> </tr>
            <tr>
                <td rowspan=7>Spark</td>
                <td rowspan=7>utils.spark.py</td>
                <td rowspan=7>SparkHelper</td>
                <td>Set Connection Parameters</td>
            </tr>
            <tr>
                <td>Get Connection Parameters</td>
            </tr>
            <tr>
                <td>Start Spark Session</td>
            </tr>
            <tr>
                <td>Stop Spark Session</td>
            </tr>
           <tr>
                <td>Clear session cache</td>
            </tr>
           <tr>
                <td>Read file</td>
            </tr>
           <tr>
                <td>Write file</td>
            </tr>
    <tr><td colspan="4"></td> </tr>
            <tr>
                <td rowspan=3>Mail Operations</td>
                <td rowspan=3>utils.mailops.py</td>
                <td rowspan=3>MailHelper</td>
                <td>Set Sending Parameters</td>
            </tr>
            <tr>
                <td>Get Sending Parameters</td>
            </tr>
            <tr>
                <td>Send Email</td>
            </tr>
    <tr>
        <td colspan="4"></td> </tr>
        <tr>
                <td rowspan=4>Git</td>
                <td rowspan=4>utils.git.py</td>
                <td rowspan=4>GitHelper</td>
                <td>Add files</td>
            </tr>
            <tr>
                <td>Add commit</td>
            </tr>
            <tr>
                <td>Push Files</td>
            </tr>
            <tr>
                <td>Find file in git commit history</td>
            </tr>
    <tr><td colspan="4"></td> </tr>
        <tr>
                <td rowspan=2>Decorators</td>
                <td rowspan=2>utils.decorators.py</td>
                <td rowspan=2>Decorators</td>
                <td>Timer</td>
            </tr>
            <tr>
                <td>Retry</td>
            </tr>
    <tr><td colspan="4"></td> </tr>
</tbody>
</table>

