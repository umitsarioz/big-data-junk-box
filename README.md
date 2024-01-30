# big-data-junk-box
I created a big data junk box. It consists of some modules about datasource operations, pyspark operations, io operations etc. Check readme.md and codes for details.

> In this module all submodules need to logger object. You can create logger object easily using with utils.logger.LogHelper submodule.
# DataSource 

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
        <hr class="bg">
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
        <hr class="bg">
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


<style>
table, th, td {
  border: 1px solid black;
  border-collapse: collapse;
}

thead{
  background-color: rgba(100, 200, 5, 0.3);
}

tr {
  border-bottom: 1px solid #ddd;
}

.bg{
background-color: grey;
}


</style>