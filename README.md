<h3>FromMySqlToPostgreSql - the database migration tool.</h3>

<h3>WHAT IS IT ALL ABOUT?</h3>
<p>FromMySqlToPostgreSql is a tool, intended to make a process of migration 
from MySql to PostgreSql as easy and smooth as possible.</p>


<h3>KEY FEATURES </h3>
<ul>
<li> Ease of use - the only thing needed to run this script is the PHP(CLI) interpreter.</li>
   
<li> Accuracy of migration the database structure - FromMySqlToPostgreSql converts 
   MySql data types to corresponding PostgreSql data types, creates constraints,
   indeces, primary and foreign keys exactly as they were before migration.</li>

<li> In order to migrate data fast - FromMySqlToPostgreSql uses PostgreSQL COPY protocol.
   Note: migration of tables, containing "varbinary" or "blob" columns may be 
   considerably slower.</li>

<li> Ease of monitoring - FromMySqlToPostgreSql will provide detailed output
   about every step, it takes during the execution.</li>

<li> Ease of configuration - all the parameters required for migration 
   (maximum 7 parameters) should be put in one single file, 
   which can be in either "xml" or "json" format.</li>
</ul>

<h3>SYSTEM REQUIREMENTS</h3>
<ul>
<li> PHP (CLI) 5.4 or above.</li>
<li> PDO_MYSQL support.</li>
<li> PDO_PGSQL support.</li>
<li> register_argc_argv should be enabled (check php.ini).</li>
</ul>


<h3>USAGE</h3>
<ul>
<li> <p>Create a new database.</p>
   <p><b>Sample:</b> CREATE DATABASE my_postgresql_database;</p></li>

<li> <p>Download FromMySqlToPostgreSql package and put it on the machine running 
   your PostgreSql.</p>
  <p> <b>Sample:</b> /path/to/FromMySqlToPostgreSql</p></li>

<li> <p>Create configuration file in either "xml" or "json" format and put it on 
   the machine running your PostgreSql. </p>
   <p><b>Sample:</b> /path/to/config.json or /path/to/config.xml</p>
   <p>Remarks:</p>
   <ul>
   <li> sample_config.json or sample_config.xml are examples of configuration
      file, so you can edit one of them and use for migration.</li> 
      
   <li> Brief description of each configuration parameter will be found at 
      sample_config.json and sample_config.xml</li>
</ul>
</li>

<li> <p>Run the script from a terminal. </p>
   <p><b>Sample:</b>&nbsp;&nbsp; php &nbsp;&nbsp; /path/to/FromMySqlToPostgreSql/index.php &nbsp;&nbsp; /path/to/config[.xml | .json]</p></li>
       
<li> At the end of migration check log files (described above) if necessary.</li>

<li> In case of any remarks, misunderstandings or errors during migration, 
   please feel free to email me <anatolyuss@gmail.com>.</li>
</ul>

<h3>VERSION</h3>
<p>Current version is 1.0.0</p>


<h3>TEST</h3>
<p>Tested using MySql Community Server (5.6.21) and PostgreSql (9.3).</p>
<p>The entire process of migration 59.6 MB database (49 tables, 570750 rows), 
which includes data types mapping, creation of tables, constraints, indeces, 
PKs, FKs, migration of data, garbage-collection and analyzing the newly created 
PostgreSql database took 3 minutes 6 seconds.</p>


<h3>LICENSE</h3>
<p>FromMySqlToPostgreSql is available under "GNU GENERAL PUBLIC LICENSE" (v. 3)</p> 
<p><http://www.gnu.org/licenses/gpl.txt>.</p>


<h3>REMARKS</h3>
<p>Errors/Exceptions are not passed silently.</p>
<p>Any error will be immediately written into the error log file.</p>
