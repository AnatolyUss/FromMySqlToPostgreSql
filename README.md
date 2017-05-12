<h3>FromMySqlToPostgreSql - the database migration tool.</h3>

<h3>WHAT IS IT ALL ABOUT?</h3>
<p>FromMySqlToPostgreSql is a tool, intended to make a process of migration 
from MySql to PostgreSql as easy and smooth as possible.</p>


<p><b>Important update!</b><br />
Recently I've developed <a href="https://github.com/AnatolyUss/nmig">NMIG</a>, a database migration app, written in Node.js, and highly inspired by FromMySqlToPostgreSql.<br />
<a href="https://github.com/AnatolyUss/nmig">NMIG</a> has a few important functional enhancements.<br />
Due to Node.js asynchronous nature, <a href="https://github.com/AnatolyUss/nmig">NMIG</a> executes a part of its tasks in parallel, what makes it up to 3 times faster than FromMySqlToPostgreSql. Visit <a href="https://github.com/AnatolyUss/nmig">https://github.com/AnatolyUss/nmig</a>.<br />
However, if you're not into Node.js, you can still use old and proven FromMySqlToPostgreSql.
</p>


<h3>KEY FEATURES</h3>
<ul>
<li> Ease of use - the only thing needed to run this script is the PHP(CLI) interpreter.</li>
   
<li> Accuracy of migration the database structure - FromMySqlToPostgreSql converts 
   MySql data types to corresponding PostgreSql data types, creates constraints,
   indexes, primary and foreign keys exactly as they were before migration.</li>

<li>Ability to migrate big databases - in order to reduce RAM consumption <br/> 
FromMySqlToPostgreSql will split each table's data into several chunks. <br />
The chunk size can be adjusted easily via configuration file.</li>

<li> Speed of data transfer - in order to migrate data fast <br />
   FromMySqlToPostgreSql uses PostgreSQL COPY protocol.<br />
</li>

<li>Ease of monitoring - FromMySqlToPostgreSql will provide detailed output
   about every step, it takes during the execution.</li>
<li>
 Ease of configuration - all the parameters required for migration 
 should be put in one single file, 
 which can be in either "xml" or "json" format.</li>
<li>
   Ability to transfer only a data (in case of an existing database).
</li>
</ul>

<h3>SYSTEM REQUIREMENTS</h3>
<ul>
<li> <b>PHP (CLI) 5.4</b> or above </li>
<li> <b>PDO_MYSQL</b> should be installed and enabled </li>
<li> <b>PDO_PGSQL</b> should be installed and enabled </li>
<li> <b>mbstring</b> should be installed and enabled </li>
<li> <b>register_argc_argv</b> should be enabled (check php.ini).</li>
</ul>

<h3>USAGE</h3>
<p><b>1.</b> Create a new database.<br />&nbsp;&nbsp;&nbsp;
   <b>Sample:</b><code>CREATE DATABASE my_postgresql_database;</code></p>

<p><b>2.</b> Download FromMySqlToPostgreSql package and put it on the machine running 
   your PostgreSql.<br />
   &nbsp;&nbsp;&nbsp;&nbsp;<b>Sample:</b><code> /path/to/FromMySqlToPostgreSql</code></p>

<p><b>3.</b> Create configuration file in either "xml" or "json" format and put it on 
   the machine running your PostgreSql.<br /> 
   &nbsp;&nbsp;&nbsp;
   <b>Sample:</b><code> /path/to/FromMySqlToPostgreSql/config.json</code> &nbsp; or&nbsp; <code> /path/to/FromMySqlToPostgreSql/config.xml</code></p>
   <p><b>Remarks:</b></p>
   <ul>
   <li> sample_config.json and sample_config.xml are examples of configuration
      file, so you can edit one of them and use for migration.</li> 
      
   <li> Brief description of each configuration parameter will be found at 
      sample_config.json and sample_config.xml</li>
   <li>Make sure, that username, you use in your PostgreSQL connection string, defined as superuser (usually "postgres")<br> More info: <a href="http://www.postgresql.org/docs/current/static/app-createuser.html">http://www.postgresql.org/docs/current/static/app-createuser.html</a></li>
   </ul>
     
<p><b>4.</b> Run the script from a terminal.<br /> 
   &nbsp;&nbsp;&nbsp;&nbsp;<b>Sample:</b> <code>
   php /path/to/FromMySqlToPostgreSql/index.php /path/to/FromMySqlToPostgreSql/config[.xml | .json]</code></p>
       
<p><b>5.</b> At the end of migration check log files, if necessary.<br />&nbsp;&nbsp;&nbsp;
   Log files will be located in "logs_directory" folder in the root of the package.<br />&nbsp;&nbsp;&nbsp;
   <b>Note:</b> "logs_directory" will be created during script execution.</p>


<p><b>6.</b> In case of any remarks, misunderstandings or errors during migration,<br /> &nbsp;&nbsp;&nbsp;
   please feel free to email me 
   <a href="mailto:anatolyuss@gmail.com?subject=FromMySqlToPostgreSql">anatolyuss@gmail.com</a></p>

<h3>VERSION</h3>
<p>Current version is 1.4.2<br />
(major version . improvements . bug fixes)</p>

<h3>TESTING</h3>
<p>The tests/ folder contains SQL files that exercise the migration tool to ensure it can
process different types of input structures correctly.  To use these files you must
setup a mysql server with them. An example might be <code>mysql < tests/foreign_key.sql</code>.
You can then follow the use the USAGE section to create a migration script it runs without error.
If you wish to add further tests to verify issues that are fixed, just add another SQL file
and future developers will be able to confirm that changes don't cause any regressions.
</p>

<h3>PERFORMANCE RESULTS</h3>
<p>Tested using MariaDB 10 and PostgreSql (9.6).<br />
The entire process of migration 33 GB database (90 tables, approximately 72 million rows),<br />
which includes data types mapping, creation of tables, constraints, indexes, <br />
PKs, FKs, migration of data, garbage-collection and analyzing the newly created <br />
PostgreSql database took 54 minutes.</p>

<h3>LICENSE</h3>
<p>FromMySqlToPostgreSql is available under "GNU GENERAL PUBLIC LICENSE" (v. 3) <br />
<a href="http://www.gnu.org/licenses/gpl.txt">http://www.gnu.org/licenses/gpl.txt.</a></p>


<h3>REMARKS</h3>
<p>Errors/Exceptions are not passed silently.<br /> 
Any error will be immediately written into the error log file.</p>


<h3>ACKNOWLEDGEMENTS</h3>
<p>Big thanks to all contributors for their valuable remarks!</p> 
