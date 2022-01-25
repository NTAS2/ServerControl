#!/usr/bin/perl
#The aim of the script is to control the PC (Laptop) from the XPS ( Control)
#Date: 20 March 2012
#
# palmarc/NTAS2 
#
# Technically, this script forms a TCP server to be hosted in the XPS server that communicates with the TCP client hosted in the Linux Laptop;
# Functional use: 
# 1. Primarily used to listen to the TCP client polls and respond appropriately.
# 2. Query the table Linux_config from the database test and store the results to an array and send it to TCP Client to parse.
# 3. Log the experimental start and stop times. Ideally, must be able to log the exceptions. If not flag the case when experiment is not properly done. 

# SUCCESS
#Delete from TODO, update LOG, if runs == total runs, delete from general_todo
# FAILURE
#Delete from TODO , update LOG , insert into TODO, increment into general todo, send email
# Default, must send email to the experiment guy (vkk) log failure, increment total run count, 
# QUERY
# Query from todo_... return HOLD / PROCEED, 
# CONFIG 
#check time , if experimental time is greater than current time continue
#return TODO_list
# configuration files


use IO::Socket;
use IO::Socket::Timeout;
use Sys::Hostname;
use Getopt::Long;
use DBI;
use DBD::mysql;
use DBI qw(:sql_types);
use POSIX;
use DateTime;
use DateTime::Format::MySQL;
use Switch;
use Sys::Syslog qw(:DEFAULT setlogsock);
use MIME::Base64;
use Data::Dumper;
use Config::Simple;


my $cfg=new Config::Simple('serverControl.conf');
    

my $db = $cfg->param('Database');
my $host = $cfg->param('DBhost');
my $user = $cfg->param('DBuser');
my $password = $cfg->param('DBpassword');

my $capmarker_control = $cfg->param('Marker');

my $server_port = $cfg->param('Port'); #1579;  # expects a server port using options. Example(1579)
my $merge_port = $cfg->param('MergePort');#1589; 
my $MergeIP= $cfg->param('MergePort'); #"127.0.0.1";



my $general_todo = 'general_Todo'; #option #5
my $todo_table_name = 'todo_ubuntu-ProBook'; #Option#4 obtain the list to do.
my $log_table_name = 'log_ubuntu-ProBook'; #Option 5  used to insert logs
my $platform = 'LINUXPCSENDER'; #Option6 name of platform used in the experiments

#my $capmarker_control = "";#192.168.1.10:4000";

my $platform_status = 'platform_Status'; 
$experiment_summary = "experiment_Summary";

#fetch basic info 

#my $localendpoint='192.168.186.135:4000';


print Timestamp() . " Booting, collecting information.\n";

$host_name = hostname();
$pid = $$;

#print Timestamp() . " host_name = $host_name.\n";

$host_IP = gethostbyname($host_name) or die ("could not resolve host IP of $host_name: $!");
#print Timestamp() . " host_IP =  $host_IP.\n";
$host_IP = inet_ntoa($host_IP);
print Timestamp() . " Server information: $host_name / $host_IP.\n";


#@args = @ARGV;
#Provide Options for commandline arguments
#Command line arguments can be a. Port b. Username  of Database c.Password of database; 
GetOptions ("port=i" => \$server_port,
	    "username=s" => \$user,
            "password=s" => \$password,
            "todo_table=s" => \$todo_table_name,
            "log_table=s" => \$log_table_name,
	    "general_todo=s" => \$general_todo,
            "platform=s"  => \$platform,       
	);
#Connect to Database. 
print Timestamp() . " Server port is $server_port.\n";
print Timestamp() . "        todo_table is $todo_table_name.\n";
print Timestamp() . "        logtable is $log_table_name.\n";
print Timestamp() . "        general table is $general_todo \n";
my $dbh = DBI->connect ("DBI:mysql:$db:$host;user=$user;password=$password",{'mysql_auto_reconnect'=>1}) or die ("$platform PC CONTROL Could not connect to database : " .DBI->errstr);
$dbh->{mysql_auto_reconnect}=1;


#Start the TCP server                                                                                                                                                                                     
print Timestamp() . " Starting server on $host_name($host_IP) on port $server_port\n";
print Timestamp() . " MySQL.autoreconnect = " . $dbh->{mysql_auto_reconnect} . "\n";

$serv = "$host_name:$server_port($pid)";
$server = IO::Socket::INET->new (LocalPort => $server_port, Type => SOCK_STREAM, Reuse => 1,Listen => 10) or die "TG could not be a server port on port $server_port: $@ \n" ;

$server->autoflush();
print Timestamp() . " Server started\n";


 SERVER: while (($client, $client_address) = $server->accept()) {
     ($port,$iaddr) = sockaddr_in($client_address);
     print Timestamp() . " Connected ( " . inet_ntoa($iaddr) . ":$port), \n"; 
   CLIENT: while ( $msg2 = <$client>) {
#       print "READ = [$msg2] ";
       chomp($msg2);
       ($cid,$msg)=split(';',$msg2);
       print Timestamp() . " Platform <$cid>"; #, cont= $msg "; 
       if ($cid =~/REPORT/) {
	   ($expid,$runid) = split (':',$msg);
	   print "REPORTING $expid $runid \n";
	   my $sth = $dbh->prepare ("select `serial_Number`, `platform_Name`,`total_run_Number`, `application_Command`, `Person`,`Status`  from $general_todo WHERE serial_Number = $expid");
	   $sth->execute();
	   my $val = $sth->fetchrow_hashref();
	   %val=%$val;
	   $total_runid = $val{'total_run_Numer'}+1;
	   $application_command = $val{'application_Command'};
	   $platform = $val{'platform_Name'};
	   $toaddress = $val{'Person'};#[4];
	   $current_status = $val{'Status'};#[5];	       
	   print Timestamp(). "\t\t------------Current Status : $current_status-------------------------------\n";
	   @p = split ("todo_",$platform);
	   
	   
	   # UPDATE LOGS
	   my $jth = $dbh->prepare ("UPDATE  `log_$p[1]` SET  Status = ?  WHERE exp_Number = $expid AND run_Number = $runid");
	   print  "UPDATE  `log_$p[1]` SET  Status = ?  WHERE exp_Number = $expid AND run_Number = $runid\n";
	   $jth->execute ("FAILURE");
	   if ($current_status eq 'CANCELED')
	   {
	       last CLIENT;
	   }
	   # update general_todo
	   my $jth = $dbh->prepare ("UPDATE  $general_todo SET  total_run_Number = $total_runid  WHERE serial_Number = $expid");
	   $jth->execute();
	   #Send email
	   $timenow=  strftime "%Y-%m-%d %H:%M:%S",localtime;
	   print Timestamp() . "SENDING EMAIL to $toaddress about experiment failiure for $expid $runid on $platform.\n";
	   sendEmail("$toaddress","lontas\@mma.nplab.bth.se", "Experiment FAILURE-- experiment ID -$expid, runID - $runid ,Platform -- $platform", "This is to notify you for the experiment Failure of Experiment ID $expid -- configured for--- $application_command for  $total_runid failed for the run $runid at   $timenow due to MEASUREMENT PROBLEMS. Kindly goto the MMA Experiment webpage and check your experiment Logs and kindly attend to it. This is autogenerated Email, Kindly donot reply!");
	   #Re execute if the status is not marked CANCELLED OR INVALID
	   if ($current_status =~/CANCELED/) {
	       print Timestamp() . "Experiment is Deleted, I am not going to UPDATE!\n";

	   }else{
#	       print Timestamp() . " jing\n";
	       my $sth = $dbh->prepare ("INSERT INTO `todo_$p[1]` (exp_Number, run_Number, total_run_Number,application_Command,preferred_Time) VALUES (?,?,?,?,?)");
	       $sth->bind_param(5, $timenow , SQL_DATETIME);
	       
	       $sth->execute($expid,$total_runid,$total_runid,$application_command,$timenow);
	   }
	   last CLIENT;
       } # E
       # updating platform status if platform exists fine, UPDATE else ADD
       my $sth = $dbh-> prepare ("SELECT 1 FROM `platform_Status` where `platform_name` = ? LIMIT 1");
       $sth->bind_param(1, $cid);
       $result = $sth->execute();
       print $result;
       if (($result eq '0E0' )) {
#				insert 
	   my $sth = $dbh->prepare ("INSERT INTO `platform_Status` (`platform_Name`)  VALUES (?)");
	   $sth->bind_param(1, $cid);
	   $sth->execute() 			
       }
       
#				update
       
       my $sth = $dbh->prepare ("UPDATE `platform_Status` SET platform_Time = ?, platform_Message = ? WHERE platform_name = ?");
       $timelater=  strftime "%Y-%m-%d %H:%M:%S",localtime;
       $sth->bind_param(1, $timelater, SQL_DATETIME);
       $sth->execute ($timelater, $msg, $cid);
       
       
       switch ($cid) {			
	   case 'NOTRECOGNIZED' {
	       print Timestamp() . " CID($cid) is NOT Recognized, checking \n";
	       @mobile_platform_name = ("IPhone","Android","WinMobile");
	       if (@mobile_platform_name > 0) {
		   $prepare = sprintf ("select * from $general_todo WHERE platform_Name = \"todo_". $mobile_platform_name[0] ."\"" );
		   for ($i = 1 ; $i < @mobile_platform_name; $i++) {
		       $t = " OR platform_Name = \"todo_". $mobile_platform_name[$i]. "\"";
		       $prepare = "$prepare"."$t ";
		   }
		   $prepare = "$prepare"." order by preferred_Time";
		   #print " Prepare statement is $prepare\n";
		   $counter = $counter + 1;
		   print Timestamp() . "\t\t counter = $counter \n";
		   if ($counter > 0) {
		       if ( ($counter == 1) || ( ($counter/100000) == 0) ) {  
			   # Check the todo_phone and inform the first guy regarding this
			   # send an email, parse the general todo for email address
			   my $sth = $dbh->prepare ($prepare);
			   $result = $sth->execute();
			   if (($result eq '0E0' )) {
			       print Timestamp() . "\t I am not Informing anyone that Mobile platforms are disconnected!";
			       print Timestamp() . "\t As there is no experiments involved \n";
			       #				
			   }
			   else {
			       #				
			       my $val = $sth->fetchrow_hashref();
			       %val=%$val;
			       $person = $val{'Person'};
			       #	print " person is $person\n";
			       # CHECK IF STATUS IS STARTED OR NULL
			       if ($val{'Status'} eq undef) {
				   $toaddress = $person;
				   $timenow=  strftime "%Y-%m-%d %H:%M:%S",localtime;
				   sendEmail("$toaddress","lontas\@mma.nplab.bth.se", "Kindly Connect your Mobile Platform to Mobile Controller", " This is to notify you that your Mobile phone is not connected to Mobile Controller Platform or perhaps it is not default to Home Screen. It would be great if you could come and connect your Mobile Station Home screen to Controller. This email is being sent to you at  $timenow with following-- An automated email, donot reply back\n");								
			       }							
			       
			   }
		       }						    
		   }										
	       }
	       
	       print Timestamp() . "\t Printing HOLD to CLIENT\n";
	       my $sth = $dbh->prepare ("UPDATE  $platform_status  SET server_Time = ?, server_Message = 'HOLD'  WHERE platform_name = ?");
	       $timelater=  strftime "%Y-%m-%d %H:%M:%S",localtime;
	       $sth->bind_param(1, $timelater, SQL_DATETIME);
	       $sth->execute ($timelater,$cid);
	       print $client "HOLD\n";
	       $msg = "BYE";
	   }
	   else {
	       $sqlc = "SELECT count(*)  FROM platforms WHERE name = '$cid' ";
	       my ($crows) = $dbh->selectrow_array($sqlc);
#	       print $crows;
	       if ($crows > 0) {
		   $sql1 = "select `IP`, `plKEY`,`shaperInfo` from platforms where name = '$cid'";
		   $sth = $dbh->prepare($sql1);
		   $sth->execute();
		   if ($sth->rows < 0) {
		       print "\t\t Sorry, no domains found.\n";
		       $shaper_IP = '';
		       $shaperconfig = "";
		       $consumer_IP = '';
		       $capmarker_key='';
		       $capmarker_control = '';
		   } else {
		       printf "\t\t Found %d platform(s)\n", $sth->rows;
		       # Loop if results found
		       while (my $results = $sth->fetchrow_hashref) {
#			   $consumer_IP = $results->{'consumerIP'}; # get the domain name field
#			   $capmarker_control = $results->{'sendMarkersTo'};

			   $shaperinfo = $results->{'shaperInfo'};
			   $capmarker_key= ''; #$results->{'KEY'};
			   print Timestamp() . "\t\t GOT FROM DB \n";
			   @args2 = split (/ /,$shaperinfo) ;
			   $shaper_IP = $args2[0];
			   $shaperconfig = "$args2[1];$args2[2]";
			   $consumer_IP_print =~ s/\r\n/,/g;
			   $consumer_IP_print =~ s/,$//;
			   print Timestamp() . "\t\t ShaperConfig IP   : $shaper_IP Configuration: $shaperconfig \n";
#			   print Timestamp() . "\t\t Consumer(s)       : " . join(" ", split(/\s+/,$consumer_IP))  . " \n";
#			   print Timestamp() . "\t\t Capmarker_control : " . join(" ", split(/\s+/,$capmarker_control)) ." \n";
			   print Timestamp() . "\t\t Capmarker_key     : " . $capmarker_key ."\n";
		       }
		   }
		   $todo_table_name="todo_$cid";    $log_table_name="log_$cid";   # $capmarker_control = "";
		   print Timestamp(). " Identified as platfrom $cid \n";
	       }else {
		   print Timestamp(). " ERROR! unknown $cid.\n";
		   last CLIENT;
	       }
	   }
       }
#       print " $cid ";
       if ($msg =~/\bBYE\b/) {
	   print "TG got BYE\n";
	   print "terminating connection with server and closing SOCKET\n";
	   last CLIENT;
       } # End of IF Message is BYE
       elsif ($msg=~/\bSHUTDOWN \b/) {
	   print "TG got Shutdown\n";
	   print $client "SERVER SHUTTING DOWN\n";
	   last SERVER;
       } # End of Elsif MSG is equal to Shut down
       
       elsif ($msg =~ /CRAP/)  {
	   
	   print "GOT a CRAP message\n";
	   print "***************************\n";
	   print $msg;
	   print "***************************\n";

	   @args = split (/:/,$msg) ;
	   $status = $args[0];
	   $expid = $args[1];
	   $runid = $args[2];
	   $total_runid = $args[3];
	   $application_command = decode_base64($args[4]);
	   $bigstdout = decode_base64($args [5]);
	   $x = "";
	   @std = split (/GGGGGG/,$bigstdout);
	   for ($i = 0 ; $i < @std ; $i++) {
	       $t = $std[$i];
	       $x = "$x"."\n$t";
	   }
	   ## SEND END MARKER
	   @std = split (/todo_/,$todo_table_name); 
	   
	   $platform = $std[1];
	   $platform = "$platform;$shaperconfig";
	   $shaperStr = "TC:$platform:RESET";
	   informConsumer($shaper_IP,$shaperStr);

#IDENTIFY the CONSUMERs that particiapted. 
#Find what consumers where involved.                                                                                                                                                            
	   print Timestamp() . " Idenifying consumers used in setup (l310).\n";
	   print Timestamp() . " ==> SELECT * from activeConsumers WHERE expid='$expid' AND runid='$runid' AND keyid='$keyid' LIMIT 0,1 \n";
	   my $sthCon = $dbh-> prepare ("SELECT * from activeConsumers WHERE expid='$expid' AND runid='$runid' AND keyid='$keyid' LIMIT 0,1");
	   $resultCon = $sthCon->execute();
	   print Timestamp() . " Rows = " . $sthCon->rows . ". \n";
	   my $conInfo = $sthCon->fetchrow_hashref();
	   if(!$conInfo){
	       print Timestamp() . " Consumers where used, something went wrong I guess.  \n";
	       $capmarker_control='';
	   } else {
	       print Timestamp() . " Consumer => " .$$conInfo{'consumers'}  ." Markers => " . $$conInfo{'markers'} . "\n";
	       $capmarker_control=$$conInfo{'markers'};
	       #REMOVE NOTE

	       my $activeId = $$conInfo{'id'};
	       my $consumerTableId = $$conInfo{'consumerID'};
	       print Timestamp() . " Table  => DELETE FROM activeConsumers WHERE expid='$expid' AND runid='$runid' AND keyid='$keyid' and id='$activeId'\n" ;
	       my $sthCon = $dbh-> prepare ("DELETE FROM activeConsumers WHERE expid='$expid' AND runid='$runid' AND keyid='$keyid' and id='$activeId'");
	       $resultCon = $sthCon->execute();

	       print Timestamp() . " Table  =>   UPDATE consumers SET status=status - 1 WHERE id='$consumerTableId'\n" ;
	       my $sthCon = $dbh-> prepare ("UPDATE consumers SET status=status - 1 WHERE id='$consumerTableId'");
	       $resultCon = $sthCon->execute($$conInfo{'consumerID'});


	   }	   
	   
	   if($capmarker_control){
	       ## SEND END MARKER,
	       print Timestamp() . "Sending End Marker; with expid=$expid, runid=$runid k=$keyid to " . join(" ", split(/\s+/,$capmarker_control)) . "\n";
	       sendmarker($expid,$runid,$keyid,1, $capmarker_control);
	   }




#	   print Timestamp(). "\t Sending Marker; with expid=$expid, runid=$runid k=$capmarker_key to " . join(" ", split(/s+/,$capmarker_control)) . "\n";
#	   sendmarker($expid,$runid,$capmarker_key,1, $capmarker_control);



	   if ($runid < 2) {
	       my $sth = $dbh->prepare ("DELETE FROM `$todo_table_name` WHERE exp_Number = $expid ");
	       $sth->execute();
#	       print Time"DELETE FROM $todo_table_name WHERE exp_Number = $expid \n";
	   } else {
	       $status = 'FAILURE';
	   }

	   
	   my $sth = $dbh->prepare ("UPDATE  `$log_table_name` SET experiment_end_Time = ?, Status = ? , Log = ?  WHERE exp_Number = $expid AND run_Number = $runid AND total_run_Number = $total_runid ");
	   $timelater=  strftime "%Y-%m-%d %H:%M:%S",localtime;
	   $sth->bind_param(1, $timelater, SQL_DATETIME);
	   $sth->execute ($timelater,$status,$x);
	   my $sth = $dbh->prepare ("UPDATE  `$general_todo` SET Status = ? WHERE serial_Number = $expid  ");
	   if ($runid < 2) {
	       $sth->execute ("REMOVED");
	   }else {
	       $sth->execute ("TEMPFAILURE");
	   }
	   # send an email, parse the general todo for email address
	   my $sth = $dbh->prepare ("select * from $general_todo WHERE serial_Number = $expid");
	   $sth->execute();
	   my $val = $sth->fetchrow_hashref();
	   %val=%$val;
	   $person = $val{'Person'};
	   $platform = $val{'platform_Name'};
	   @p = split ("todo_",$platform);

	   $toaddress = $person;
	   print "$toaddress \n";
	   $timenow=  strftime "%Y-%m-%d %H:%M:%S",localtime;
	   sendEmail("$toaddress","lontas\@mma.nplab.bth.se", "Config/Issue: $expid/$runid, @p", "This is to notify you for the experiment misconfiguration of Experiment ID $expid -- configured for--- $application_command for  $totalruns at inter-experimental-run time of $frequency failed at $timenow with following $x\nLog: $bigstdout\n");
       }
       
       
       elsif ($msg =~ /SUCCESS/) {
	   print Timestamp() . "GOT message " . substr($msg,0, 30) ."... \n";
	   #parse message, delete from todo, update timestamp and result field in log, if runs == total runs, delete from general todo
	   @args = split (/:/,$msg);
	   $status = $args[0];
	   $expid = $args[1];
	   $runid = $args[2];
	   $keyid = $args[3];
	   $total_runid = $args[4];
	   $application_command = decode_base64($args[5]);
	   $bigstdout = decode_base64($args [6]);
	   $x = "";
	   @std = split (/GGGGGG/,$bigstdout);
	   for ($i = 0 ; $i < @std ; $i++) {
	       $t = $std[$i];
	       $x = "$x"."\n$t";
	   }
#IDENTIFY the CONSUMERs that particiapted. 
#Find what consumers where involved.                                                                                                                                                            
	   print Timestamp() . "Identifying consumers used in setup(l405).\n";
	   print Timestamp() . "==> SELECT * from activeConsumers WHERE expid='$expid' AND runid='$runid' AND keyid='$keyid' LIMIT 0,1 \n";
	   my $sthCon = $dbh-> prepare ("SELECT * from activeConsumers WHERE expid='$expid' AND runid='$runid' AND keyid='$keyid' LIMIT 0,1");
	   $resultCon = $sthCon->execute();
	   print Timestamp() . " Rows = " . $sthCon->rows . ". \n";
	   my $conInfo = $sthCon->fetchrow_hashref();
	   if(!$conInfo){
	       print Timestamp() . " Consumers where used, something went wrong I guess.  \n";
	       $capmarker_control='';
	   } else {
	       print Timestamp() . " Consumer => " .$$conInfo{'consumers'} . " Markers => " . $$conInfo{'markers'} ."\n";
	       $capmarker_control=$$conInfo{'markers'};
	       #REMOVE NOTE                                                                                                                                                                 
	       my $sthCon = $dbh-> prepare ("DELETE FROM activeConsumers WHERE expid='$expid' AND runid='$runid' AND keyid='$keyid'");
	       $resultCon = $sthCon->execute();

	       my $sthCon = $dbh-> prepare ("UPDATE consumers SET status=status - 1 WHERE id=?");
	       $resultCon = $sthCon->execute($$conInfo{'consumerID'});

	   }
	   
	   
	   if($capmarker_control){
	       ## SEND END MARKER,                                                                                                                                                          
	       print Timestamp() . "Sending End Marker; with expid=$expid, runid=$runid k=$keyid to " . join(" ", split(/\s+/,$capmarker_control)) . "\n";
	       sendmarker($expid,$runid,$keyid,1, $capmarker_control);
	   }
	   $consumers=$$conInfo{'consumers'};
	   $consumer_IP='';
#needed to filter away the port information. Not needed by the merged.
	   foreach $name (split(/\s+/,$consumers)){
	       ($IP,$port)=split(/:/,$name);
	       $consumer_IP.="$IP ";
	   }


	   $consumerStr="COPY:$expid:$runid";
	   print Timestamp(). " Consumer @ ". join(" ",split(/\s+/, $consumers)). " < $consumerStr \n";
	   informConsumer($consumers,$consumerStr);

	   print Timestamp(). " Merger @ " . join(" ", split(/\s+/,$consumer_IP)) . " < $consumerStr \n";
	   informMerger(join(" ", split(/\s+/,$consumer_IP)),$consumerStr);

	   @std = split (/todo_/,$todo_table_name);
	   $platform = $std[1];
	   $platform = "$platform;$shaperconfig";
	   $shaperStr = "TC:$platform:RESET";
	   print Timestamp() . " Shaper @ $shaper_IP < $shaperStr \n";
	   informConsumer($shaper_IP,$shaperStr);
	   print Timestamp() . "\t 112 executed successfully. \n"; #,localtime;
	   #update timestamp and result in log table 
	   my $sth = $dbh->prepare ("UPDATE `$log_table_name` SET experiment_end_Time = ?, Status = ?, Log = ?  WHERE exp_Number = $expid AND run_Number = $runid AND total_run_Number = $total_runid ");
	   $timelater=  strftime "%Y-%m-%d %H:%M:%S",localtime;
	   $sth->bind_param(1, $timelater, SQL_DATETIME);
	   $sth->execute ($timelater,$status,$x);





	   
	   my $sth = $dbh->prepare ("DELETE FROM `$todo_table_name` WHERE exp_Number = $expid AND run_Number =$runid AND total_run_Number = $total_runid");
	   $sth->execute();
	   print "DELETE FROM `$todo_table_name` WHERE exp_Number = $expid AND run_Number = $runid  AND total_run_Number = $total_runid\n";
	   my $sth = $dbh->prepare ("UPDATE `$experiment_summary` SET `ts` = NULL WHERE `expid` = $expid  ");			
	   $sth->execute ();
	   my $sth = $dbh->prepare ("SELECT COUNT(exp_Number) AS num_Tasks FROM `$todo_table_name` WHERE `exp_Number` = $expid  ");			
	   $sth->execute ();
	   my $pal = $sth->fetchrow_hashref() ;



	   print Timestamp() . "\t Tasks left for platform with expid $expid is $$pal{'num_Tasks'}\n";					
	   if ($$pal{'num_Tasks'} == 0)	{
	       #my $sth = $dbh->prepare ("DELETE FROM $general_todo WHERE serial_Number = $expid  AND total_run_Number = $total_runid");
	       #$sth->execute();
	       #print "DELETE FROM $general_todo WHERE exp_Number = $expid AND $run_Number =$runid AND $total_run_Number = $total_runid\n";
	       # UPDATE SUCCESS
	       my $sth = $dbh->prepare ("select `Status`  from `$general_todo` WHERE serial_Number = $expid");
	       $sth->execute();
	       while (my @val = $sth->fetchrow_array()) {
		   $current_status = $val[0];
	       }
	       if ($current_status eq 'CANCELED') {
	       }
	       else{
		   my $sth = $dbh->prepare ("UPDATE `$general_todo` SET Status = ? WHERE serial_Number = $expid  ");			
		   $sth->execute ($status);

		   # Send email to the guy who configured the experiment that his runs are success and he could access his results in the a particular webpage.
		   my $sth = $dbh->prepare ("select * from `$general_todo` WHERE serial_Number = $expid");
		   $sth->execute();
		   
		   $consumerStr="EXPCOMPLETE:$expid:$runid";
		   print Timestamp() . "InformMerger " . join(" ", split(/\s+/,$consumer_IP)). "  --  $consumerStr .\n";
		   informMerger(join(" ", split(/\s+/,$consumer_IP)),$consumerStr);

		   my $val = $sth->fetchrow_hashref();
		   %val=%$val;
		   $person = $val{'Person'};
		   $comm = $val{'application_Command'};
		   $platform = $val{'platform_Name'};
		   @p = split ("todo_",$platform);
#		   print Dumper(\%val);
		   $toaddress = $person;
		   print Timestamp() . " Will send email to => $toaddress \n";
		   $timenow=  strftime "%Y-%m-%d %H:%M:%S",localtime;
		   sendEmail("$toaddress","lontas\@mma.nplab.bth.se", "Experiment SUCCESS- $expid -- platform @p", "This is to notify you for the experiment Success of Experiment ID $expid -- configured for--- $comm for  $total_runid succeeded at  $timenow. Kindly goto the MMA Experiment webpage and check your experiment Logs. This is autogenerated Email, Kindly donot reply!");
	       }	
	   }
       }
       elsif ($msg =~ /FAILURE/) {
	   print Timestamp . "GOT message $msg (detected as FAILIURE)\n";
	   #parse message, delete from todo, update timestamp and result field in log, if runs == total runs, delete from general todo
	   
	   print "***************************\n";
	   print $msg;
	   print "***************************\n";
	   @args = split (/:/,$msg);
	   $status = $args[0];
	   $expid = $args[1];
	   $runid = $args[2];
	   $keyid = $args[3];
	   $total_runid = $args[4];
	   $application_command = $args[5];
	   $application_command = decode_base64($args[5]);
	   $bigstdout = decode_base64($args[6]);
	   $x = "";
	   @std = split (/GGGGGG/,$bigstdout);
	   for ($i = 0 ; $i < @std ; $i++) {
	       $t = $std[$i];
	       $x = "$x"."\n$t";
	   }
	   if($expid>0){
#Find what consumers where involved. 
	       print Timestamp() . " Idenifying consumers used in setup (l557)\n ";
	       print Timestamp() . " ==> SELECT * from activeConsumers WHERE expid='$expid' AND runid='$runid' AND keyid='$keyid' LIMIT 0,1 ";
	       my $sthCon = $dbh-> prepare ("SELECT * from activeConsumers WHERE expid='$expid' AND runid='$runid' AND keyid='$keyid' LIMIT 0,1");
               $resultCon = $sthCon->execute();
	       print Timestamp() . " Rows = " . $stCon->rows . ".\n";
	       my $conInfo = $sthCon->fetchrow_hashref();
               if(!$conInfo){
		   print Timestamp() . " Consumers where used, something went wrong I guess.  \n";
		   $capmarker_control='';
	       } else {
		   print Timestamp() . " Consumer => " .$$conInfo{'consumers'} . " Markers = > " . $$conInfo{'markers'} . "\n";
		   $capmarker_control=$$conInfo{'markers'};
		   #REMOVE NOTE 
		   my $sthCon = $dbh-> prepare ("DELETE FROM activeConsumers WHERE expid='$expid' AND runid='$runid' AND keyid='$keyid'");
		   $resultCon = $sthCon->execute();

		   my $sthCon = $dbh-> prepare ("UPDATE consumers SET status=status - 1 WHERE id=?");
		   $resultCon = $sthCon->execute($$conInfo{'consumerID'});
	       }
	       
	       
	       if($capmarker_control){
		   ## SEND END MARKER, 
		   print Timestamp() . "Sending Marker; with expid=$expid, runid=$runid k=$keyid to " . join(" ", split(/\s+/,$capmarker_control)) . "\n";
		   sendmarker($expid,$runid,$keyid,1, $capmarker_control);
	       }


	       @std = split (/todo_/,$todo_table_name);
	       $platform = $std[1];	$platform = "$platform;$shaperconfig";
	       $shaperStr = "TC:$platform:RESET";
	       informConsumer($shaper_IP,$shaperStr);
	       #update timestamp and result in log table 
	       my $sth = $dbh->prepare ("UPDATE  `$log_table_name` SET experiment_end_Time = ?, Status = ? , Log = ? WHERE exp_Number='$expid' AND run_Number='$runid' AND keyid='$keyid' AND total_run_Number='$total_runid' ");
	       $timelater=  strftime "%Y-%m-%d %H:%M:%S",localtime;
	       $sth->bind_param(1, $timelater, SQL_DATETIME);
	       $sth->execute ($timelater,$status,$x);
	       
	       my $sth = $dbh->prepare ("DELETE FROM `$todo_table_name` WHERE exp_Number='$expid' AND run_Number='$runid' AND keyid='$keyid' AND total_run_Number='$total_runid'");
	       $sth->execute();
	       print "DELETE FROM `$todo_table_name` WHERE exp_Number = $expid AND run_Number =$runid AND keyid=$keyid AND total_run_Number = $total_runid\n";
	       
	       #increment general todo total run number
	       $total_runid = $total_runid +1;
	       
	       
	       # email  ## Send email to right person! !! BUG
	       #sendEmail("vkk\@bth.se","failure\@bth.se", "Experiment Failure", "This is to notify you for the failure of the run number $runid, $expid -- $runid -- $timenow --- $application_command"); 
	       my $sth = $dbh->prepare ("select * from $general_todo WHERE serial_Number = $expid");
	       $sth->execute();
	       my $val = $sth->fetchrow_hashref();
	       %val=%$val;
	       $person = $val{'Person'};
	       $comm = $val{'application_Command'};
	       $platform = $val{'platform_Name'};
	       @p = split ("todo_",$platform);
	       $current_status = $val{'Status'};
	       print "Hello someone deleted the experiment! -- $current_status";
		   
	       $toaddress = $person;
	       print "$toaddress \n";
	       $timenow=  strftime "%Y-%m-%d %H:%M:%S",localtime;
	       if ($expid > 0) {
		   sendEmail("$toaddress","lontas\@mma.nplab.bth.se", "Experiment FAILURE-- experiment ID -$expid, runID - $runid ,Platform -- $platform", "This is to notify you for the experiment Failure of Experiment ID $expid -- configured for--- $comm for  $total_runid failed for the run $runid at   $timenow. Kindly goto the MMA Experiment webpage and check your experiment Logs and kindly attend to it. This is autogenerated Email, Kindly donot reply!");
	       }
	       if ($current_status =~/CANCELED/) {
		   print "Experiment is Deleted, I am not going to UPDATE!";
	       }
	       else {
		   # insert into todo
		   my $sth = $dbh->prepare ("UPDATE  `$general_todo`  SET total_run_Number = $total_runid  WHERE serial_Number = $expid");
		   $sth->execute();
		   print "UPDATE  `$general_todo`  SET total_run_Number = $total_runid  WHERE serial_Number = $expid";
		   my $sth = $dbh->prepare ("UPDATE  $general_todo SET Status = ? WHERE serial_Number = $expid  ");			
		   $sth->execute ("TEMPFAILURE");
		   $timenow=  strftime "%Y-%m-%d %H:%M:%S",localtime;
		   my $sth = $dbh->prepare ("INSERT INTO `$todo_table_name` (´exp_Number´, ´run_Number´, ´keyid´, ´total_run_Number´,´application_Command´,´preferred_Time´) VALUES (?,?,?,?,(select ´application_Command´ from ´general_Todo´ where ´serial_Number´=? limit 1),?)");
		   $sth->bind_param(5, $timenow , SQL_DATETIME);
		   
		   $sth->execute($expid,$total_runid,$total_runid,$keyid,$expid,$timenow);
		   #update all fields of todo tables and log tables where the case is met 
		   my $sth = $dbh->prepare ("UPDATE  `$todo_table_name`  SET total_run_Number='$total_runid'  WHERE exp_Number='$expid'");
		   print "UPDATE  `$todo_table_name`  SET total_run_Number = $total_runid  WHERE exp_Number = $expid \n";
		   $sth->execute();
		   
		   my $sth = $dbh->prepare ("UPDATE  `$log_table_name`  SET total_run_Number = $total_runid  WHERE exp_Number = $expid");
		   print "UPDATE  `$log_table_name`  SET total_run_Number = $total_runid  WHERE exp_Number = $expid \n";
		   $sth->execute();

	       }
	   } else {
	       print Timestamp() . " The platform terminated, without having executed any experiment ($expid, $runid).\n";
	   }
	   
       }
       elsif ($msg=~/QUERY/) {
	   print Timestamp() . " Query: SELECT * from `$todo_table_name` WHERE NOW() >= `preferred_Time` \n";
	   my $sth = $dbh-> prepare ("SELECT * from `$todo_table_name` WHERE NOW() >= `preferred_Time`");
	   $result = $sth->execute();
	   my ($count) = 0;
	   $count = $sth->fetchrow_hashref();
	   @std = split (/todo_/,$todo_table_name);
	   $platform = $std[1];
	   #	$shaperStr = "TC:$platform:RESET";
	   #	informConsumer($shaper_IP,$shaperStr);
#			print " executing SELECT * from $todo_table_name \n";
#			print "sth returned $sth\n Count is $count  Result is $result\n";
	   # if the sql returns 0E0 it means ; return NORESULTFORQUERY to the client, else obtain the experimental configuration and send the configuration to the client; 
	   if (($result eq '0E0' )) {
	       print  Timestamp() . "\t No jobs found in $todo_table_name, for " . inet_ntoa($iaddr) . ":$port.\n"; 
	       my $platformIdentity=sprintf("%s:%d",inet_ntoa($iaddr),$port);
	       my $sth = $dbh->prepare ("UPDATE  $platform_status  SET server_Time = ?, server_Message = 'HOLD', lastIP = ? WHERE platform_name = ?");
	       $timelater=  strftime "%Y-%m-%d %H:%M:%S",localtime;
	       $sth->bind_param(1, $timelater, SQL_DATETIME);
	       $sth->execute ($timelater,$platformIdentity, $cid);
	       print $client "HOLD\n";
	   }
	   else {
	       print Timestamp() . "Found atleast ONE job.\n";
	       print Timestamp() . "Scheduled to run at " . $count->{'preferred_Time'} . ".\n";
	       
	       
	       $sth = $dbh-> prepare ("SELECT id,IP,PORT,status, Comment from consumers WHERE status=0 ORDER BY status ASC LIMIT 1");
	       $resultCon = $sth->execute();
	       if ($resultCon eq '0E0') {
		   print Timestamp() . "No consumers availible. Check if this needs a consumer.\n";

		   $sth = $dbh-> prepare ("SELECT * from `$todo_table_name` WHERE `measurementstreams`=`-1`");
		   $resultMS = $sth->execute();
		   if ($resultMS eq '0E0'){
		       print Timestamp() . "There are jobs, but they all need consumers. \n";
		       my $sth = $dbh->prepare ("UPDATE  $platform_status  SET server_Time = ?, server_Message = 'HOLD' WHERE platform_name = ?");
		       $timelater=  strftime "%Y-%m-%d %H:%M:%S",localtime;
		       $sth->bind_param(1, $timelater, SQL_DATETIME);
		       $sth->execute ($timelater, $cid);
		       print $client "HOLD\n";
		   } else {
		       print Timestamp() . "There are jobs, and atleast one can manage without a consumers. \n";
		       print Timestamp() . "Telling client to proceede .\n";
		       my $sth = $dbh->prepare ("UPDATE  $platform_status  SET server_Time = ?, server_Message = 'PROCEED'  WHERE platform_name = ?");
		       $timelater=  strftime "%Y-%m-%d %H:%M:%S",localtime;
		       $sth->bind_param(1, $timelater, SQL_DATETIME);
		       $sth->execute ($timelater,$cid);
		       print $client "PROCEED\n";
		   }
	       } else {
		   print Timestamp() . " Telling client to proceede .\n";
		   my $sth = $dbh->prepare ("UPDATE  $platform_status  SET server_Time = ?, server_Message = 'PROCEED'  WHERE platform_name = ?");
		   $timelater=  strftime "%Y-%m-%d %H:%M:%S",localtime;
		   $sth->bind_param(1, $timelater, SQL_DATETIME);
		   $sth->execute ($timelater,$cid);
		   print $client "PROCEED\n";
	       }
	   }
       } elsif ($msg=~/CONFIG/) {

	   print Timestamp() . "Pre-checking Consumer availability.\n";
	   $sthCon = $dbh-> prepare ("SELECT id,IP,PORT,status, Comment from consumers ORDER BY status ASC LIMIT 1");
	   $resultCon = $sthCon->execute();
	   my $conInfo = $sthCon->fetchrow_hashref();
	   
	   my $sth = $dbh->prepare ("select * from `$todo_table_name` order by preferred_Time, run_Number LIMIT 1");
	   if ($conInfo) {
	       print Timestamp() . "Atleat one consumer is available, select any job. \n";
	       	   $sth = $dbh->prepare ("select * from `$todo_table_name` order by preferred_Time, run_Number LIMIT 1");
	   } else {
	       print Timestamp(). "We run out of consumers, try to select a job that does not require a consumer.\n";
	       $sth = $dbh->prepare ("select * from `$todo_table_name` WHERE `measurementstreams`=`-1` ORDER BY preferred_Time, run_Number LIMIT 1");
	   }
	   $sth->execute();

	   $i = 1;
	   while (my $val = $sth->fetchrow_hashref()) {
	       $expid = $$val{'exp_Number'};# [0];
	       $runid = $$val{'run_Number'};#val [1];
	       $total_run_number = $$val{'total_run_Number'};#[2];
	       $application_Command = $$val{'application_Command'};# [3];
	       $preferredtime = $$val{'preferred_Time'};#[4];
	       $keyid = $$val{'keyid'};
	       $localStreams = $$val{'measurementstreams'};
	       $extStreams = $$val{'extMeasurementstreams'};

	       $sequence_Number = $expid + $runid ;
	       print Timestamp() . "$i\n";
	       $i++;
	       $timenow=  strftime "%Y-%m-%d %H:%M:%S",localtime;
	       $time = localtime();
	       my $end = DateTime::Format::MySQL->parse_datetime($preferredtime);
	       my $notz= DateTime->now->set_time_zone( 'Europe/Stockholm' );
	       if (DateTime->compare( $notz, $end ) == 1) {  
		   print Timestamp() . "STARTING an Experiment.\n";
		   #update the Status in general Todo to STARTED
		   my $sth = $dbh->prepare ("UPDATE  $general_todo SET Status = ? WHERE serial_Number = $expid  ");			
		   $sth->execute ("STARTED");
		   $app = encode_base64($application_Command,"");
		   print Timestamp(). "ApplicationCommand ( ".length($application_Command)."chars):\n$application_Command\n";
		   print Timestamp() . "Encoded the application_command.\n";
		   if (decode_base64($app) != "$application_Command" ){
		       print Timestamp(). " Encoded != original ·\n";
		       print Timestamp(). " Encoded = " . decode_base64($app) . "\n";
		       print Timestamp(). " Original= " . $application_Command . "\n";
		   } else {
		       print Timestamp() . "Encoded and original seems same," . length(decode_base64($app)) . " vs  " . length($application_Command). "\n"; 
		   }

		   my $sth = $dbh->prepare ("UPDATE  $platform_status  SET server_Time = ?, server_Message = \"CONFIG,$expid,$runid,$total_run_number,$app\"  WHERE platform_name = ?");
		   $timelater=  strftime "%Y-%m-%d %H:%M:%S",localtime;
		   $sth->bind_param(1, $timelater, SQL_DATETIME);
		   $sth->execute ($timelater,$cid);

## Setup Consumer...
		   print Timestamp() . "Setting up Consumers; local: $localStreams \n";

		   #$sthCon = $dbh-> prepare ("SELECT id,IP,PORT,MIN(status),Comment from consumers ");
		   $sthCon = $dbh-> prepare ("SELECT id,IP,PORT,status, Comment from consumers ORDER BY status ASC LIMIT 1");
		   $resultCon = $sthCon->execute();
		   my $conInfo = $sthCon->fetchrow_hashref();

		   if ($localStreams ne "-1" and $localStreams ne "" ) { 

		       if(!$conInfo){
			   print Timestamp() . " ************************************************** \n";
			   print Timestamp() . "            No Local Consumers availible \n";
			   print Timestamp() . " ************************************************** \n";
			   my $sth = $dbh->prepare ("UPDATE  $general_todo SET Status = ? WHERE serial_Number = $expid  ");			
			   $sth->execute ("PENDING");
			   my $sth = $dbh->prepare ("UPDATE  $platform_status  SET server_Time = ?, server_Message = 'HOLD'  WHERE platform_name = ?");
			   $timelater=  strftime "%Y-%m-%d %H:%M:%S",localtime;
			   $sth->execute ($timelater,$cid);
			   print $client "CONFIG,0\n";
			   print Timestamp() .  " Telling client to try again later \n";
			   next;
		       } else {
			   print Timestamp() . " Consumer => " .$$conInfo{'Comment'} . "@" .$$conInfo{'IP'} . ":" .$$conInfo{'PORT'} .".\n";
		       }
		       $conString="START:$expid:$runid:$keyid:$localStreams";
		       if (startConsumer($$conInfo{'IP'},$$conInfo{'PORT'},$conString)) {
			   my $sthcon2=$dbh->prepare("UPDATE consumers SET status=? WHERE id = ?");
			   $sthcon2->execute($$conInfo{'status'}+1,$$conInfo{'id'});
			   print Timestamp() . " Allocated a consumer => " . $$conInfo{'IP'}.":" . $conInfo{'PORT'} . " " . $$conInfo{'Comment'} . "\n";
		       } else {
			   print Timestamp() . " ************************************************** \n";
			   print Timestamp() . "  Failed to allocate a consumer, for some reason.\n";	
			   print Timestamp() . "         Break assignment somehow nicely.\n";
			   print Timestamp() . " ************************************************** \n";

		       }
		       $capmarker_control=$$conInfo{'IP'}.":4000";
		       $consumers_control=$$conInfo{'IP'}.":" . $$conInfo{'PORT'};
		   } else {
		       print Timestamp() . " This is a no trace experiment, skip the local consumers.\n";
		       $consumers_control="-1";
		   }

	
		   if($extStreams){ #Handle External Streams
		       print Timestamp() . " External Consumers used, initialzing them.\n";
		       @extCons=split(/\n/,$extStreams);
		       foreach $externalConsumer (@extCons) {
			   ($devIP,$devPort,$devStreams)=split(/\*/,$externalConsumer);
			   $devStreams=~ tr|;|,|;
			   $conString="START:$expid:$runid:$keyid:$devStreams";
			   print Timestamp() . "$devIP:$devPort handling  $devStreams \n";
			   $devResponse=startConsumer($devIP,$devPort,$conString);
			   print Timestamp() . "\t Device responded: $devResponse .\n";
			   
			   $capmarker_control.= " $devIP:4000";
			   $consumers_control.= " $devIP:$devPort";
		       }
		   } else {
		       print Timestamp() . " No External Consumers used.\n";
		   }

		   # Added to simplify sending the END marker.
		   my $sth = $dbh->prepare ("INSERT INTO `activeConsumers` (consumerID, expid, runid, keyid, consumers, markers) VALUES (?,?,?,?,?,?)"); 
		   print "localStreams = $localStreams.\n";
		   if ($localStreams ne "-1" ){
		       ## SEND START MARKER, 		   #sleep(10);$timenow=  strftime "%Y-%m-%d %H:%M:%S",localtime;
		       print Timestamp() . "Sending Marker; with expid=$expid, runid=$runid k=$keyid to " . join(" ", split(/\s+/,$capmarker_control)) . "\n";
		       sendmarker($expid,$runid,$keyid,0, $capmarker_control);
		       print Timestamp() . " consumer id = " . $$conInfo{'id'} . ". \n";
		       $sth->execute($$conInfo{'id'},$expid,$runid,$keyid,$consumers_control,$capmarker_control);
		   } else {
		       print Timestamp() . "No markers to be sent. \n";
		       $sth->execute(-1,$expid,$runid,$keyid,$consumers_control,$capmarker_control);
		   }
		   
		   ## CONFIGURE SHAPER
		   @std = split (/todo_/,$todo_table_name);
#		   print "\nSHAPERSETT std[1] = $std[1] .. todo_table_name -- $todo_table_name\n";
#		   print "shaperconfig=$shaperconfig<br>\n";
		   $platform = $std[1]; 	$platform = "$platform;$shaperconfig";
		   $shaperStr = "TC:$platform:$expid";
		   print Timestamp() . "SHAPER SETTINGS: <<$shaperStr>>-- |$platform|$expid  ";
		   informConsumer($shaper_IP,$shaperStr);
		   print Timestamp() . " Now we are supposed to be ready, lets tell the requesting device to continue.\n";
		   ## INFORM PLATFORM
		   print $client "CONFIG,$expid,$runid,$keyid,$total_run_number,$app\n"; # limiter space
		   print Timestamp() . "Sent . " .length("CONFIG,$expid,$runid,$keyid,$total_run_number,$app\n") . " chars to client.\n";

		   #insert into log_database
		   $timenow=  strftime "%Y-%m-%d %H:%M:%S",localtime;
		   my $sth = $dbh->prepare ("INSERT INTO `$log_table_name` (exp_Number, run_Number, keyid, total_run_Number,experiment_start_Time,Comment) VALUES (?,?,?,?,?,'none')");
		   $sth->execute($expid,$runid,$keyid,$total_run_number,$timenow);
		   print Timestamp() . "Inserted in to database ($log_table_name, $expid,$runid,$total_run_number). \n";
	       } else {  
		   my $sth = $dbh->prepare ("UPDATE  $platform_status  SET server_Time = ?, server_Message = 'HOLD'  WHERE platform_name = ?");
		   $timelater=  strftime "%Y-%m-%d %H:%M:%S",localtime;
		   $sth->execute ($timelater,$cid);
		   print $client "CONFIG,0\n";
		   print Timestamp() .  "CONFIG,0 current time is $notz \n";
		   next;
	       }  
	       #send capmarker
	       # Do the analysis for the experiment.
	   } #while
       } #else
       #DISCONNECT FROM DATABASE
   } # Client 
     close($client);
     print Timestamp() . "Disconnected\n";
} # Server

print Timestamp() . "About to sth->finish() and disconnect()";
$sth->finish();
$dbh->disconnect();



sub informHost{
my ($SAC,$message) = @_;
print $SAC "$message\n";
}

sub startConsumer{
    my ($IP,$PORT,$message)=@_;
    my $timeout = 3;
    print Timestamp() . " startConsumer on $IP:$PORT with  $message \n";
    $socketreceiver = IO::Socket::INET->new(PeerAddr=>$IP,PeerPort=>$PORT,Proto=>"tcp",Type=>SOCK_STREAM,Timeout=>$timeout);
    if(!$socketreceiver){
	print("Cannot connect to $IP:$PORT :$@\n");
	#next;
	return(0);
    }

    IO::Socket::Timeout->enable_timeouts_on($socketreceiver);
    $socketreceiver->read_timeout(1);
    $socketreceiver->write_timeout(1);

    use Errno qw(ETIMEDOUT EWOULDBLOCK);
 
    
# Inform server QUERY
    print $socketreceiver "$message\n";
    print "startConsumer:  ($IP):$message \n";
    $reply=<$socketreceiver>;
    if (! $reply && ( 0+$! == ETIMEDOUT || 0+$! == EWOULDBLOCK )) {
	print "startConsumer: Timeout reading on the socket";
	return(0);
    }
    print "Consumer said: $reply\t";
    close($socketreceiver);
    if($reply=~m/OK/){ 
	print "nice. \n";
    } else {
	print "bad.\n";
	return(0);
    }
    return(1);		
}


sub informConsumer{
    my ($IParray,$message) =@_;   
    my (@IPlist)=split(/\s+/,$IParray);
    my $devices=scalar(@IPlist);
    my $index=0;
    my $timeout = 3;
    print "\n";
    print Timestamp() . " iC: there are $IParray entities ($devices) .\n";
    if ( $devices == 0 ){
	print Timestamp() . "[inform] No IP provided ($IParray, $message), will not bother to check.\n";
	return(0);
    }
    
    for ( $index=0;$index<$devices;$index++){
	$IP=$IPlist[$index];
	
	print Timestamp() . " iC: IP= $IP \n";
	if (!$IP){
	    print Timestamp() . "[inform] No IP provided ($IParray, $message), will not bother to check.\n";
	    next;
	    #return(0);
	}
	$port=1579;
	if( $IP =~ /:/ ) {
	    ($IP,$port)=split(/:/,$IP);
	} 
	if($IP==-1){
	    print Timestamp(). "[inform] No consumer used.\n";
	} else {
	    $socketreceiver = IO::Socket::INET->new(PeerAddr=>$IP,PeerPort=>$port,Proto=>"tcp",Type=>SOCK_STREAM, Timeout=>$timeout);
	    if(!$socketreceiver){
		print("Cannot connect to $IP:$port :$@\n");
		close($socketreceiver);
		next;
		#return(1);
	    }


	     IO::Socket::Timeout->enable_timeouts_on($socketreceiver);
	    $socketreceiver->read_timeout(1);
	    $socketreceiver->write_timeout(1);

    use Errno qw(ETIMEDOUT EWOULDBLOCK);
# Inform server QUERY
	    print $socketreceiver "$message\n";
	    print "Sending ($IP:$port) => $message \n";
	    $reply=<$socketreceiver>;
	    if (! $reply && ( 0+$! == ETIMEDOUT || 0+$! == EWOULDBLOCK )) {
		print "startConsumer: Timeout reading on the socket";
		close($socketreceiver);
		next;
	    }
	    if($reply=~m/OK/){ 
		#OK
	    } else {
		print "Consumer said no:$reply\n";
	    }
	    close($socketreceiver);
	}
    }
    
    return(0);	
}


sub informMerger{
    my ($IParray,$message) =@_;   
    my (@IPlist)=split(/\s+/,$IParray);
    my $devices=scalar(@IPlist);
    my $index=0;
    my $timeout = 3;
    print "\n";
    print Timestamp() . " Merger: there are " . join(" ", $IParray) . "  entities ($devices) .\n";
    if ( $devices == 0 ){
	print Timestamp() . "[inform] No IP provided ($IP, $message), will not bother to check.\n";
	return(0);
    }
    if ( $IParray =~ m/-1/ ) {
	print Timestamp() . "[inform] No traces collected, no need to merge nothing.\n";
	return(0);
    }
    $socketreceiver = IO::Socket::INET->new(PeerAddr=>$MergeIP,PeerPort=>$merge_port,Proto=>"tcp",Type=>SOCK_STREAM,Timeout=>$timeout);
    if(!$socketreceiver){
	print("Cannot connect to $MergeIP:$merge_port :$@\n");
	return(0);
    }
    
    IO::Socket::Timeout->enable_timeouts_on($socketreceiver);
    $socketreceiver->read_timeout(1);
    $socketreceiver->write_timeout(1);

    use Errno qw(ETIMEDOUT EWOULDBLOCK);
    
    print Timestamp() . "Sending ($MergeIP):$message:" . join(" ", $IParray) . " \n"; # Inform server QUERY
    print $socketreceiver "$message:" . join(" ", $IParray) . " \n"; # Inform server QUERY
    
    $reply=<$socketreceiver>;
    if (! $reply && ( 0+$! == ETIMEDOUT || 0+$! == EWOULDBLOCK )) {
	print "startConsumer: Timeout reading on the socket";
	close($socketreceiver);
	return(0);
    }
    
    if($reply=~m/OK/){ 
	#OK
    } else {
	print "Merger said no:$reply\n";
    }
    close($socketreceiver);
    return(0);	
}



    
sub waitforreply {
    my ($SAC,$str) = @_;
    print "Am waiting.....for....$SAC.......to give me $str..............\n";
    $SAC->autoflush(1);
    
    
  line: while ($line = <$SAC>) {
      print " I got a msggggg ..:<br> $line </br>\n";
      # next unless /pig/;
      chomp($line);
      last line if ($line=~ /$str/) 
  }
    print "Hello World, My Wait for $SAC is over\n";
}



sub sendEmail
{
    my ($to, $from, $subject, $message) = @_;
    my $sendmail = '/usr/lib/sendmail';
    open(MAIL, "|$sendmail -oi -t");
    print MAIL "From: $from\n";
    print MAIL "To: $to\n";
    print MAIL "Subject: $subject\n\n";
    print MAIL "$message\n";
    close(MAIL);
} 

sub Timestamp {
    return strftime "%Y-%m-%d %H:%M:%S", localtime;
}


sub sendmarker{
    my ($expid,$runid,$key,$terminate, $capmarker_control) = @_;
    my (@IPlist)=split(/\s+/,$capmarker_control);
    my $devices=scalar(@IPlist);
    my $index=0;
    print "\n";
    print Timestamp() . " SendMarker : there are $capmarker_control entities ($devices) .\n";
    print Timestamp() . " SendMarker : terminate = $terminate key = $key \n";
    if ( $devices == 0 ){
	print Timestamp() . "SendMarker:  No consumers provided. \n";
	return(0);
    }
    
    for ( $index=0;$index<$devices;$index++){
	($IP,$cport)=split(/:/,$IPlist[$index]);
	if (!$IP or !$cport){
	    print Timestamp() . "SendMarker: No IP or port provided cant contact consumer.\n";
	    next;
	    #return(0);
	}
	$TFLAG='';
	if($terminate){
	    $TFLAG='-T ';
	}
	$KEYFLAG='';
	if($key){
	    $KEYFLAG="-k $key";
	}
	my $systring="capmarker -e $expid -r $runid $KEYFLAG -t $TFLAG $IP:$cport";
	print Timestamp() . "\t Sending Marker; $systring \n";
	system ("$systring");
    }
    return(0)
}
