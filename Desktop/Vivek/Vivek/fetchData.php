<?php
//setting header to json
header('Content-Type: application/json');

$conn_string = "host=localhost port=5432 dbname=vivekdb user=vivek password=postgres";
$dbconn = pg_connect($conn_string);

if(!$dbconn){
	die("Connection failed: " . $dbconn->error);
}

//query to get data from the table
$query = "SELECT humditiy FROM  iot.dht22_humidity_data";
 
//execute query
 $result = pg_query($query); 

//$result = $dbconn->query($query);

//loop through the returned data
$data = array();
foreach ($result as $row) {
	$data[] = $row;
}

//free memory associated with result
$result->close();


$dbconn->close();

//now print the data
print json_encode($data);
//close connection
pg_close($dbconn);
?>