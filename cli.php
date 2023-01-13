#!/usr/bin/php -q
<?php

require_once 'vendor/autoload.php';

use TheMoiza\Csvtopostgresql\CsvToPgsql;

$csvToPgsql = new CsvToPgsql;

$csvToPgsql->setConfigs([
	'createPkey' => true,
	'enableTrim' => true,
	'enableTransaction' => true,
	'justCreateTables' => false,
	'inputEncoding' => 'UTF-8',
	'outputEncoding' => 'UTF-8'
]);

print 'Wait...'.PHP_EOL;

$result = $csvToPgsql->convertCsvFromZip(
	'csv_example.zip',
	[
		"DB_HOST" => "127.0.0.1",
		"DB_PORT" => "5432",
		"DB_DATABASE" => "csvtopostgresql",
		"DB_USERNAME" => "csvtopostgresql",
		"DB_PASSWORD" => "csvtopostgresql",
		"DB_SCHEMA" => "target_schema"
	]
);

print ($result['result'] ? 'Success' : 'Fail').PHP_EOL;

print $result['message'].PHP_EOL;