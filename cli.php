#!/usr/bin/php -q
<?php

require_once 'vendor/autoload.php';

use TheMoiza\Csvtopostgresql\CsvToPgsql;

$zip = 'csv_example.zip';

$connection = [
	"DB_HOST" => "127.0.0.1",
	"DB_PORT" => "5432",
	"DB_DATABASE" => "target_database",
	"DB_USERNAME" => "user",
	"DB_PASSWORD" => "pass",
	"DB_SCHEMA" => "target_schema"
];

$csvToPgsql = new CsvToPgsql($connection);
$csvToPgsql->verifySchema();
$csvToPgsql->convertFromZip($zip);