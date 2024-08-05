<?php

namespace TheMoiza\Csvtopostgresql\Tests;

use TheMoiza\Csvtopostgresql\CsvToPgsql;

use PHPUnit\Framework\TestCase;

class ImportTest extends TestCase{

	public $failOnRisky = true;

	public $connection;

	public function setUp() :void
	{

		try{

			$dsn = 'pgsql'.':host=127.0.0.1;port=5432;dbname=csvtopostgresql';
			$this->connection = new \PDO($dsn, 'csvtopostgresql', 'csvtopostgresql', []);
			$this->connection->setAttribute(\PDO::ATTR_EMULATE_PREPARES, 1);

		}catch(\PDOException $e){

		}
	}

	public function testImport(){

		// DROP SCHEMA test_schema
		$this->connection->query('DROP SCHEMA if exists test_schema CASCADE;');

		$csvToPgsql = new CsvToPgsql;

		$csvToPgsql->setConfigs([
			'createPkey' => true,
			'enableTrim' => true,
			'enableTransaction' => true,
			'justCreateTables' => false,
			'inputEncoding' => 'UTF-8',
			'outputEncoding' => 'UTF-8',
			'readSeparator' => '2',
			'skipFiles' => []
		]);

		$result = $csvToPgsql->convertCsvFromZip(
			'csv_example.zip',
			[
				"DB_HOST" => "127.0.0.1",
				"DB_PORT" => "5432",
				"DB_DATABASE" => "csvtopostgresql",
				"DB_USERNAME" => "csvtopostgresql",
				"DB_PASSWORD" => "csvtopostgresql",
				"DB_SCHEMA" => "test_schema"
			]
		);

		$this->assertEquals(true, $result['result']);
	}

	public function testAmountLinesOfCitiesCsv(){

		$query = $this->connection->query('SELECT count(1) as total from test_schema.cities');
		$fetch = $query->fetch(\PDO::FETCH_OBJ);

		$this->assertEquals(128, $fetch->total);
	}

	public function testAmountLinesOfTreesCsv(){

		$query = $this->connection->query('SELECT count(1) as total from test_schema.trees');
		$fetch = $query->fetch(\PDO::FETCH_OBJ);

		$this->assertEquals(31, $fetch->total);
	}

	public function testDataTypes(){

		$query = $this->connection->query(
			"SELECT 
				cols.column_name AS column,
				cols.data_type AS type,
				cols.numeric_precision,
				cols.numeric_scale
			FROM information_schema.columns as cols
			WHERE cols.table_name = 'datatypes' AND cols.table_schema = 'test_schema'
			ORDER BY cols.ordinal_position ASC");

		$fetch = $query->fetchAll(\PDO::FETCH_ASSOC);

		$adjustArray = [];
		foreach($fetch as $line){

			$adjustArray[$line['column']] = $line;
		}

		$this->assertEquals('bigint', $adjustArray['integer']['type']);
		$this->assertEquals('text', $adjustArray['string']['type']);
		$this->assertEquals('timestamp without time zone', $adjustArray['timestamp']['type']);
		$this->assertEquals('date', $adjustArray['date']['type']);
		$this->assertEquals('boolean', $adjustArray['boolean']['type']);
		$this->assertEquals('numeric', $adjustArray['numeric']['type']);
	}
}