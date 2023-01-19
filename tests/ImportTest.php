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
			'outputEncoding' => 'UTF-8'
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

		$this->assertEquals($result['result'], true);
	}

	public function testAmountLinesOfCitiesCsv(){

		$query = $this->connection->query('SELECT count(1) as total from test_schema.cities');
		$fetch = $query->fetch(\PDO::FETCH_OBJ);

		$this->assertEquals($fetch->total, 128);
	}

	public function testAmountLinesOfTreesCsv(){

		$query = $this->connection->query('SELECT count(1) as total from test_schema.trees');
		$fetch = $query->fetch(\PDO::FETCH_OBJ);

		$this->assertEquals($fetch->total, 31);
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

		$this->assertEquals($adjustArray['integer']['type'], 'integer');
		$this->assertEquals($adjustArray['string']['type'], 'text');
		$this->assertEquals($adjustArray['timestamp']['type'], 'timestamp without time zone');
		$this->assertEquals($adjustArray['date']['type'], 'date');
		$this->assertEquals($adjustArray['boolean']['type'], 'boolean');
		$this->assertEquals($adjustArray['numeric']['type'], 'numeric');
	}
}