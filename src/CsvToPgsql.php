<?php

namespace TheMoiza\Csvtopostgresql;

use TheMoiza\Csvtopostgresql\CsvToPgsqlException;

class CsvToPgsql
{
	/**
	 * PDO connection to database
	 *
	 * @var object|bool
	 */
	protected $_pdo = false;

	/**
	 * PDO connection config, user, pass, db, schema, port, ip
	 *
	 * @var array
	 */
	protected $_dbConnection = [];

	/**
	 * ZIP temp_file()
	 *
	 */
	protected $_tempZipFile;

	protected $_currentTable;

	/**
	 * If will be created a _pkey_ columns and pkey constraint
	 *
	 * @var bool
	 */
	protected $createPkey = false;

	/**
	 * If a fit function is performed on all values
	 *
	 * @var bool
	 */
	protected $enableTrim = true;

	/**
	 * If true, start the transaction, if it fail make the rollback, if it success make the commit
	 *
	 * @var bool
	 */
	protected $enableTransaction = true;

	/**
	 * If true, only tables structure is created
	 *
	 * @var bool
	 */
	protected $justCreateTables = false;

	/**
	 * Encoding of the CSV
	 *
	 * @var string
	 */
	protected $inputEncoding = 'UTF-8';

	/**
	 * Encoding of the database
	 *
	 * @var string
	 */
	protected $outputEncoding = 'UTF-8';

	protected $skipFiles = [];

	/**
	 * Read separator
	 *
	 * @var string
	 * 1 = Only first line, 2 line by line
	 */
	protected $readSeparator = '1';

	private $_delimiter = ';';

	private $_colsHead = false;

	private $_insertQuery = [];

	public $byPageInsert = 50000;

	/**
	 * Set a config.
	 *
	 * @param  string  $param
	 * @param  bool|string  $value
	 * @param  array  $options
	 * @return object $this
	 */
	public function setConfig(string $param, $value)
	{
		// VALIDATE BOOL PARAMS
		if(is_bool($value) and in_array($param, ['createPkey', 'createPkey', 'enableTransaction', 'justCreateTables'])){

			$this->{$param} = $value;
		}

		// VALIDATE STRING PARAMS
		if(is_string($value) and in_array($param, ['inputEncoding', 'outputEncoding', 'readSeparator'])){
			$this->{$param} = $value;
		}

		// VALIDATE ARRAY PARAMS
		if(is_array($value) and in_array($param, ['skipFiles'])){
			$this->{$param} = $value;
		}

		return $this;
	}

	/**
	 * Set multiples configs.
	 *
	 * @param  array  $configs
	 * @return object $this
	 */
	public function setConfigs(array $configs)
	{

		foreach($configs as $param => $value){

			$this->setConfig($param, $value);
		}

		return $this;
	}

	/**
	 * Try to connect to postgresql.
	 *
	 * @param array $dbConnection
	 * @return object $this
	 *
	 * @throws \TheMoiza\Csvtopostgresql\CsvToPgsqlException
	 */
	protected function _connectPgsql(array $dbConnection)
	{

		$this->_dbConnection = $dbConnection;

		try{

			$dsn = $this->_dbConnection['DB_CONNECTION']??'pgsql'.':host='.$this->_dbConnection['DB_HOST'].';port='.$this->_dbConnection['DB_PORT'].';dbname='.$this->_dbConnection['DB_DATABASE'];
			$this->_pdo = new \PDO($dsn, $this->_dbConnection['DB_USERNAME'], $this->_dbConnection['DB_PASSWORD'], []);
			$this->_pdo->setAttribute(\PDO::ATTR_EMULATE_PREPARES, 1);

		}catch(\PDOException $e){

			throw new CsvToPgsqlException($e->getMessage());
		}

		return $this;
	}

	// REMOVE ACENTOS E Ç
	public function removeAccentCedil($string) :string
	{

		$string = str_replace(
			['À','È','Ì','Ò','Ù','Ã','Ẽ','Ĩ','Õ','Ũ','Â','Ê','Î','Ô','Û','Á','É','Í','Ó','Ú','à','è','ì','ò','ù','ã','ẽ','ĩ','õ','ũ','â','ê','î','ô','û','á','é','í','ó','ú','ç','ĺ'],
			['a','e','i','o','u','a','e','i','o','u','a','e','i','o','u','a','e','i','o','u','a','e','i','o','u','a','e','i','o','u','a','e','i','o','u','a','e','i','o','u','c','l'],
			$string
		);

		return $string;
	}

	/**
	 * Try to create the schema if it doesn't exist
	 *
	 * @return object $this
	 *
	 * @throws \TheMoiza\Csvtopostgresql\CsvToPgsqlException
	 */
	protected function _createSchema()
	{

		try{

			$this->_pdo->query('CREATE SCHEMA IF NOT EXISTS '.$this->_dbConnection['DB_SCHEMA']);

		} catch (\PDOException $e){

			throw new CsvToPgsqlException($e->getMessage());
		}

		return $this;
	}

	/**
	 * Save Zip file to temp file with tempfile() method
	 *
	 * @param string $zipUrl
	 * @return array
	 *
	 * @throws \TheMoiza\Csvtopostgresql\CsvToPgsqlException
	 */
	protected function _readZip($zipUrl) :array{

		if(!is_file($zipUrl)){
			throw new CsvToPgsqlException(sprintf('The %s file was not found, check the correct path.', $zipUrl));
		}

		if(mime_content_type($zipUrl) != 'application/zip'){
			throw new CsvToPgsqlException(sprintf('The %s file is not a zip file.', $zipUrl));
		}

		$this->_tempZipFile = tmpfile();
		fwrite($this->_tempZipFile, file_get_contents($zipUrl));
		fseek($this->_tempZipFile, 0);

		$zip = new \ZipArchive;
		$zip->open(stream_get_meta_data($this->_tempZipFile)['uri']);

		$extracted = [];

		for ($i = 0; $i < $zip->numFiles; $i++){

			$fileInZip = $zip->statIndex($i);

			$extracted[$fileInZip['name']] = $i;
		}

		// OPTIMIZE RAM MEMORY
		$zip = null;

		return $extracted;
	}

	// DETERMINA O DELIMITADOR DO CSV
	protected function _findDelimiter($pointer) :string{

		fseek($pointer, 0);
		$string = fread($pointer, 10000);

		if(!empty($string)){

			$ex = str_split($string);
			$chars = [];
			$chars[';'] = 0;
			$chars[','] = 0;

			foreach ($ex as $char){

				if($char == ';'){
					$chars[';'] = $chars[';'] + 1;
				}

				if($char == ','){
					$chars[','] = $chars[','] + 1;
				}
			}

			if($chars[';'] > $chars[',']){
				return ';';
			}
		}

		return ',';
	}

	// PROCESSA NOME DO ARQUIVO CSV PARA UM NOME ACEITO
	protected function _safeString(string $string) :string{

		// ALGUNS AQUIVOS VEM file..csv
		$string = str_replace('..', '.', $string);

		// REMOVE A EXTENSÃO
		$string = preg_replace('/.csv$/', '', $string);

		// REMOVE ACENTOS DE Ç
		$string = $this->removeAccentCedil($string);

		// TUDO PARA MINÚSCULO
		$string = strtolower($string);

		$string = trim($string ?? '', '_');

		// ALTERA ESPAÇOS E HIFEN PARA UNDERLINE
		$string = str_replace([' ', '-'], '_', $string);

		return $string;
	}

	protected function _detectDataType(string $value) :string{

		// DETECT TIMESTAMP
		if(preg_match('/^[0-9]{4}\-[0-9]{2}\-[0-9]{2} [0-9]{2}\:[0-9]{2}\:[0-9]{2}\.[0-9]+$/', $value)){

			$detected = 'timestamp';

		// DETECT DATE
		}else if(preg_match('/^[0-9]{4}\-[0-9]{2}\-[0-9]{2}$/', $value)){

			$detected = 'date';

		// IF MORE THAN 131072, SKIP, IT WILL BE A STRING
		}else if(preg_match('/^[0-9]+[.]{1}[0-9]+$|^-[0-9]+[.]{1}[0-9]+$/', $value) and $value <= 131072){

			$detected = 'numeric';

		// IF IS INTEGER BUT IS OUT OF RANGE, TURN TO bigint
		}else if(preg_match('/^[0-9]+$/', $value) and $value > 2147483647){

			$detected = 'bigint';

		// IF LESS THAN 2147483647 IS INTEGER
		}else if(preg_match('/^[0-9]+$/', $value) and $value <= 2147483647){

			$detected = 'integer';

		// BOOLEAN
		}else if(preg_match('/^[tf]+$/', $value) and strlen($value) == 1){

			$detected = 'boolean';

		}else{

			$detected = 'text';
		}

		return $detected;
	}

	// CRIA A TABELA CASO ELA AINDA NÃO EXISTA
	protected function _createTable($name, $ddls) :void{

		$ddlCols = [];

		if($this->createPkey){
			$ddlCols[] = '_pkey_ serial NOT NULL';
		}

		foreach($ddls as $key => $col){

			if($col['type'] == 'timestamp'){

				$ddlCols[] = '"'.$col['column'].'" timestamp NULL';

			}else if($col['type'] == 'bigint'){

				$ddlCols[] = '"'.$col['column'].'" bigint NULL';

			}else if($col['type'] == 'integer'){

				$ddlCols[] = '"'.$col['column'].'" int4 NULL';

			}else if($col['type'] == 'numeric'){

				$ddlCols[] = '"'.$col['column'].'" numeric(1000,50) NULL';

			}else if($col['type'] == 'date'){

				$ddlCols[] = '"'.$col['column'].'" date NULL';

			}else if($col['type'] == 'boolean'){

				$ddlCols[] = '"'.$col['column'].'" boolean NULL';

			}else if($col['type'] == 'undefined'){

				$ddlCols[] = '"'.$col['column'].'" varchar(100) NULL';
			}else{

				$ddlCols[] = '"'.$col['column'].'" text NULL';
			}
		}

		if($this->createPkey){
			$ddlCols[] =  'CONSTRAINT "'.$name.'_pkey" PRIMARY KEY (_pkey_)';
		}

		$cols = implode(','.PHP_EOL."\t", $ddlCols);

		$ddl = <<<ddl
CREATE TABLE IF NOT EXISTS {$this->_dbConnection['DB_SCHEMA']}."{$name}" (
	$cols
);
ddl;

		try{

			$this->_pdo->query($ddl);

		} catch (\PDOException $e){

			throw new CsvToPgsqlException($e->getMessage());
		}
	}

	// INSERE DADOS NA TABELA
	protected function _prepareInsert($tableName, $ddls, $currentLine) :void{

		// EXECUTE THIS ON TIME, PERFORMANCE OPTIMIZATION
		if(!$this->_colsHead){

			// CRIA COLUNAS
			$this->_colsHead = [];
			foreach ($ddls[$tableName] as $line) {
				$this->_colsHead[] = $line['column'];
			}
		}

		$payload = [];
		foreach($this->_colsHead as $key => $column){

			$val = $currentLine[$key] ?? null;

			// ESCAPE SIMPLES QUOTES '
			if(!is_null($val)){
				$val = 'E\''.str_replace("'", "\'", $val).'\'';
			}

			// EMPTY STRING OR NULL TO 'null'
			if($val === '' or is_null($val)){
				$val = 'null';
			}

			// SKIP INVALID CHAR
			if($val == "E'\x00'"){
				$val = "E''";
			}

			$payload['"'.$column.'"'] = $val;
		}

		// CREATE SQL
		$cols = implode(', ', array_keys($payload));
		$vals = implode(', ', array_values($payload));

		$this->_insertQuery[] = sprintf('INSERT INTO %s."%s" (%s) VALUES (%s);', $this->_dbConnection['DB_SCHEMA'], $tableName, $cols, $vals);

		$this->_currentTable = $tableName;
	}

	protected function _insert(bool $inLoop = true) :void
	{

		$savepoint = $this->_safeString(str_replace('/', '_', $this->_currentTable));

		if($inLoop === true and count($this->_insertQuery) == $this->byPageInsert){

			$sql = implode(PHP_EOL, $this->_insertQuery);

			// CREATE SAVE POINT
			$this->_pdo->query('SAVEPOINT savepoint_'.$savepoint.';');

			try{

				$this->_pdo->query($sql);

				$this->_insertQuery = [];

				print 'Insert into table '.$this->_safeString(str_replace('/', '_', $this->_currentTable)).' page...'.PHP_EOL;

			} catch (\PDOException $e){

				// ROLLBACK SAVE POINT
				$this->_pdo->query('ROLLBACK TO savepoint_'.$savepoint.';');

				print 'Insert page error at table: '.$this->_currentTable.', '.$e->getMessage().PHP_EOL;
			}

		}else if($inLoop === false and count($this->_insertQuery) > 0){

			$sql = implode(PHP_EOL, $this->_insertQuery);

			// CREATE SAVE POINT
			$this->_pdo->query('SAVEPOINT savepoint_'.$savepoint.';');

			try{

				$this->_pdo->query($sql);

				$this->_insertQuery = [];

			} catch (\PDOException $e){

				// ROLLBACK SAVE POINT
				$this->_pdo->query('ROLLBACK TO savepoint_'.$savepoint.';');

				print 'Insert error at table: '.$this->_currentTable.', '.$e->getMessage().PHP_EOL;
			}
		}
	}

	protected function scapeDelimiterInPgsqlArray($line){

		$start = strpos($line, '[{');
		$end = strrpos($line, '}]');

		if($start and $end){

			$arrayInString = substr($line, $start, ($end - $start + 2));

			$replace = str_replace(';', '[&semi]', $arrayInString);

			$line = str_replace($arrayInString, $replace, $line);
		}

		return $line;
	}

	// CONVERTE CSV TO ARRAY, FAZENDO OS DEVIDOS TRATAMENTOS
	protected function _readCsvStructure($pointer, string $delimiter, $ddls, $fn) :array{

		$key = 0;
		while (($line = fgets($pointer, 10000)) !== false){

			// PROCESS ARRAY PGSQL IN CSV
			$line = $this->scapeDelimiterInPgsqlArray($line);

			$line = str_getcsv($line, $delimiter);

			// SKIP EMPTY LINES ON CSV FILE
			if(count($line) > 0){

				// MAKE TRIM
				foreach($line as $k => $v){

					// CONVERT ENCODING
					if($this->outputEncoding != $this->inputEncoding){
						$v = mb_convert_encoding($v, $this->outputEncoding, $this->inputEncoding);
					}

					// TRIM
					if($this->enableTrim){
						$v = trim($v ?? '', " \n\r\t");
					}

					$line[$k] = $v;
				}

				// CSV IS EMPTY
				if(!$line or count($line) == 0){
					continue;
				}

				// MAKE DDLS
				$ddls = $fn($line, $key, $ddls);
			}

			$key++;
		}

		return $ddls;
	}

	// CONVERTE CSV TO ARRAY, FAZENDO OS DEVIDOS TRATAMENTOS
	protected function _readCsvAsArray($pointer, string $delimiter, $tableName, $ddls, $fnInsert) :void{

		$key = 0;
		while (($line = fgets($pointer, 10000)) !== false){

			// PROCESS ARRAY PGSQL IN CSV
			$line = $this->scapeDelimiterInPgsqlArray($line);

			$line = str_getcsv($line, $delimiter);

			// SKIP THE FIRST LINE OF CSV
			if($key == 0){
				$key++;
				continue;
			}

			// SKIP TABLE WITH READING ERROR
			if(!isset($ddls[$tableName])){
				continue;
			}

			// SKIP CSV EMPTY LINES
			if(count($line) != count($ddls[$tableName])){
				continue;
			}

			foreach($line as $k => $v){

				// CONVERT ENCODING
				if($this->outputEncoding != $this->inputEncoding){
					$v = mb_convert_encoding($v, $this->outputEncoding, $this->inputEncoding);
				}

				// TRIM
				if($this->enableTrim){
					$v = trim($v ?? '', " \n\r\t");
				}

				// EMPTY TO NULL
				if($v === ''){
					$v = null;
				}

				$line[$k] = $v;
			}

			$fnInsert($tableName, $ddls, $line);

			$key++;
		}
	}

	// MÉTODO PRINCIPAL PARA A CONVERSÃO
	public function convertCsvFromZip(string $zipUrl, $dbConnection = false) :array{

		try {

			$allFiles = $this->_readZip($zipUrl);

			if(is_array($dbConnection) and $dbConnection){
				$this->_connectPgsql($dbConnection);
			}

			if($this->enableTransaction and !$this->_pdo->inTransaction()){

				// START TRANSACTION
				$this->_pdo->beginTransaction();
			}

			$this->_createSchema();

			$zip = new \ZipArchive;
			$zip->open(stream_get_meta_data($this->_tempZipFile)['uri']);

			print 'Start to create tables...'.PHP_EOL;

			// FIND SEPARATOR BY FIRST LINE ONLY
			if($this->readSeparator == '1'){
				$delimiter = $this->_delimiter;
			}

			// SKIP FILES
			$files = array_filter($allFiles, function($key) use ($allFiles) {
				foreach ($this->skipFiles as $item) {
					if (strpos($key, $item) !== false) {
						return false;
					}
				}
				return true;
			}, ARRAY_FILTER_USE_KEY);

			// CREATE TABLES
			$ddls = [];
			foreach($files as $name => $index){

				print 'Reading file '.$name.'...'.PHP_EOL;

				try {

					if(preg_match('/.csv$/', $name)){

						$tableName = $this->_safeString($name);

						$pointer = tmpfile();
						fwrite($pointer, $zip->getFromIndex($index));

						// READ ONE BY ONE
						fseek($pointer, 0);

						// FIND SEPARATOR LINE BY LINE
						if($this->readSeparator == '2'){
							$delimiter = $this->_findDelimiter($pointer);
						}

						fseek($pointer, 0);

						$ddls = $this->_readCsvStructure($pointer, $delimiter, $ddls, function($currentLine, $key, $ddls) use ($tableName){

							// HEADER
							if($key == 0){

								// CREATE COLUMNS
								foreach ($currentLine as $colName){

									$ddls[$tableName][] = [
										'column' => $this->_safeString($colName),
										'type' => 'undefined'
									];
								}

								return $ddls;
							}

							// VALUES
							if($key >= 1){

								// IDENTIFICA TIPOS DE DADOS
								// DETECT DATA TYPES
								foreach($currentLine as $colIndex => $value){

									// TRIM ON VALUE
									$value = trim($value ?? '', " \n\t\t");

									// IF STRING IS EMPTY, FORCE TO NULL
									if($value === ''){
										$value = null;
									}

									// IF NULL SKIP LOOP AND CONTINUE
									if(is_null($value)){
										continue;
									}

									// IGNORE TOTALS COLUMNS INDEX MORE THAN HEADER INDEX TOTAL
									if(($colIndex + 1) > count($ddls[$tableName])){
										continue;
									}

									// ERROR IN FILE
									if(!isset($ddls[$tableName], $ddls[$tableName][$colIndex], $ddls[$tableName][$colIndex]['type'])){
										throw new CsvToPgsqlFileException('CSV '.$tableName.' with parse error: '.$ddls[$tableName][$colIndex]);
									}

									// IF VALUE WAS DETECTED AS STRING, SKIP LOOP AND CONTINUE
									if($ddls[$tableName][$colIndex]['type'] == 'text'){
										continue;
									}

									$detected = $this->_detectDataType($value);

									// SPECIAL CONDITION TO BIGINT
									if($detected == 'integer' and ($ddls[$tableName][$colIndex]['type'] == 'undefined' or $ddls[$tableName][$colIndex]['type'] == 'bigint')){
										$detected = 'bigint';
									}

									// SPECIAL CONDITION TO BIGINT
									if($detected == 'bigint' and ($ddls[$tableName][$colIndex]['type'] == 'undefined' or $ddls[$tableName][$colIndex]['type'] == 'integer')){
										$detected = 'bigint';
									}

									// SPECIAL CONDITION TO NUMERIC
									if($detected == 'numeric' and ($ddls[$tableName][$colIndex]['type'] == 'undefined' or $ddls[$tableName][$colIndex]['type'] == 'integer')){
										$detected = 'numeric';
									}

									// SPECIAL CONDITION TO NUMERIC
									if($detected == 'integer' and ($ddls[$tableName][$colIndex]['type'] == 'undefined' or $ddls[$tableName][$colIndex]['type'] == 'numeric')){
										$detected = 'numeric';
									}

									// DETECT DATA TYPES
									$ddls[$tableName][$colIndex]['type'] = $detected;
								}

								return $ddls;
							}
						});

						// CREATE TABLE
						if(isset($ddls[$tableName])){

							$this->_createTable($tableName, $ddls[$tableName]);
						}

						fclose($pointer);
					}

				} catch (CsvToPgsqlFileException $e){

					print 'Skip file '.$name.' reading error'.PHP_EOL;
				}
			}

			// INSERT DATA
			if(!$this->justCreateTables){

				print 'Start to insert data...'.PHP_EOL;

				foreach($files as $name => $index){

					if(preg_match('/.csv$/', $name)){

						$tableName = $this->_safeString($name);

						$pointer = tmpfile();
						fwrite($pointer, $zip->getFromIndex($index));

						// READ ONE BY ONE
						fseek($pointer, 0);

						// FIND SEPARATOR LINE BY LINE
						if($this->readSeparator == '2'){
							$delimiter = $this->_findDelimiter($pointer);
						}

						fseek($pointer, 0);
						$this->_readCsvAsArray($pointer, $delimiter, $tableName, $ddls, function($tableName, $ddls, $currentLine){

							// INSERT DATA ON TABLE
							$this->_prepareInsert($tableName, $ddls, $currentLine);

							$this->_insert($inLoop = true);
						});

						fclose($pointer);

						$this->_insert($inLoop = false);
					}

					// INSERT RESET TO A NEW TABLE
					$this->_colsHead = false;
					$this->_insertQuery = [];

					print 'Insert into table '.$tableName.' was done'.PHP_EOL;
				}
			}

			// MAKE COMMIT
			if($this->enableTransaction and $this->_pdo->inTransaction()){
				$this->_pdo->commit();
			}

		} catch (CsvToPgsqlException $e){

			// MAKE ROLLBACK
			if( $this->_pdo and $this->enableTransaction and $this->_pdo->inTransaction()){
				$this->_pdo->rollBack();
			}

			return [
				'result' => false,
				'message' => $e->getMessage()
			];
		}

		return [
			'result' => true,
			'message' => 'Convertido com sucesso!'
		];
	}
}