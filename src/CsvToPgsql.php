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
	protected object|bool $_pdo = false;

	/**
	 * PDO connection config, user, pass, db, schema, port, ip
	 *
	 * @var array
	 */
	protected array $_dbConnection = [];

	/**
	 * ZIP temp_file()
	 *
	 */
	protected $_tempZipFile;

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

    /**
     * Set a config.
     *
     * @param  string  $param
     * @param  bool|string  $value
     * @param  array  $options
     * @return object $this
     */
	public function setConfig(string $param, bool|string $value) :object
	{
		// VALIDATE BOOL PARAMS
		if(is_bool($value) and in_array($param, ['createPkey', 'createPkey', 'enableTransaction', 'justCreateTables'])){

			$this->{$param} = $value;
		}

		// VALIDATE STRING PARAMS
		if(is_string($value) and in_array($param, ['inputEncoding', 'outputEncoding'])){
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
	public function setConfigs(array $configs) :object
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
	protected function _connectPgsql(array $dbConnection) :object
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
	protected function _createSchema() :object{

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
	protected function _findDelimiter(string $string) :string{

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

	// CONVERTE CSV TO ARRAY, FAZENDO OS DEVIDOS TRATAMENTOS
	protected function _readCsvAsArray(string $name, string $csv) :array{

		$arr = [];

		// fgetcsv PRECISA DE UM ARQUIVO EM DISCO, ENTÃO GRAVAR EM UM ARQUIVO TEMPORÁRIO
		$tempCsv = tmpfile();
		fwrite($tempCsv, $csv);
		fseek($tempCsv, 0);

		// FIRST 8192 BYTES TO FIND DELIMITER
		$delimiter = substr($csv, 0, 8192);
		$delimiter = $this->_findDelimiter($delimiter);

		while (($line = fgetcsv($tempCsv, 2048, $delimiter)) !== false){

			// SKIT EMPTY LINES ON CSV FILE
			if(count($line) > 0){

				// FAZ TRIM
				foreach($line as $k => $v){

					// CONVERT ENCODING
					if($this->outputEncoding != $this->inputEncoding){
						$v = mb_convert_encoding($v, $this->outputEncoding, $this->inputEncoding);
					}

					// TRIM
					if($this->enableTrim){
						$v = trim($v, " \n\r\t");
					}

					$line[$k] = $v;
				}
				$arr[] = $line;
			}
		}

		fclose($tempCsv);

		return $arr;
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

		// REMOVE A EXTENSÃO
		$string = preg_replace('/[^a-z]+/', '_', $string);

		$string = trim($string, '_');

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

		// IF MORE THAN 2147483647, SKIP, IT WILL BE A STRING
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
	protected function _createTable($name, $columns) :void{

		$table = $this->_safeString($name);

		$headCols = array_values($columns[0]);

		// CREATE COLUMNS
		$ddls = [];
		foreach ($headCols as $colName) {
			$ddls[] = [
				'column' => $this->_safeString($colName),
				'type' => 'undefined'
			];
		}

		// IDENTIFICA TIPOS DE DADOS
		foreach ($columns as $key => $cols) {

			// PULA CABEÇALHOS
			if($key == 0){
				continue;
			}

			// DETECT DATA TYPES
			foreach($cols as $colIndex => $value){

				// TRIM ON VALUE
				$value = trim($value, " \n\t\t");

				// IF STRING IS EMPTY, FORCE TO NULL
				if($value === ''){
					$value = null;
				}

				// IF NULL SKIP LOOP AND CONTINUE
				if(is_null($value)){
					continue;
				}

				// IF VALUE WAS DETECTED AS STRING, SKIP LOOP AND CONTINUE
				if($ddls[$colIndex]['type'] == 'text'){
					continue;
				}

				// DETECT DATA TYPES
				$ddls[$colIndex]['type'] = $this->_detectDataType($value);
			}
		}

		$ddlCols = [];

		if($this->createPkey){
			$ddlCols[] = '_pkey_ serial NOT NULL';
		}

		foreach($ddls as $key => $col){

			if($col['type'] == 'timestamp'){

				$ddlCols[] = '"'.$col['column'].'" timestamp NULL';

			}else if($col['type'] == 'integer'){

				$ddlCols[] = '"'.$col['column'].'" int4 NULL';

			}else if($col['type'] == 'numeric'){

				$ddlCols[] = '"'.$col['column'].'" numeric(14,8) NULL';

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
			$ddlCols[] =  'CONSTRAINT '.$table.'_pkey PRIMARY KEY (_pkey_)';
		}

		$ddls = implode(','.PHP_EOL."\t", $ddlCols);

		$ddl = <<<ddl
CREATE TABLE IF NOT EXISTS {$this->_dbConnection['DB_SCHEMA']}.{$table} (
	$ddls
);
ddl;

		try{

			$this->_pdo->query($ddl);

		} catch (\PDOException $e){

			throw new CsvToPgsqlException($e->getMessage());
		}
	}

	// INSERE DADOS NA TABELA
	protected function _insert($name, $columns) :void{

		$table = $this->_safeString($name);

		$headCols = array_values($columns[0]);

		// CRIA COLUNAS
		$colsHead = [];
		foreach ($headCols as $colName) {

			$colName = trim($colName, " \n\r\t");

			$colsHead[] = $this->_safeString($colName);
		}

		// PREPARA DADOS
		$data = [];
		foreach ($columns as $key => $cols) {

			// PULA CABEÇALHOS
			if($key == 0){
				continue;
			}

			// IGNORA LINHAS VAZIAS DO FIM DO CSV
			if(count($colsHead) > 1 and count($cols) == 1){
				continue;
			}

			$temp = [];
			foreach($colsHead as $key => $head){

				$value = null;
				if(isset($cols[$key]) and $cols[$key] !== ''){
					$value = $cols[$key];
				}

				if($value === ''){
					$value = null;
				}

				$temp[$head] = $value;
			}

			$data[] = $temp;
		}

		if(count($data) > 0){

			$pre = $data[0];

			$cols = [];
			$vals = [];
			foreach(array_keys($pre) as $col){

				$cols[] = '"'.$col.'"';
				$vals[] = ':'.$col;
			}

			$cols = implode(',', $cols);
			$vals = implode(',', $vals);

			// CREATE SQL
			$sql = "INSERT INTO ".$this->_dbConnection['DB_SCHEMA'].".$table ($cols) VALUES ($vals);".PHP_EOL;

			$query = $this->_pdo->prepare($sql);

			try{

				foreach($data as $line){
					$query->execute($line);
				}

			} catch (\PDOException $e){

				throw new CsvToPgsqlException($e->getMessage());
			}
		}
	}

	// MÉTODO PRINCIPAL PARA A CONVERSÃO
	public function convertCsvFromZip(string $zipUrl, bool|array $dbConnection = false) :array{

		try {

			$files = $this->_readZip($zipUrl);

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

			// CREATE TABLES
			foreach($files as $name => $index){

				if(preg_match('/.csv$/', $name)){

					$currentFile = $zip->getFromIndex($index);

					// LÊ UM POR UM
					$columns = $this->_readCsvAsArray($name, $currentFile);

					// CSV IS EMPTY
					if(!$columns or count($columns) == 0){
						continue;
					}
					// CREATE TABLE
					$this->_createTable($name, $columns);
				}
			}

			// INSERT DATA
			if(!$this->justCreateTables){

				foreach($files as $name => $index){

					if(preg_match('/.csv$/', $name)){

						$currentFile = $zip->getFromIndex($index);

						// LÊ UM POR UM
						$columns = $this->_readCsvAsArray($name, $currentFile);

						// CSV IS EMPTY
						if(!$columns or count($columns) == 0){
							continue;
						}

						// INSERT DATA ON TABLE
						$this->_insert($name, $columns);
					}
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