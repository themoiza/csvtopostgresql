<?php

namespace TheMoiza\Csvtopostgresql;

class CsvToPgsql{

	public $pdo;

	protected $_config;

	// PONTEIRO PARA O ZIP EM ARQUIVO TEMPORÁRIO
	protected $_pointerZip;

	function __construct($config){

		$this->_config = $config;

		try{

			$dsn = $config['DB_CONNECTION']??'pgsql'.':host='.$config['DB_HOST'].';port='.$config['DB_PORT'].';dbname='.$config['DB_DATABASE'];
			$this->pdo = new \PDO($dsn, $config['DB_USERNAME'], $config['DB_PASSWORD'], []);
			$this->pdo->setAttribute(\PDO::ATTR_EMULATE_PREPARES, 1);

		}catch(\PDOException $e){

			die($e->getMessage());
		}
	}

	// REMOVE ACENTOS E Ç
	public static function removeAcentCedil($string) :string{

		$string = str_replace(
			['À','È','Ì','Ò','Ù','Ã','Ẽ','Ĩ','Õ','Ũ','Â','Ê','Î','Ô','Û','Á','É','Í','Ó','Ú','à','è','ì','ò','ù','ã','ẽ','ĩ','õ','ũ','â','ê','î','ô','û','á','é','í','ó','ú','ç','ĺ'],
			['a','e','i','o','u','a','e','i','o','u','a','e','i','o','u','a','e','i','o','u','a','e','i','o','u','a','e','i','o','u','a','e','i','o','u','a','e','i','o','u','c','l'],
			$string
		);

		return $string;
	}

	// CRIA SCHEMA SE AINDA NÃO EXISTOR
	public function verifySchema() :object{

		try{

			$this->pdo->query('CREATE SCHEMA IF NOT EXISTS '.$this->_config['DB_SCHEMA']);

		} catch (\Exception $e){

			die($e->getMessage());
		}

		return $this;
	}

	// CARREGA ZIP E SALVA EM UM ARQUIVO TEMPORÁRIO
	protected function _loadZip($zipUrl) :array{

		$this->_pointerZip = tmpfile();
		fwrite($this->_pointerZip, file_get_contents($zipUrl));
		fseek($this->_pointerZip, 0);

		$zip = new \ZipArchive;
		$zip->open(stream_get_meta_data($this->_pointerZip)['uri']);

		$extracted = [];

		for ($i = 0; $i < $zip->numFiles; $i++){

			$fileInZip = $zip->statIndex($i);

			$extracted[$fileInZip['name']] = $i;
		}

		return $extracted;
	}

	// DETERMINA O DELIMITADOR DO CSV
	protected function _delimitador($string) :string{

		if(!empty($string)){

			$string = substr($string, 0, 10000);

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
	protected function _readCsvToArray($name, $bin) :array{

		// DETECTA DELIMITADOR
		$delimitador = $this->_delimitador($bin);

		// fgetcsv PRECISA DE UM ARQUIVO EM DISCO, ENTÃO GRAVAR EM UM ARQUIVO TEMPORÁRIO
		$tempCsv = tmpfile();
		fwrite($tempCsv, $bin);
		fseek($tempCsv, 0);

		$arr = [];
		while (($line = fgetcsv($tempCsv, null, $delimitador)) !== FALSE){

			// IGNORA LINHAS VAZIAS DO CSV
			if(count($line) > 0){

				// FAZ TRIM
				foreach($line as $k => $v){

					// UTF8 PARA LATIN
					$v = mb_convert_encoding($v, 'ISO-8859-1', 'UTF-8');
					$line[$k] = trim($v, ' ');
				}
				$arr[] = $line;
			}
		}

		fclose($tempCsv);

		return $arr;
	}

	// PROCESSA NOME DO ARQUIVO CSV PARA UM NOME ACEITO
	protected function _safeTablename($name) :string{

		// ALGUNS AQUIVOS VEM file..csv
		$name = str_replace('..', '.', $name);

		// REMOVE A EXTENSÃO
		$name = preg_replace('/.csv$/', '', $name);

		// REMOVE ACENTOS DE Ç
		$name = $this->removeAcentCedil($name);

		// TUDO PARA MINÚSCULO
		$name = strtolower($name);

		// ALTERA ESPAÇOS E HIFEN PARA UNDERLINE
		$name = str_replace([' ', '-'], '_', $name);

		return $name;
	}

	// CRIA A TABELA CASO ELA AINDA NÃO EXISTA
	protected function _verifyTable($name, $columns) :void{

		$table = $this->_safeTablename($name);

		$headCols = array_values($columns[0]);

		// CRIA COLUNAS
		$ddls = [];
		foreach ($headCols as $colName) {
			$ddls[] = [
				'column' => strtolower($colName),
				'type' => 'integer'
			];
		}

		// IDENTIFICA TIPOS DE DADOS
		foreach ($columns as $key => $cols) {

			// PULA CABEÇALHOS
			if($key == 0){
				continue;
			}

			foreach($cols as $colIndex => $value){

				$value = trim($value, ' ');

				// STRING VAZIA FORÇA PARA NULO
				if($value === ''){
					$value = null;
				}

				// SE FOR NULL, PULAR LOOP E CONTINUAR
				if(is_null($value)){
					continue;
				}

				// SE JÁ FOI DETECTADO COMO STRING, PULAR LOOP E CONTINUAR
				if($ddls[$colIndex]['type'] == 'string'){
					continue;
				}

				if(preg_match('/^[0-9]{4}\-[0-9]{2}\-[0-9]{2} [0-9]{2}\:[0-9]{2}\:[0-9]{2}$/', $value)){

					$ddls[$colIndex]['type'] = 'timestamp';
					
				}else if(preg_match('/^[0-9]{4}\-[0-9]{2}\-[0-9]{2}$/', $value)){
					
					$ddls[$colIndex]['type'] = 'date';

				// SE FOR MAIOR QUE 131072, PULAR, POIS SERÁ STRING
				}else if(preg_match('/^[0-9]+[.]{1}[0-9]+$|^-[0-9]+[.]{1}[0-9]+$/', $value) and $value <= 131072){

					$ddls[$colIndex]['type'] = 'numeric';

				// SE FOR MAIOR QUE 2147483647, PULAR, POIS SERÁ STRING
				}else if(preg_match('/^[0-9]+$/', $value) and $value <= 2147483647){

					$ddls[$colIndex]['type'] = 'integer';

				// BOOLEAN
				}else if(preg_match('/^[tf]+$/', $value) and strlen($value) == 1){

					$ddls[$colIndex]['type'] = 'boolean';

				}else{

					$ddls[$colIndex]['type'] = 'string';
				}
			}
		}

		$ddlCols = [];
		$pKey = $ddls[0]['column'];
		foreach($ddls as $key => $col){

			if($col['type'] == 'integer'){

				$ddlCols[] = '"'.$col['column'].'" int4 NULL';

			}else if($col['type'] == 'numeric'){

				$ddlCols[] = '"'.$col['column'].'" numeric(14,8) NULL';

			}else if($col['type'] == 'date'){

				$ddlCols[] = '"'.$col['column'].'" date NULL';

			}else if($col['type'] == 'boolean'){

				$ddlCols[] = '"'.$col['column'].'" boolean NULL';

			}else{

				$ddlCols[] = '"'.$col['column'].'" text NULL';
			}
		}

		$colType = implode(','.PHP_EOL."\t", $ddlCols);

		$ddl = <<<ddl
CREATE TABLE IF NOT EXISTS {$this->_config['DB_SCHEMA']}.{$table} (
	$colType
);
ddl;

		try{

			$this->pdo->query($ddl);

		} catch (\Exception $e){

			echo $e->getMessage().PHP_EOL;
			print_r($ddl);
			exit;
		}
	}

	// INSERE DADOS NA TABELA
	protected function _insert($name, $columns) :void{

		$table = $this->_safeTablename($name);

		$headCols = array_values($columns[0]);

		// CRIA COLUNAS
		$colsHead = [];
		foreach ($headCols as $colName) {
			$colsHead[] = strtolower($colName);
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

				$value = 'NULL';
				if($cols[$key] !== ''){

					// ESCAPA ASPAS SIMPLES
					$value = 'E\''.str_replace("'", "\'", $cols[$key]).'\'';
				}

				$value = trim($value, ' ');

				$temp['"'.$head.'"'] = $value;
			}

			$data[] = $temp;
		}

		if(count($data) > 0){

			$sql = '';
			foreach($data as $line){

				$cols = implode(',', array_keys($line));
				$vals = implode(',', array_values($line));

				$sql .= "INSERT INTO ".$this->_config['DB_SCHEMA'].".$table ($cols) VALUES ($vals);".PHP_EOL;
			}

			try{

				$this->pdo->query($sql);

			} catch (\Exception $e){

				echo $table.' => '.$e->getMessage().PHP_EOL;
				exit;
			}
		}
	}

	// MÉTODO PRINCIPAL PARA A CONVERSÃO
	public function convertFromZip($zipUrl) :void{

		$files = $this->_loadZip($zipUrl);

		foreach($files as $name => $index){

			if(preg_match('/.csv$/', $name)){

				$zip = new \ZipArchive;
				$zip->open(stream_get_meta_data($this->_pointerZip)['uri']);

				$bin = $zip->getFromIndex($index);

				// LÊ UM POR UM
				$columns = $this->_readCsvToArray($name, $bin);
				$this->_verifyTable($name, $columns);

				$this->_insert($name, $columns);
			}
		}

		echo 'Convertido com sucesso!';
	}
}