<?php

/**
 * SimpleDB
 * https://github.com/Vorticode/SimpleDB
 * @author Eric Poggel
 * @license MIT
 *
 * SimpleDB is a thin wrapper around PHP's PDO library, to automate away creating connections, preparing statements,
 * iterating over results, converting types, and setting up transactions.
 * It's a one-database-at-a-time class, a deliberate choice to keep it
 * simple enough to be implemented as a set of static functions.
 * It supports MySQL, MariaDB, and SQLite.
 *
 * @example
 * // Connect to MySQL or MariaDB
 * $GLOBALS['SimpleDB'] = [
 *    'url'=>'mysql:host=localhost;dbname=test;charset=utf8mb4',
 *    'user'=>'root',
 *    'password'=> 'sql',
 *    'options'=> [ // Optional options array to pass to PDO:
 *        PDO::ATTR_PERSISTENT => false,
 *        PDO::MYSQL_ATTR_FOUND_ROWS => true
 *    ]
 * ];
 *
 * // Connect to SQLite:
 * $GLOBALS['SimpleDB'] = 'sqlite:path/to/db.sqlite3';
 *
 * // Get multiple rows, passing an argument:
 * // Database connection doesn't occur until this line.
 * $rows = SimpleDB::getRows("SELECT name, email FROM users WHERE type=?", ['noob']);
 * foreach ($rows as $row)
 *     print "Hello $row->name!  Your email is $row->email!";
 *
 * // Run arbitrary queries:
 * SimpleDB::execute("DELETE FROM users");
 *
 * // Insert new row:
 * $user = ['name'=>'Fred', 'email'=>'fred@fred.com'];
 * SimpleDB::save('users', $user);
 * print $user['id']; // auto-increment id is set automatically.
 *
 * // Update existing row when primary key(s) are set:
 * SimpleDB::save('users', ['id'=>3, 'name'=>'Fred', 'email'=>'fred@fred.com']);
 *
 *
 * TODO:
 * Write tests!
 *
 * I can use $stm->getColumnMeta() instead of $description for type conversions?
 *     MYSQLI_OPT_INT_AND_FLOAT_NATIVE or something similar for PDO to get int and float back for query result columns, instead of strings.
 *     This would prevent needing to call describe() just to return query results.
 * createList() and converting values for WHERE a in (b,c,d) queries?
 * Extend PDOStatement here to access executionTime and returned rows?
 *     https://www.php.net/manual/en/class.pdostatement.php
 * Support more database types.
 *
 */
class SimpleDB {

	// Before I disabled statement reuse inside transactions, this would occasionally cause
	// "General error: 21 bad parameter or other API misuse" when multiple queries happen simultaneously.
	const ReuseStatements = true;
	const DefaultLockTimeout = 30;
	const MaxTransactionDepth = 10;
	const TransactionRetryWait = 0.005;


	/** @type PDO */
	static $connection = null;

	protected static $driver = null; // Will be 'mysql' or 'sqlite'
	protected static $tables = [];    // Table descriptions, populated as they are queried.
	protected static $lastQuery = [];
	protected static $transactionDepth = 0;

	/** @type PDOStatement[] Cache of PDO statements.  Indexed by the sql string that generated them. */
	protected static $statements = [];
	protected static $statementsStack = []; // We can't re-use statements from one transaction to another.
	//protected static $statementsInUse = []; // Used only for debugging



	/**
	 * This typically doesn't need to be called manually because
	 * it's done automatically when the first query happens.
	 * Use setConnection() for setting the connection parameters. */
	static function connect() {

		// Do nothing if already connected.
		if (static::$connection)
			return;


		$config = static::getConfig();

		static::$connection = new PDO($config['url'], $config['user']??null, $config['password']??null, $config['options']??null);
		static::$connection->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

		// Unless we set these, int and float values come back as strings - stackoverflow.com/a/20123337
		static::$connection->setAttribute(PDO::ATTR_STRINGIFY_FETCHES, false);
		//static::$connection->setAttribute(PDO::ATTR_EMULATE_PREPARES, false); // causes occasional "prepared statement" something error on inmotion.

		static::$driver = static::$connection->getAttribute(PDO::ATTR_DRIVER_NAME);

		if (static::$driver==='sqlite') // https://dba.stackexchange.com/a/94251
			static::$connection->prepare("PRAGMA foreign_keys = ON")->execute();
	}

	static function disconnect() {
		static::$connection = null; // Php Docs say this is necessary to close existing connection.
		static::$tables = [];
		static::$driver = null;
		static::$lastQuery = [];
		foreach (static::$statements as $statement)
			if ($statement)
				$statement->closeCursor();
		static::$statements = [];
		//static::$statementsInUse = []; // Useful for debugging only.
	}



	/**
	 * Get code for a php class that maps to a row in $tableName.
	 * @param string $tableName
	 * @param string|null $className
	 * @param bool $typeComments If true, append a comment to each field with the original database type.
	 * @return string
	 * @throws SimpleDBException */
	static function getPhpClassCode(string $tableName, ?string $className=null, $typeComments=true): string {
		if (!$className)
			$className = str_replace(' ', '', ucwords(str_replace('_', ' ', $tableName)));
		$types = [
			'Bool'=>'bool',
			'Int'=>'int',
			'Float'=>'float',
			'Date'=>'DateTime',
			'Enum'=>'string',
			'Text'=>'string'
		];

		$desc = static::describe($tableName);

		$result = "// This class was generated automatically from the $tableName database table:\r\nclass $className {\r\n";

		$fields = [];
		foreach ($desc as $col) {
			$type = $types[$col->type];
			if ($col->isNull || $col->isAutoIncrement)
				$type = '?' . $type;

			$fieldCode = "\tpublic $type \$$col->name";
			if (($col->isNull || isset($col->default)) && $col->default !== 'current_timestamp()')
				$fieldCode .= '=' . json_encode($col->default);
			$fieldCode .= ";";
			$fields []= $fieldCode;
		}

		if ($typeComments) {
			$maxLen = 0;
			foreach ($fields as $field)
				if (strlen($field) > $maxLen)
					$maxLen = strlen($field);

			foreach (array_values($desc) as $i=>$col) {
				$fields[$i] = str_pad($fields[$i], $maxLen) . " // $col->originalType "
					. ($col->isNull ? 'NULL' : 'NOT NULL')
					. ($col->isPrimary ? ' PRIMARY' : '')
					. ($col->isAutoIncrement ? ' AUTO_INCREMENT' : '');
			}
		}

		return $result . implode("\r\n", $fields) . "\r\n}";
	}

	static function getDriver(): ?string {
		if (static::$driver)
			return static::$driver;
		if (!empty(static::getConfig()['url'])) // If we're not connected yet, get it from the config.
			return substr(static::getConfig()['url'] ?? static::getConfig(), 0, 5) === 'mysql' ? 'mysql': 'sqlite';
		return null;
	}

	/**
	 * Get a database-agnostic description of all columns in a database table.
	 * @param string|null $table Test
	 * @param bool $force
	 * @return SimpleDBColumn[] Key is lower case version of the column name.
	 * @throws SimpleDBException|PDOException */
	static function describe(string $table=null, bool $force=false): array {
		static::connect();

		// Check cache for a result.  Cache is resete after each page load.
		if (!$force && array_key_exists($table, static::$tables))
			return static::$tables[$table];

		$result = [];



		/**
		 * Table descriptions are different for each database. */
		if (static::getDriver() === 'sqlite') {
			$statement = static::$connection->prepare("PRAGMA table_info('" . $table . "')");
			$statement->execute();
			$columns = $statement->fetchAll(PDO::FETCH_OBJ);
			if (!count($columns))
				throw new SimpleDBException("Table \"$table\" doen't exist.");

			$types = array ( // Convert MySQL types to a simpler enum of types
				SimpleDBColType::INT  => 'INT|INTEGER|TINYINT|SMALLINT|MEDIUMINT|BIGINT|UNSIGNED BIG INT|INT2|INT8|BOOLEAN|DATE|DATETIME',
				SimpleDBColType::FLOAT  => 'REAL|DOUBLE|DOUBLE PRECISION|FLOAT|NUMERIC|DECIMAL.*',
				//ColType::DATE    => 'DATE|DATETIME',
				SimpleDBColType::TEXT    => '.' // everything else, including blobs
			);

			// Set the description of each column
			foreach ($columns as $row) {
				$col = new SimpleDBColumn();
				$col->name = $row->name;
				$col->default = $row->dflt_value;
				$col->isPrimary = $row->pk === '1';
				$col->originalType = $row->type;

				// Detect if column is auto-increment:
				if ($col->isPrimary) {
					if ($row->type==='INTEGER') // "a column with type INTEGER PRIMARY KEY is an alias for the ROWID"
						$col->isAutoIncrement = true; // http://sqlite.org/autoinc.html
					else {

						// Make sure sqllite_sequenec table exists.  If it doesn't then the database has no auto-inc columns in any tables.
						$stm1 = static::$connection->prepare(
							"SELECT count(*) AS c FROM sqlite_master WHERE type='table' AND name='sqlite_sequence'");
						$stm1->execute();

						$count = $stm1->fetchAll()[0]['c'];
						if ($count) {
							/** @noinspection SqlResolve */
							$st2 = static::$connection->prepare(
								"SELECT COUNT(*) as c FROM sqlite_sequence WHERE name='$table';");
							$st2->execute();
							$col->isAutoIncrement = $st2->fetchAll(PDO::FETCH_OBJ)[0]->c === '1';
						}
						else
							$col->isAutoIncrement = false;
					}
				}

				$col->isNull = $row->notnull === '0';
				foreach ($types as $phpType=>$pattern)
					if (preg_match('/'.$pattern.'/i', $row->type)) {
						$col->type = $phpType;
						break;
					}
				$result[strtolower($col->name)] = $col;
			}
		}
		elseif (static::getDriver() === 'mysql') {
			$statement =  static::$connection->prepare("DESCRIBE " . $table);

			$statement->execute(); // Throws PDOException if table doesn't exit.
			$columns = $statement->fetchAll(PDO::FETCH_OBJ);

			$types = array ( // Convert MySQL types to a simpler enum of types
				SimpleDBColType::BOOL 	 => 'tinyint\(1\)|bool|boolean|bit|int1',
				SimpleDBColType::INT    =>  'tinyint|smallint|mediumint|int[\((0-9)+\)]?|bigint|int4|int8|middleint|timestamp|year',
				SimpleDBColType::FLOAT  =>  'float[\((0-9)+\)]?|double[\((0-9)+\)]?|decimal[\((0-9)+\)]?|numeric[\((0-9)+\)]?',
				SimpleDBColType::DATE    => 'datetime|date|time',
				SimpleDBColType::ENUM    => 'enum\([^\)]*\)',
				SimpleDBColType::TEXT    => '.' // everything else, including blobs
			);

			// Set the description of each column
			foreach ($columns as $row) {
				$col = new SimpleDBColumn();
				$col->name = $row->Field;
				$col->isPrimary = $row->Key=='PRI';
				$col->isAutoIncrement = $row->Extra == 'auto_increment';
				$col->isNull = $row->Null=='YES';
				$col->default = $row->Default;
				$col->originalType = $row->Type;

				// Get the type
				$mysqlType = $row->Type;
				foreach ($types as $phpType=>$pattern)
					if (preg_match('/'.$pattern.'/i', $mysqlType)) {
						$col->type = $phpType;
						break;
					}
				$result[strtolower($col->name)] = $col;
			}
		}
		else
			throw new SimpleDBException('Unsuported database'); // unsupported

		// SQLite will return an empty array for non-existant table.
		static::$tables[$table] = $result;
		return $result;

	}

	/**
	 * @param string $sql
	 * @param array|string $params An array of parameters, or a single value if only one parameter is needed.
	 * @throws SimpleDBException */
	static function execute(string $sql, $params=null) {
		$statement = static::internalExecute($sql, $params);
		$statement->closeCursor();
	}

	static function getLastQuery(): string {
		if (!count(static::$lastQuery))
			return '';

		$result = '"' . static::$lastQuery[0] . '" ';
		if (isset(static::$lastQuery[1]) && static::$lastQuery[1] !== null) {
			// 512 is JSON_INVALID_UTF8_SUBSTITUTE, first defined in PHP 7.2:
			$result .= json_encode(static::$lastQuery[1], 512 | JSON_PARTIAL_OUTPUT_ON_ERROR);
		}
		return $result;
	}

	static function getRows(string $sql, $params=null, $Type=null): array {

		// The code below is inlined for performance, but is equivalent to:
		// return static::getCursor($sql, $params, $Type)->toArray();
		$statement = static::internalExecute($sql, $params);
		if (is_array($Type))
			$statement->setFetchMode(PDO::FETCH_ASSOC);
		elseif ($Type)
			$statement->setFetchMode(PDO::FETCH_CLASS | PDO::FETCH_PROPS_LATE, $Type);
		else
			$statement->setFetchMode(PDO::FETCH_OBJ);
		$result = [];
		while ($row = $statement->fetch())
			$result[] = $row;

		$statement->closeCursor();
		return $result;
	}

	/**
	 * @param string $sql
	 * @param null $params
	 * @param null $Type
	 * @return mixed|null
	 * @throws SimpleDBException */
	static function getRow(string $sql, $params=null, $Type=null) {

		// The code below is inlined for performance, but is roughly equivalent to:
		// return static::getCursor($sql, $params, $Type)->toArray()[0];
		$statement = static::internalExecute($sql, $params);

		if (is_array($Type))
			$statement->setFetchMode(PDO::FETCH_ASSOC);
		elseif ($Type)
			$statement->setFetchMode(PDO::FETCH_CLASS | PDO::FETCH_PROPS_LATE, $Type);
		else
			$statement->setFetchMode(PDO::FETCH_OBJ);

		if ($row = $statement->fetch()) {
			$statement->closeCursor();
			return $row;
		}

		return null;
	}

	/**
	 * @param string $sql
	 * @param null $params
	 * @return mixed|null
	 * @throws SimpleDBException */
	static function getOne(string $sql, $params=null) {
		$statement = static::internalExecute($sql, $params);
		if ($row = $statement->fetch()) {
			$statement->closeCursor();
			return reset($row);
		}
		return null;
	}



	static function getColumn(string $sql, $params=null): array {
		// TOOD:  Use PDO::FETCH_COLUMN ?
		$result = [];
		$rows = static::getCursor($sql, $params)->toArray();
		foreach ($rows as $row)
			$result[]= reset($row); // gets first item.

		return $result;
	}


	/**
	 * Similar to getRows() except it returns a cursor to stream/iterate over the rows.
	 * If you want the returned
	 * @param string $sql
	 * @param array|string $params An array of parameters, or a single value if only one parameter is needed.
	 * @param string|array= $Type Class name or an empty array().  Each result row will be cast to this type.
	 * @return SimpleDBResultSet A buffered result-set.  Each row is loaded as it is iterated, but it can be iterated only once.
	 * @throws SimpleDBException */
	static function getCursor(string $sql, $params=null, $Type=null) : SimpleDBResultSet {
		$statement = static::internalExecute($sql, $params);
		static::statementCheckout($sql); // Make sure
		return new SimpleDBResultSet($statement, $sql, $Type);
	}

	/**
	 * @param string $sql
	 * @param array $params
	 * @param string $keyColumn Defaults to first column if not set.
	 * @param string $TypeOrValueColumn
	 *     If not set, the value will be the whole row.
	 *     If a column name, the value will be that column.
	 *     If a class name, the whole row will be cast to that type of class. (Not supported yet)
	 * @return object
	 * @throws SimpleDBException */
	static function getRowsAsObject(string $sql, $params=null, $keyColumn=null, $TypeOrValueColumn=null) {
		return (object)static::getRowsByKey($sql, $params, $keyColumn, $TypeOrValueColumn);
	}

	/**
	 * @param string $sql
	 * @param null $params
	 * @param null $keyColumn
	 * @param null $TypeOrValueColumn
	 * @return array
	 * @throws SimpleDBException */
	static function getRowsByKey(string $sql, $params=null, $keyColumn=null, $TypeOrValueColumn=null): array {
		$result = [];
		$rows = static::getRows($sql, $params);
		foreach ($rows as $row) {
			$firstKey = key($row);
			$key = $keyColumn !== null ? $row->$keyColumn : $row->$firstKey;

			if (isset($row->$TypeOrValueColumn))
				$value = $row->$TypeOrValueColumn;
//			else if (class_exists($row->$TypeOrValueColumn))
//				$value = (object)$row;
			else
				$value = $row;

			$result[$key] = $value;
		}
		return $result;
	}

	/**
	 * 1. Find all rows that match the $params associative array.
	 * 2. Cast each row to $Type, or a generic object if $type is not specified.
	 * 3. Cast the values of each row according to the types in the table description.
	 * @param string $table
	 * @param array[string]= $params
	 * @param ?string|array $Type The type used for each result row.  Can be a class name, an empty array(), or null to make it a stdClass.
	 * @param int= $start
	 * @param int= $limit
	 * @throws SimpleDBException */
	static function findCursor(string $table, array $params=null, $Type=null, $start=0, $limit=null): SimpleDBResultSet {
		$q = static::getDriver() === 'mysql' ? '`' : "'";

		// Build the query
		$sql = "SELECT * FROM $q$table$q"; // TODO: Different quotes for MySQL
		if ($params)
			$sql .= ' WHERE ' . static::buildWhere($params);

		if ($limit !== null && $start !== 0)
			$sql .= " LIMIT $start, $limit ";
		else if ($limit !== null)
			$sql .= " LIMIT $limit ";
		else if ($start > 0)
			$sql .= " LIMIT $start, 1844674407370955161 "; // Max BigInt, http://dev.mysql.com/doc/refman/5.7/en/select.html

		// Execute the query
		return static::getCursor($sql, $params, $Type);
	}

	/**
	 * Find the first row that matches the $params associative array.
	 * @param string $table
	 * @param array|string $params column names/values to search on, or a single value to find a matching primary key.
	 * @param string $Type
	 * @throws Exception
	 * @return $Type|stdclass|array */
	static function findRow(string $table, $params=null, $Type=null) {
		if ($params !== null && !is_array($params)) {

			// Build an array of primary key columns.
			$primaries = [];
			$description = static::describe($table);
			foreach($description as $col)
				if ($col->isPrimary)
					$primaries[]=$col->name;

			// If there's exactly 1, we can default to using it.
			if (count($primaries) === 1)
				$params = array($primaries[0]=>$params);
			else
				throw new SimpleDBException("findFirst() \$params cannot be a primitive value because $table does not have exactly one primary key.");
		}

		$rows = static::find($table, $params, $Type, 0, 1);
		foreach ($rows as $item)
			return $item;
		return null;
	}

	/**
	 * Find a row that matches only the primary keys from $row.
	 * @param string $table
	 * @param array|object $row
	 * @throws SimpleDBException */
	static function findRowByPrimaries(string $table, $row): ?object {
		$description = static::describe($table);
		// If the primary keys are not set, return null
		foreach ($description as $col)
			if (is_array($row) && $col->isPrimary && (!isset($row[$col->name]) || $row[$col->name]===null))
				return null;

			elseif (is_object($row) && $col->isPrimary && (!isset($row->{$col->name}) || $row->{$col->name}===null))
				return null;


		// Otherwise, check for an existing row.
		$primaries = [];
		foreach ($row as $name=>$value) {
			$lowerName = strtolower($name);
			if (isset($description[$lowerName]) && $description[$lowerName]->isPrimary) {
				$colName = $description[$lowerName]->name;
				$val = is_array($row) ? $row[$colName] : $row->{$colName};
				$primaries[$colName] = $val;
			}
		}

		return static::findRow($table, $primaries);
	}

	/**
	 * Import a CSV file into a database table, inside a single DB transcation.
	 * TODO: Add option to create table?
	 *
	 * @param string $tableName
	 * @param string $csvFilePath
	 * @param bool $hasHeaders True if the first line of the csv file is headers.
	 * @param ?array $headersMap Key is the name of the table's column, the value is an array of other names it might go by.
	 *     Or if $hasHeaders=false, it's an array where the values are the headers.
	 *
	 * @param ?array $transform Key is the name of the tables column, value can be:
	 *     1. A function(string[] $row):string that will transform it to the proper value before inserting.
	 *     2. A string csv column name or number from which to get the value.
	 *     3. A string[] of csv column names or numbers for where to look for the value.
	 * @return array An array of ids created when inserting into auto-increment columns.
	 * @throws SimpleDBException
	 * @throws Throwable
	 *
	 * @example
	 * SimpleDB::importCsv('stocks', 'stocks.csv', [
	 *     'name'=>'Stock Name',
	 *     'marketCap'=>function($row) { return $row['Market Cap'] / 1000; },
	 *     'shares'=>1,
	 *     'beta'=>['beta2', 'Beta', 'B']
	 * ]); */
	static function importCsv(string $tableName, string $csvFilePath, ?array $transform=[], $hasHeaders=true): array {
		try {
			$ids = [];
			$file = fopen($csvFilePath, 'r');
			static::transaction(function() use ($file, $tableName, $hasHeaders, $transform, &$ids) {

				// Figure out headers.
				$headers = $hasHeaders ? fgetcsv($file) : null;
				$desc = static::describe($tableName);

				while ($line = fgetcsv($file)) {

					$row = [];
					$csvRow = $hasHeaders ? array_combine($headers, $line) : $line;
					foreach ($desc as $meta) {
						$dbCol = $meta->name;

						// Transform columns
						if (isset($transform[$dbCol])) {

							// Function
							if (is_callable($transform[$dbCol]))
								$row[$dbCol] = $transform[$dbCol]($csvRow);

							// Array of csv column names to search.
							elseif (is_array($transform[$dbCol])) {
								foreach ($transform[$dbCol] as $alias)
									if (isset($csvRow[$alias])) {
										$row[$dbCol] = $csvRow[$alias];
										break;
									}
							} // Brace needed so elseif doesn't attach to if() above.

							// Column number
							elseif (is_int($transform[$dbCol]))
								$row[$dbCol] = $line[$transform[$dbCol]];

							// Column name
							else
								$row[$dbCol] = $csvRow[$transform[$dbCol]];
						}

						// Get value from csv column with the same name (no transform).
						elseif (isset($csvRow[$dbCol]))
							$row[$dbCol] = $csvRow[$dbCol];
					}

					// Insert into DB
					$ids []= static::insert($tableName, $row);
				}
			});
			return $ids;
		}
		finally {
			fclose($file);
		}
	}

	/**
	 * Execute an SQL INSERT statement.
	 * @param string $table
	 * @param object|array $row.  If an object, it will be updated with any auto-increment id's.
	 *      Arrays are copy-on-write, so the original array can't be updated.
	 * @return ?int A new auto-increment id if inserting into an auto-increment table.
	 * @throws SimpleDBException
	 * TODO: use the logic from simpledb.py's insert, that will fill unspecified values with coerced defaults. */
	static function insert(string $table, $row): ?int {
		// Build the query:
		$names = [];
		$values = [];
		$params = [];
		$description = static::describe($table);
		if (!$description)
			throw new SimpleDBException("Table '$table' doesn't exist.");


		$q = static::getDriver() === 'mysql' ? '`' : "'";

		foreach ($description as $lName=>$col) {
			$name = $col->name;

			// Only specify a value where $row has a value,
			// or if the column is non-null with no default and no auto-increment, we add a value to prevent an insert error.
			$exists = is_object($row) ? isset($row->$name) : isset($row[$name]);

			if ($exists or (!$col->isNull && $col->default === null && !$col->isAutoIncrement)) {
				$names[]= $q . $name . $q;
				$values[]= '?';
				if (is_object($row))
					$params []= static::coerce($row->$name ?? null, $col);
				else
					$params []= static::coerce($row[$name] ?? null, $col);
			}
		}

		// Run the query
		$names  = join(', ', $names);
		$values = join(', ', $values);
		$sql = "INSERT INTO $q$table$q ($names) VALUES ($values)";
		$stm = static::internalExecute($sql, $params);
		$stm->closeCursor();

		// Set last_insert_id
		$newId = null;
		foreach ($description as $name=>$col)
			if ($col->isAutoIncrement) {
				$newId = static::$connection->lastInsertId();
				$nameWithCase = $col->name;
				if (is_object($row)) // update in original object
					$row->$nameWithCase = intval($newId);
				else
					$row[$nameWithCase] = intval($newId);
				break;
			}
		return $newId;
	}

	/**
	 * Insert a new object as a row, or update an existing one if a row with the primary key already exists
	 * @param string $table
	 * @param object|array $row.  If an object, it will be updated with any auto-increment id's.
	 *      Arrays are copy-on-write, so the original array can't be updated.
	 * @param bool $isInsert
	 * @return bool True if a new row was inserted.
	 * @throws SimpleDBException */
	static function save(string $table, $row, $isInsert=null) : bool {
		if ($isInsert === null)
			$isInsert = !static::findRowByPrimaries($table, $row);

		if ($isInsert)
			static::insert($table, $row);
		else
			static::update($table, $row);
		return $isInsert;
	}

	/**
	 * Return the statement to the cache to be used on subsequent queries.
	 * Used with the internal function statementCheckout()
	 * @param SimpleDBResultSet $statement*/
	static function statementReturn(SimpleDBResultSet $statement) {
		if ($statement->statement) { // If it hasn't already been closed/returned.  Finishing iteration will also close/return it.
			$statement->statement->closeCursor();
			static::$statements[$statement->sql] = $statement->statement; // Add it back to the cached statements.
			$statement->statement = null;
		}

		// For debugging, remove it from $statementsInUse
		//$index = array_search($sql, static::$statementsInUse);
		//if ($index !== false)
		//	array_splice(static::$statementsInUse, $index);
	}

	/**
	 * Returns true if the table exists.
	 * @param string $table
	 * @return bool */
	static function tableExists(string $table): bool {
		if (static::getDriver() === 'sqlite')
			$sql = "SELECT name FROM sqlite_master WHERE type='table' AND name=?";
		else
			$sql = "SHOW TABLES LIKE ?";
		$result = static::getRows($sql, $table);
		return count($result) > 0;
	}

	/**
	 * Execute all of the code within $func as a single transaction, committing on success or
	 * rolling back if an exception is thrown.
	 * @param callable $func
	 * @param ?callable $error
	 * @return mixed The result of $func, or null on failure.
	 * @throws SimpleDBException|Throwable */
	static function transaction(callable $func, ?callable $error=null) {
		$startTime = microtime(true);
		$timeout = static::getConfig()['lockTimeout'] ?? static::DefaultLockTimeout;
		static::connect();

		TryAgain:

		if (static::$transactionDepth)
			throw new SimpleDBException("Already in a transaction.");

		static::$transactionDepth ++;
		if (static::$transactionDepth > static::MaxTransactionDepth)
			throw new SimpleDBException("Too many nested transactions.");


		static::$statementsStack []= static::$statements;
		static::$statements = [];

		static::$connection->beginTransaction();
		try {
			$result = $func();
			static::$connection->commit();
			static::$transactionDepth --;
			static::$statements = array_pop(static::$statementsStack);
			return $result;
		}
		catch (Throwable $e) {
			static::$connection->rollBack();
			static::$transactionDepth --;
			static::$statements = array_pop(static::$statementsStack);

			// If there's a lock, go back and try again until we hit the timeout.
			// According to the php documentation we should be able to do that with:
			// $connection->setAttribute(PDO::ATTR_TIMEOUT, 2000)
			// https://www.php.net/manual/en/pdo.setattribute.php
			// But that doesn't work.
			if (static::getDriver()==='sqlite' && microtime(true) - $startTime < $timeout
				&& strstr($e->getMessage(), 'General error: 5 database is locked') !== false) {

				usleep(intval(static::TransactionRetryWait*1000000)); // 5 ms, an arbitrary value
				goto TryAgain;
			}


			if (!empty($error))
				call_user_func($error, $e);
			else
				throw $e;
		}
		return null;
	}

	/**
	 * Execute an SQL UPDATE statement.
	 * @param string $table
	 * @param object|array $row.  If an object, it will be updated with any auto-increment id's.
	 *      Arrays are copy-on-write, so the original array can't be updated.
	 * @throws SimpleDBException */
	static function update(string $table, $row) {
		$description = static::describe($table);
		$q = static::getDriver() === 'mysql' ? '`' : "'";

		$set = [];
		$params = [];
		$whereParams = [];
		foreach ($row as $name=>$value) {
			$lowerName = strtolower($name);
			if (isset($description[$lowerName])) {
				$col = $description[$lowerName];

				if ($col->isPrimary && $value !== null) { // Set parameter for primary key
					$params[$name] = static::coerce($value, $col);
					$whereParams[$name] = $params[$name];
				}
				else if (!$col->isAutoIncrement) { // Create code to update values.
					$set[]= "$q$name$q=:$name";
					$params[$name] = static::coerce($value, $col);
				}
			}
		}

		if (!count($params))
			throw new SimpleDBException('No primary keys set on object '. print_r($row, true) . ' for updating table ' . $table);
		if (!count($set))
			throw new SimpleDBException('No fields to update from object '. print_r($row, true) . ' in table ' . $table);

		// Combine the parts of the query
		/** @noinspection SqlWithoutWhere */
		$sql = "UPDATE $q$table$q SET " . join(', ', $set);
		$where = static::buildWhere($whereParams);
		if (strlen($where))
			$sql .= ' WHERE ' .$where;
		if (static::getDriver() === 'mysql')
			$sql .= ' LIMIT 1'; // failsafe, but UPDATE with LIMIT is not supported in SQLite

		// Execute
		$stm = static::internalExecute($sql, $params);
		$stm->closeCursor();
	}






	// Internal functions:

	protected static function buildWhere(array $params): string {
		if (count($params)) {
			$driver = static::getDriver();
			$q1 = $driver === 'mysql' ? '`' : '"';
			$q2 = $driver === 'mysql' ? '`' : '"';
			$conditions = [];
			foreach ($params as $col=>$value) {
				$op	= $value === null ? ' IS ' : '=';
				$conditions[]= "$q1$col$q2$op:" . static::namedParameter($col);
			}
			return implode(' AND ', $conditions);
		}
		return '';
	}


	/**
	 * Convert $value to a value that can go into $col without an error.
	 * @throws SimpleDBException
	 * @return bool|int|float|string|DateTime */
	public static function coerce($value, SimpleDBColumn $col) {
		if ((is_array($value) || is_object($value)) && !($value instanceof DateTime))
			throw new SimpleDBException(
				'Column '.$col->name.' cannot store '. var_export($value, true));

		// Coerce empty string to a different "empty" value for non-text fields
		if ($value==='' && $col->type != SimpleDBColType::TEXT) {
			if ($col->isNull)
				return null;
			if ($col->type==SimpleDBColType::INT || $col->type==SimpleDBColType::FLOAT)
				return 0;
			if ($col->type==SimpleDBColType::BOOL)
				return false;
			if ($col->type==SimpleDBColType::DATE)
				return new DateTime(); // current date/time
		}

		// Coerce null to a different "empty" value for non-nullable fields.
		if ($value === null && !$col->isNull) {
			if ($col->default!==null)
				return $col->default;
			if ($col->type==SimpleDBColType::TEXT)
				return '';
			if ($col->type==SimpleDBColType::INT || $col->type==SimpleDBColType::FLOAT)
				return 0;
			if ($col->type==SimpleDBColType::BOOL)
				return false;
		}

		// Convert numbers to dates for date fields.
		if ($col->type == SimpleDBColType::DATE) {
			if (is_int($value) || is_float($value)) {
				$unixTime = number_format($value, 3, '.', '');
				return DateTime::createFromFormat('U.u', $unixTime);
			}
			if (is_string($value))
				return new DateTime($value);
		}

		// No coercion necessary
		return $value;
	}

	protected static function getConfig():array {
		$config = $GLOBALS[static::class];
		if (!is_array($config))
			$config = ['url'=>$config];
		return $config;
	}

	/**
	 * @param string $sql
	 * @param array $params
	 * @return PDOStatement.  If this statement is given to code outside SimpleDB, call SimpleDB::statementCheckout() on it first,
	 *     then SimpleDB::statementReturn() when done with it.
	 *     Iterating over it to the end will automatically call statementReturn().
	 * @throws SimpleDBException */
	static function internalExecute(string $sql, $params=null): PDOStatement {
		if ($params!== null && !is_array($params))
			$params = [$params];

		try {
			if (!static::$connection) // benchmarking shows this check makes it slightly faster.
				static::connect(); // sets lastQuery()

			// If we re-use statements inside transactions, we get "General error: 21 bad parameter or other API misuse"
			// So we use static::$statementStack to create a new group of statements for each transaction depth.
			if (static::ReuseStatements)
				$statement = static::$statements[$sql] ?? (static::$statements[$sql] = static::$connection->prepare($sql));
			else
				$statement = static::$connection->prepare($sql);

			static::$lastQuery = [$sql];

			if ($params) {
				static::$lastQuery []= $params;

				// Set parameters on the statement.
				$intKeys = array_key_exists(0, $params); // isset() will return false if key 0 exists but is null.

				$i = 1;
				foreach ($params as $name => $value) {
					$name = $intKeys ? $i : $name;

					$type = getType($value);
					if ($type === 'integer')
						$statement->bindValue($name, $value, PDO::PARAM_INT);
					else if ($type === 'boolean')
						$statement->bindValue($name, $value, PDO::PARAM_BOOL);
					else if ($value instanceof DateTime) {
						$val = ($value->format('u') !== '000000')
							? $value->format('Y-m-d H:i:s.u')
							: $value->format('Y-m-d H:i:s');
						$statement->bindValue($name, $val, PDO::PARAM_STR);
					}
					else { // there is no PARAM_FLOAT
						//if (!is_scalar($value) && $value !== null) // Is this too much magic?
						//	$value = json_encode($value, JSON_INVALID_UTF8_SUBSTITUTE);
						$statement->bindValue($name, $value, PDO::PARAM_STR);
					}
					$i++;
				}
			}

			// Execute the statement
			TryAgain:
			$statement->execute();

		} catch (PDOException $e) {
			$sleep = .01;
			$timeout = static::getConfig()['lockTimeout'] ?? static::DefaultLockTimeout;

			if (!isset($startTime))
				$startTime = microtime(true);

			if (static::$transactionDepth===0 && static::getDriver()==='sqlite' && microtime(true) - $startTime < $timeout
				&& strstr($e->getMessage(), 'General error: 5 database is locked') !== false) {

				usleep($sleep * 1000000); // 10 ms
				goto TryAgain;
			}

			throw new SimpleDBException($e->getMessage() . "\nLast query was: " . static::getLastQuery(), 0, $e);
		}

		return $statement;
	}

	protected static function namedParameter(string $name):string {
		return preg_replace("/[^a-zA-Z0-9_]/", "", $name); //  TODO: Are these the right values?
	}


	/**
	 * Mark the statement as being used until statementReturn() is called.
	 * That way if we run another query with the same sql, a new statement will be created.
	 * This is used only internally, but statementReturn() should be called sometimes by the user.*/
	protected static function statementCheckout(string $sql) {
		unset(static::$statements[$sql]); // Remove from cached statements.
	}
} // End class SimpleDB

class SimpleDBException extends Exception {}

class SimpleDBColumn {
	public $name;
	public $type = SimpleDBColType::TEXT;
	public $originalType;
	//public $precision = 4294967296; // int.max // TODO:  This will store int and varchar size.  What about decimal?
	public $default;
	public $isPrimary = false;
	public $isAutoIncrement = false;
	public $isNull = false;
	//public $foreignTable;
	//public $foreignColumn;
}

// TODO: Make these match the php type names when possible?
class SimpleDBColType {
	const BOOL = 'Bool';
	const INT = 'Int';
	const FLOAT = 'Float'; // Includes decimal
	const DATE = 'Date';
	const ENUM = 'Enum';
	const TEXT = 'Text';
}


/**
 * Allows using PDO result sets inside a foreach statement. */
class SimpleDBResultSet implements Iterator {
	/** @type PDOStatement */
	public $statement;
	public $sql;

	protected $Type;
	/** @type array */
	protected $types; // Unused

	protected $key = 0;
	protected $current;
	protected $fetchModeSet = false;

	/**
	 * @param PDOStatement $statement
	 * @param string|array $Type
	 * @param string $sql
	 * @param ?array $description */
	function __construct(PDOStatement $statement, string $sql, $Type=null, array $description=null) {
		$this->statement = $statement;
		$this->sql = $sql;
		$this->Type = $Type;

		$typeMap = [
			'BIT' => SimpleDBColType::BOOL,
			'TINY' => SimpleDBColType::INT,
			'LONG' => SimpleDBColType::INT,
			'DOUBLE' => SimpleDBColType::FLOAT,
			'DATETIME' => SimpleDBColType::DATE
		];

		$len = $statement->columnCount();
		for ($i=0; $i<$len; $i++) {
			$meta = $statement->getColumnMeta($i);
			$this->types[$meta['name']] = $typeMap[$meta['native_type']] ?? SimpleDBColType::TEXT;
		}
	}

	// Unreliable
	function rowCount(): int {
		return $this->statement->rowCount();
	}

	/**
	 * Get the entire result set as an array.
	 * This can only be called once, and only if we have not yet iterated over the result set with a foreach()
	 * @return array */
	function toArray(): array {
		$result = [];

		// Determine what type to convert the row to.
		if (!$this->fetchModeSet) {
			static::setFetchMode($this->statement, $this->Type);
			$this->fetchModeSet = true;
		}

		// We do this instead of fetchAll() because we can convert types.
		// We do this instead of foreach($this as $row) because benchmarking shows it's faster.
		while ($row = $this->statement->fetch()) {
			$this->coerceRow($row);
			$result[] = $row;
		}


		// Re-cache the statement after we're done with it.
		if ($this->statement) // If it hasn't already been closed.
			SimpleDB::statementReturn($this);
		$this->statement = null;

		return $result;
	}



	/** Implements Iterable.*/
	function current() {
		return $this->current;
	}

	static function setFetchMode(PDOStatement $statement, $Type) {
		if (is_array($Type))
			$statement->setFetchMode(PDO::FETCH_ASSOC);
		elseif ($Type)
			$statement->setFetchMode(PDO::FETCH_CLASS | PDO::FETCH_PROPS_LATE, $Type);
		else // FETCH_OBJ is the default mode, but we set it again because statements are re-used.
			$statement->setFetchMode(PDO::FETCH_OBJ);
	}

	function coerceRow(&$row) {
		foreach ($row as $name => &$value) {
			if ($value !== null) {
				if ($this->types[$name] === SimpleDBColType::BOOL)
					$value = boolval($value);
				if ($this->types[$name] === SimpleDBColType::INT)
					$value = intval($value);
				if ($this->types[$name] === SimpleDBColType::FLOAT)
					$value = floatval($value);
				if ($this->types[$name] === SimpleDBColType::DATE)
					$value = new DateTime($value);
			}
		}
	}

	/** Implements Iterable.*/
	function next() {

		// Determine what type to convert the row to.
		if (!$this->fetchModeSet) {
			static::setFetchMode($this->statement, $this->Type);
			$this->fetchModeSet = true;
		}

		// Get next values
		$this->key++;
		$this->current = $this->statement->fetch();

		// Convert values of columns to the types from $this->description
		if ($this->current)
			$this->coerceRow($this->current);

		else {
			if ($this->statement) // If it hasn't already been closed.
				SimpleDB::statementReturn($this);
			$this->statement = null;
		}
	}

	/** Implements Iterable.*/
	function key() {
		return $this->key;
	}

	/** Implements Iterable.*/
	function valid() : bool {
		return $this->current !== false;
	}

	/** Implements Iterable.*/
	function rewind() {
		if ($this->current === false)
			throw new SimpleDBException('Can\'t iterate over a result set more than once.  Use toArray() instead.');
		$this->next();
		$this->key = 0;
		//$this->statement->rewind(); // This function doesn't actually exist.
	}
}