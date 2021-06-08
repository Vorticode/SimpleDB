# SimpleDB

SimpleDB is a thin wrapper around PHP's PDO library, to automate away creating connections, preparing statements, iterating over results, converting types, and setting up transactions.  Supports MySQL, MariaDB, and SQLite.  Other databases might be supported in time.  Requires PHP 7.4 and the PDO extensions for MySQL or SQLite.

SimpleDB is currently beta software.

## Connect

Internally, SimpleDB doesn't connect to the database until the first query happens.  If you want to connect immediately, call `SimpleDB::connect()`

Connect to SQLite:

```php
$GLOBALS['SimpleDB'] = 'sqlite:path/to/db.sqlite3';
```

Connect to MySQL or MariaDB:

```php
$GLOBALS['SimpleDB'] = [
    'url'=>'mysql:host=localhost;dbname=test;charset=utf8mb4',
    'user'=>'root',
    'password'=> 'sql',
    'options'=> [ // Optional options array to pass to PDO:
        PDO::ATTR_PERSISTENT => false,
        PDO::MYSQL_ATTR_FOUND_ROWS => true
    ]
];
```

Specifying the config options via `$GLOBALS` allows them to be set in a configuration file without importing SimpleDB.  That way a PHP class autoloader can only import SimpleDB if it's used, reducing PHP execution time.

## Query

Use `execute()` when the query returns no result.

```php
SimpleDB::execute("DELETE FROM users");
```

## Select

Use `getRows()` to get an array of rows where each row is a generic object:

```php
$rows = SimpleDB::getRows("SELECT name, email FROM users WHERE type=?", ['noob']);
foreach ($rows as $row)
    print "Hello $row->name!  Your email is $row->email!";
```

Or pass an object type to get an array of that object:

```php
class User {
	public $name;
    public $email;
}

$rows = SimpleDB::getRows("SELECT name, email FROM users WHERE type=?", ['noob'], 'User');
foreach ($rows as $row)
    print "Hello $row->name!  Your email is $row->email!";
```

Use `getRow()` to get the first row as an object.

```php
$rows = SimpleDB::getRow("SELECT name, email FROM users WHERE type=? LIMIT 1", ['noob']);
print "Hello $row->name!  Your email is $row->email!";
```

Use `getOne()` to get a single value.  Note that the 2nd parameter doesn't have to be an array if retrieving a single value.

```php
$name = SimpleDB::getOne("SELECT name FROM users WHERE id=?", 3);
```

Use `getColumn()` to get all values in a column as an array:

```php
$names = SimpleDB::getColumn("SELECT name FROM users");
```

Use `getCursor()` to get an iterator to loop over columns.  Unlike `getRows()` , `getCursor()` will stream the data from the database one row at a time.  This is useful to avoid running out of memory when processing large result sets.

```php
$rows = SimpleDB::getCursor("SELECT name, email FROM users WHERE type=?", ['noob']);
foreach ($rows as $row)
    print "Hello $row->name!  Your email is $row->email!";
```

When iterating completes, the returned `SimpleDBResultSet` object automatically has its PDO statement returned to the cache of statements for re-use.  However, if you stop before iteration completes, you can also return in manually via:

```php
SimpleDB::statementReturn($rows);
```

If you fail to do this, nothing bad will happen.  But if the query is executed again, a new statement will have to be prepared internally, which is slightly slower.

## Insert and Update

Use `insert()` to automatically insert a row:

```php
$user = [
    'name'=>'Fred', 
    'email'=>'fred@fred.com'
];
SimpleDB::insert('users', $user);
print $user['id']; // auto-increment id is set automatically.
```

Use `update()` to update an existing row:

```php
SimpleDB::update('users', ['id'=>3, 'name'=>'Fred', 'email'=>'fred@fred.com']);
```

Use `save()` to automatically insert or update a row, based on whether primary key is set:

```php
$user = [
    'name'=>'Fred', 
    'email'=>'fred@fred.com'
];
SimpleDB::save('users', $user);
```

The functions `insert`, `update`, or `save()` can accept either arrays or objects.  When using these functions, in some cases, column values are converted to the values the database table will accept:

1. If empty string is inserted or updated into a column that can't except empty string, it will become:
    1. null for nullable columns
    2. 0 for int and float columns
    3. false for boolean columns
2. Inserting updating null into non-nullable columns will become:
    1. The default value if the column has a default.
    2. Empty string for text columns.
    3. 0 for int and float columns.
    4. false for boolean columns
3. Inserting a number into a DateTime column will interpret it as the current Unix timestamp.
4. Inserting a string into a DateTime column will parse it as a date according to the rules of `strtotime()`.

Other than these rules, an error will occur when attempting to insert a value into a column that can't accept it.

## Transactions

```php
$bestUser = ['name'=>'Fred'];

SimpleDB::transaction(function() use ($bestUser) {
   SimpleDB::insert('users', $bestUser);
   SimpleDB::insert('users', ['name'=>'Mary']);                           
});
```

If a transaction cannot execute because another transaction is in progress, SimpleDB will retry every `SimpleDB::TransactionRetryWait` seconds until `SimpleDB::DefaultLockTimeout` is reached.

Transaction nesting is supported, up to a depth of `SimpleDB::MaxTransactionDepth`, which defaults to 10.

## Import CSV

The first row in `users.csv` should match the column names of the users table:

```php
SimpleDB::importCsv('users', 'users.csv', $csv);
```

CSV can also be remapped upon import if the `$transform` (third) parameter is provided.  In the `$transform` array, each key is the name of a column in the table, and each value is one of:

1. A CSV header name, from the first row of the CSV file.
2. An array of column names in the CSV file to search fir.
3. A column number from the CSV file.
4. A function that accepts the current $row as a `string[]` array, and returns the value for the column.

If the $transform array does not have a key for a column, a csv column with the same name will be used if it exists.

```php
$transform = [ // Rules for mapping csv columns to database columns.
    'name'=>'Stock Name',
    'beta'=>['beta2', 'Beta', 'B'],
    'shares'=>1, // Use column 1
    'marketCap'=>function($row) { return $row['Market Cap'] / 1000; }
];
SimpleDB::importCsv('stocks', 'path/to/stocks.csv', $transform);
```

## Get Table Info

See if a table exists.  Get type, default value, primary key, and auto increment  info for each table column in a standardized format:

```php
if (SimpleDB::tableExists('users')) {
    $description = SimpleDB::describe('users');
    var_dump($description);
}
```

## Generate PHP Class

Create code for a PHP class with a typed field for each column in a database table.

```php
$code = SimpleDB::getPhpClassCode('users', 'User');
file_put_contents('User.php', $code);
```

## Multiple Databases

SimpleDB is a static class that supports only one connection.  To connect to a second database, subclass SimpleDB and override each of the static properties so that they can have separate values.  Then provide a second `$GLOBALS` configuration for the subclass:

```php
class OtherDB extends SimpleDB {
	static $connection = null;
	protected static $driver = null;
	protected static $tables = [];
	protected static $lastQuery = [];
	protected static $transactionDepth = 0;
	protected static $statements = [];
	protected static $statementsStack = [];
}

$GLOBALS['SimpleDB'] = 'sqlite:site/data/db.sqlite3';

$GLOBALS['OtherDB'] = [
	'url'=>'mysql:host=localhost;dbname=otherdb;charset=utf8mb4',
	'user'=>'root',
	'password'=> 'sql',
	'options'=> [
		PDO::ATTR_PERSISTENT=>false,
		PDO::MYSQL_ATTR_FOUND_ROWS => true
	]
];

// Now we can use two databases at the same time:
foreach (SimpleDB::getCursor("SELECT * FROM users") as $user)
    OtherDB::save('users', $user);
```

