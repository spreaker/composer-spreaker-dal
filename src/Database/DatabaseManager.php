<?php

/**
 * Spreaker\Dal\Database\DatabaseManager
 *
 * $query_params  are parameters passed in when a prepare statement executed, it supports 2 forms:
 *     named paramenter, i.e.  update ... set f1=:f1, f2=f2  => array(':f1'=>'value 1', ':f2'=>'value 2')
 *     array parameters, i.e.  update ... set f1 = ?, f2 = ? => array('value 1', 'value 2')
 * obviously, the named parameters way is more flexible and so it is prefered.
 *
 * $query_options are used to change the behavior of the query, here are all the supported options
 *
 *     'table_name'     override the default table name got from Model classes
 *     'class_name'     override the default class name got from Model classes
 *     'where_cond'     specify the where condition for update/delete
 *     'no_result'      not return result set, but return the number of affected rows
 *     'set_columns'    specify which columns to update when do a update
 *     'use_slave'      `true` to use the `default` slave or a string containing the name of the slave to use (defaults to `false`, running the query on master)
 *     'fallback_slave' `true` to re-try on the `default` slave in case of an issue on master or a string containing the name of the slave to use to retry (defaults to `false`, having NO retry on slave)
 *     'slave_retries'  the times to retry on slave before we try master
 *     'fetch_column'   specify which column of a row to fetch
 *     'use_cache'      cache the result rows if true
 *     'cache_ttl'      specify the ttl of the result cache
 *     'return_array'   return array instead of objects
 *
 * @link
 * @copyright
 * @license
 **/

namespace Spreaker\Dal\Database;

use PDO, Exception, PDOException;
use Spreaker\Dal\Model\Model as Model;
use Spreaker\Dal\Cache\CacheInterface as CacheInterface;
use Psr\Log\LoggerInterface as LoggerInterface;
use Psr\Log\LoggerAwareInterface as LoggerAwareInterface;

class DatabaseManager implements LoggerAwareInterface
{
    const MAX_CACHED_PREPARED_ITEMS = 64;

    /**
     * Options
     */
    const OPTION_USE_SLAVE_BY_DEFAULT = 'use_slave_by_default';
    const OPTION_TABLE_SHARD_MAPPING  = 'table_shard_mapping';
    const OPTION_DEFAULT_SHARD        = 'default_shard';

    private static $EDITABLE_OPTIONS = array(
        self::OPTION_USE_SLAVE_BY_DEFAULT
    );

    /**
     * constants of error codes of pg, these errors make us retry the query
     */
    const ERRCODE_T_R_SERIALIZATION_FAILURE        = '40001';
    const ERRCODE_T_R_DEADLOCK_DETECTED            = '40P01';
    const ERRCODE_QUERY_CANCELED                   = '57014';
    const ERRCODE_CONNECTION_FAILURE               = '08006';
    const ERRCODE_CONNECTION_REFUSED               = 'HY000'; // No connection to the server
    const ERRCODE_PGBOUNCER_SERVER_CONNECTION_LOST = '08P01'; // PGbouncer has lost the connection to the server

    public static $RETRY_ERROR_CODES = array(
        self::ERRCODE_T_R_SERIALIZATION_FAILURE,
        self::ERRCODE_T_R_DEADLOCK_DETECTED,
        self::ERRCODE_QUERY_CANCELED,
        self::ERRCODE_CONNECTION_FAILURE,
        self::ERRCODE_CONNECTION_REFUSED,
        self::ERRCODE_PGBOUNCER_SERVER_CONNECTION_LOST
    );

    protected $_lastErrorInfo = null;

    /**
     * @var array  a list of opened PDO connections
     */
    protected $_connections = array();

    /**
     * @var array
     */
    protected $_prepared = array();

    /**
     * @var array   Internal statistics about executed queries
     */
    protected $_stats = array();

    /**
     * @var array
     */
    protected $_profilingData = null;

    /**
     * @var array
     */
    protected $_options = null;

    /**
     * @var array
     */
    protected $_schemas = null;

    /**
     * @var CacheInterface  The cache driver used for caching result sets.
     */
    protected $_resultCacheDriver = null;

    /**
     * @var Logger
     */
    protected $_logger = null;


    /**
     * Constructor
     * @param  array $databases
     * @param  array $schemas
     */
    public function __construct($databases, $schemas)
    {
        $this->_initialize($databases, $schemas);
    }

    /**
     * Sets a logger instance on the object
     *
     * @param LoggerInterface $logger
     * @return null
     */
    public function setLogger(LoggerInterface $logger)
    {
        $this->_logger = $logger;
    }

    /**
     * Set result cache driver
     *
     * @param CacheInterface $cacheDriver
     * @return null
     */
    public function setResultCacheDriver($cacheDriver)
    {
        $this->_resultCacheDriver = $cacheDriver;
    }

    /**
     * Get result cache driver
     *
     * @return CacheInterface
     */
    public function getResultCacheDriver()
    {
        return $this->_resultCacheDriver;
    }

    /**
     * getOptions
     *
     * @return array
     */
    public function getOptions()
    {
        return $this->_options;
    }

    public function resetQueryCount()
    {
        $this->_stats = array(
            'queries_count'           => 0,
            'queries_on_master_count' => 0,
            'queries_on_slave_count'  => 0
        );
    }

    public function getQueryCount()
    {
        return $this->_stats['queries_count'];
    }

    public function getQueryOnMasterCount()
    {
        return $this->_stats['queries_on_master_count'];
    }

    public function getQueryOnSlaveCount()
    {
        return $this->_stats['queries_on_slave_count'];
    }

    public function getConnectionsCount()
    {
        return count($this->_connections);
    }

    /**
     * @param string $name
     * @param mixed  $value
     */
    public function setOption($name, $value)
    {
        // Ensure the option is editable
        if (!in_array($name, self::$EDITABLE_OPTIONS)) {
            throw new Exception("The DatabaseManager option {$name} is not editable");
        }

        $this->_options[$name] = $value;
    }

    /**
     * Get a connection parameters
     *
     * @param string        $table_name
     * @param bool|string   $use_slave
     * @return array
     */
    public function getConnectionParameters($table_name = false, $use_slave = false)
    {
        $params = false;
        $shardName = false;

        // Pick the shard specific for the input table name (if any)
        if ($table_name && isset($this->_options[self::OPTION_TABLE_SHARD_MAPPING][$table_name])) {
            $shardName = $this->_options[self::OPTION_TABLE_SHARD_MAPPING][$table_name];
        } else {
            // use default shard
            $shardName = $this->_options[self::OPTION_DEFAULT_SHARD];
        }

        $shardCfg = $this->_options['shards'][$shardName];

        // Choose the slave to use
        if ($use_slave === true && !empty($shardCfg['slaves']['default'])) {
            $params = $shardCfg['slaves']['default'];
        } else if (is_string($use_slave) && !empty($shardCfg['slaves'][$use_slave])) {
            $params = $shardCfg['slaves'][$use_slave];
        } else {
            $params = $shardCfg['master'];
        }

        // ensure we got a valid parameters
        if (!$params) {
            throw new Exception('No connection parameters found!');
        }

        return $params;
    }

    /**
     * Get table from sql statement
     *
     * @param  string  $statement
     * @return string|false
     */
    public function getTableFromSql($statement)
    {
        // Only match the following forms of SQL statements
        // SELECT ... FROM table table_alias ...
        // INSERT INTO table ...
        // UPDATE table ...
        // DELETE FROM table ...
        // CREATE TABLE ...
        // ALTER TABLE ...
        // DROP TABLE ...
        // TRUNCATE table ...
        $table_name = false;
        $types = array(
            'select'   => '/\bfrom\s+([^\s]+)\b/i',
            'insert'   => '/\binto\s+([^\s]+)\b/i',
            'update'   => '/\bupdate\s+([^\s]+)\b/i',
            'delete'   => '/\bfrom\s+([^\s]+)\b/i',
            'create'   => '/\bcreate\s+table\s+([^\s]+)\b/i',
            'alter'    => '/\balter\s+table\s+([^\s]+)\b/i',
            'drop'     => '/\bdrop\s+table\s+([^\s]+)\b/i',
            'truncate' => '/\btruncate\s+([^\s,]+)\b/i',
        );

        $statement = ltrim($statement);

        foreach ($types as $type => $pattern) {
            if (stripos($statement, $type) === 0) {
                if (preg_match($pattern, $statement, $matches)) {
                    $table_name = $matches[1];
                    break;
                }
            }
        }

        return $table_name;
    }

    /**
     * establish a connection to a DB server, add it to pool and return it
     *
     *   - default connect to master server of default shard
     *   - otherwise connect to a DB server determined by $query_options
     *
     * @param  array  $query_options
     * @return PDO
     */
    public function connect($query_options = array())
    {
        $table_name = isset($query_options['table_name']) ? $query_options['table_name'] : false;
        $use_slave  = isset($query_options['use_slave'])  ? $query_options['use_slave'] : false;

        $params     = $this->getConnectionParameters($table_name, $use_slave);
        $hash       = $this->_getConnectionHash($params);

        if (isset($this->_connections[$hash])) {
            return $this->_connections[$hash];
        } else {
            $connection = null;

            if (is_object($params) && get_class($params) === 'PDO') {
                // Allow to re-use a PDO connection
                $connection = $params;
            } else if (is_object($params) && get_class($params) === 'Doctrine_Connection_Pgsql') {
                // Allow to re-use a Doctrine connection without creating it until necessary
                $connection = $params->getDbh();
            } else if (is_string($params)) {
                // Extract the connection timeout from the DSN, otherwise it's hardcoded
                // in the pdo_pgsql driver to 30 seconds
                $connection = new PDO($params, null, null, array(PDO::ATTR_TIMEOUT => $this->_getConnectTimeoutFromDsn($params)));
            } else {
                throw new Exception("Invalid connection parameters");
            }

            // Disable native prepared statements for several reasons:
            // 1. Can use pgBouncer in "Transaction Pooling" mode
            // 2. We found a performance decrease after running the same
            //    SELECT query that contains WHERE IN and some parameters
            $connection->setAttribute(PDO::ATTR_EMULATE_PREPARES, true);

            $this->_connections[$hash] = $connection;

            return $connection;
        }
    }

    /**
     * fetch one row from database and populate a model object
     *
     * @param  string  $query
     * @param  array   $query_params
     * @param  array   $query_options
     * @return object|null
     */
    public function fetchOne($query, $query_params = array(), $query_options = array())
    {
        $collection = $this->fetch($query, $query_params, $query_options);

        if (is_array($collection) && count($collection) > 0) {
            return array_shift($collection);
        }

        return null;
    }

    /**
     * fetch first column of the first row
     *
     * @param  string  $query
     * @param  array   $query_params
     * @param  array   $query_options
     * @return mixed|null
     */
    public function fetchOneColumn($query, $query_params = array(), $query_options = array())
    {
        $row = $this->fetchOne($query, $query_params, $query_options);
        if ($row == null) {
            return null;
        }

        $row = (array) $row;
        return !empty($row) ? current($row) : null;
    }

    /**
     * fetch a single column from database
     * if multiple columns exists, the specified column OR the first column will be fetched
     *
     * @param  string  $query
     * @param  array   $query_params
     * @param  array   $query_options
     * @return array
     */
    public function fetchColumn($query, $query_params = array(), $query_options = array())
    {
        // for fetchColumn, use stdClass
        if (isset($query_options['class_name'])) {
            unset($query_options['class_name']);
        }

        $collection = $this->fetch($query, $query_params, $query_options);

        if (is_array($collection)) {
            $column_name = isset($query_options['fetch_column']) ? $query_options['fetch_column'] : null;

            $result = array();
            foreach ($collection as $item) {
                if (!empty($column_name)) { // column_name is specified
                    if (!isset($item->$column_name)) {
                        throw new Exception("Column: $column_name does not exist");
                    } else {
                        $result[] = $item->$column_name;
                    }
                } else { // otherwise, we just fetch the first column
                    $vars = is_object($item) ? get_object_vars($item) : array();
                    if (count($vars) > 0) {
                        $result[] = array_shift($vars);
                    } else {
                        throw new Exception("No property found on the object");
                    }
                }
            }

            return $result;
        }

        return array();
    }

    /**
     * fetch multiple rows from database
     *
     * @param  string  $query
     * @param  array   $query_params
     * @param  array   $query_options
     * @return array
     */
    public function fetch($query, $query_params = array(), $query_options = array())
    {
        if (stripos($query, 'SELECT') === 0) {
            $query_options = $this->_addUseSlaveDefault($query_options);
        }

        if (isset($query_options['use_slave']) && $query_options['use_slave'] !== false) {
            return $this->_executeWithSlaveRetry($query, $query_params, $query_options);
        } else if (isset($query_options['fallback_slave']) && $query_options['fallback_slave'] !== false) {
            return $this->_executeWithSlaveFallback($query, $query_params, $query_options);
        } else {
            return $this->_execute($query, $query_params, $query_options);
        }
    }

    /**
     * Insert 1+ records. Returns a Model if input is a single model, array of Model if
     * input is an array of models, or null if 'no_result' option has been specified.
     *
     * @param  Model|array  $records            Single model or array of models to insert
     * @param  array        $query_options      Options
     * @return Model|array|null
     */
    public function insert($records, $query_options = array())
    {
        // Check params
        if (!($records instanceof Model) && (!is_array($records) || empty($records))) {
            throw new Exception('Invalid argument: input records should be an instance of Model or an array of Model');
        }

        // Should be convenient to always work with arrays
        $records = is_array($records) ? $records : array($records);

        // Ensure options
        $record_class_name           = get_class($records[0]);
        $query_options['class_name'] = !empty($query_options['class_name']) ? $query_options['class_name'] : $record_class_name;
        $this->_adjustQueryOptionsFromSchema($record_class_name, $query_options);

        // Detect columns to insert
        $cols = array();
        foreach ($records as $record) {
            foreach ($record->data as $key => $value) {
                if (!isset($cols[$key])) {
                    $cols[$key] = true;
                }
            }
        }
        $cols = array_keys($cols);

        // Create query params
        $query_params = array();
        foreach ($records as $record) {
            foreach ($cols as $col) {
                $query_params[] = isset($record->data->{$col}) ? $record->data->{$col} : null;
            }
        }

        // Create query template
        if (isset($query_options['no_result']) && $query_options['no_result']) {
            $query_tmpl = 'INSERT INTO %s (%s) VALUES %s';
        } else {
            $query_tmpl = 'INSERT INTO %s (%s) VALUES %s RETURNING *';
        }

        // Build query (tricky code but very compact)
        $query_table  = $query_options['table_name'];
        $query_cols   = implode(', ', $cols);
        $query_values = implode(', ', array_fill(0, count($records), '(' . implode(',', array_fill(0, count($cols), '?')) . ')'));
        $query        = sprintf($query_tmpl, $query_table, $query_cols, $query_values);

        // Run query
        $output = $this->_execute($query, $query_params, $query_options);

        // Check if we've to care about the result
        if (isset($query_options['no_result']) && $query_options['no_result']) {
            return;
        }

        // Update models data
        if (count($output) != count($records)) {
            throw new Exception('Update succeeded, but we got an expected number of output records (expected: ' . count($records) . ', actual: ' . count($output) . ')');
        }

        foreach ($records as $index => &$record) {
            $record->data = $output[$index]->data;
        }

        return count($records) > 1 ? $records : $records[0];
    }

    /**
     * @param string  $class_name
     * @param string
     */
    public function getTableName($class_name)
    {
        if (isset($this->_schemas[$class_name]) && isset($this->_schemas[$class_name]['tableName'])) {
            $tableName = $this->_schemas[$class_name]['tableName'];
            if (is_string($tableName) && !empty($tableName)) {
                return $tableName;
            }
        }
        throw new Exception('Missing schema definition for Model:' . $class_name);
    }

    /**
     * @param string  $class_name
     * @param array
     */
    public function getPrimaryKeys($class_name)
    {
        if (isset($this->_schemas[$class_name]) && isset($this->_schemas[$class_name]['primaryKey'])) {
            $primaryKeys = $this->_schemas[$class_name]['primaryKey'];
            if (is_array($primaryKeys) && !empty($primaryKeys)) {
                return $primaryKeys;
            }
        }
        throw new Exception('Missing schema definition for Model:' . $class_name);
    }

    /**
     * Update a record by primary key / unique key
     * @param   Model     $record,  contains the fields to be updated
     * @param   array     $query_options, query options
     * @return  bool|array|Model
     **/
    public function update($record,  $query_options = array())
    {
        // check $record
        if (!($record instanceof Model)) {
            throw new Exception("Invalid argument: record is not an instance of Model");
        }

        // try get table_name/where_cond of query_options from schema definition
        $modelClassName = get_class($record);
        $this->_adjustQueryOptionsFromSchema($modelClassName, $query_options);

        // return the row after update??
        $tmpl = 'UPDATE %s SET %s WHERE %s RETURNING *';
        if (isset($query_options['no_result']) && $query_options['no_result']) {
            $tmpl = 'UPDATE %s SET %s WHERE %s';
        } else {
            if (!isset($query_options['class_name']) || empty($query_options['class_name'])) {
                // use Model class if not specified
                $query_options['class_name'] = $modelClassName;
            }
        }

        // specification of where_cond
        //
        // 1. string, specify a single-column primary key/unique key, i.e. 'user_id'
        // 2. array of string, specify a multi-columns PK/UK, i.e. array('first_name', 'last_name')
        // 3. associative array, specify both key and value, i.e. array('first_name' => 'bill', 'last_name' => 'joy')
        // 4. do we really need more???

        // build the update query statement
        // update tbl set f1 = :f1, f2 = :f2 where c1 = :c1 and c2 = :c2 returning *
        $query_cond = array();
        $query_params = array();
        $where_cond = $query_options['where_cond'];
        $newRecordData  = clone $record->data;

        if (is_string($where_cond) && !empty($where_cond)) {
            // in this case, record must have the columns in condition
            if (!isset($newRecordData->{$where_cond})) {
                throw new Exception("record does not have property: $where_cond");
            }

            $query_cond[] = "$where_cond = :cond_$where_cond";
            $query_params[":cond_$where_cond"] = $newRecordData->{$where_cond};

            // don't need update the key column, since it is used as where condition
            unset($newRecordData->{$where_cond});
        } else if (is_array($where_cond)) {
            foreach ($where_cond as $cond_key => $cond_value) {
                if (is_string($cond_key)) {
                    if (empty($cond_value)) {
                        throw new Exception("Invalid where_cond options, empty condition value");
                    }
                    $query_cond[] = "$cond_key = :cond_$cond_key";
                    $query_params[":cond_$cond_key"] = $cond_value;

                    // do not update the columns both in set columns / conditions and have the same value
                    if (isset($newRecordData->$cond_key) && $cond_value == $newRecordData->$cond_key) {
                        unset($newRecordData->$cond_key);
                    }
                } else {
                    // in this case, record must have the columns in condition
                    if (!isset($newRecordData->{$cond_value})) {
                        throw new Exception("record does not have property: $cond_value");
                    }

                    // values array will be considered as key columns, like array('first', 'second')
                    $query_cond[] = "$cond_value = :cond_$cond_value";
                    $query_params[":cond_$cond_value"] = $newRecordData->{$cond_value};

                    // don't need update the key column, since it is used as where condition
                    unset($newRecordData->{$cond_value});
                }
            }
        } else {
            throw new Exception("Invalid where_cond options");
        }

        $set_cols = array();
        // use set_columns from options
        if (isset($query_options['set_columns'])) {
            // Ensure it's an array, converting a single column name (string) into an array
            $query_options['set_columns'] = is_array($query_options['set_columns']) ? $query_options['set_columns'] : (array) $query_options['set_columns'];

            foreach ($query_options['set_columns'] as $key) {
                $set_cols[] = "$key = :$key";
                $query_params[":$key"] = $newRecordData->{$key};
            }
        } else {
            foreach ($newRecordData as $key => $value) {
                $set_cols[] = "$key = :$key";
                $query_params[":$key"] = $value;
            }
        }

        $query = sprintf($tmpl, $query_options['table_name'], implode(', ', $set_cols), implode(' AND ', $query_cond));
        $retval = $this->_execute($query, $query_params, $query_options);

        if (is_array($retval) && count($retval) > 0 && $retval[0] instanceof Model) {
            if (!isset($query_options['no_result']) || !$query_options['no_result']) {
                $record->data = $retval[0]->data;
                return true;
            }
        }

        return $retval;
    }

    /**
     * delete a record by primary key/unique key
     *
     * @param Model  $record
     * @param array  $query_options
     * @return integer
     **/
    public function delete($record, $query_options=array())
    {
        // check $record
        if (!($record instanceof Model)) {
            throw new Exception("Invalid argument: record is not an instance of Model");
        }

        // try get table_name/where_cond of query_options from schema definition
        $modelClassName = get_class($record);
        $this->_adjustQueryOptionsFromSchema($modelClassName, $query_options);

        // build delete sql statement like:
        // delete from tbl where f1=:f1
        $tmpl = 'DELETE FROM %s WHERE %s';
        $query_cond = array();
        $query_params = array();
        $where_cond = $query_options['where_cond'];

       if (is_string($where_cond) && !empty($where_cond)) {
            // in this case, record must have the columns in condition
            if (!isset($record->data->{$where_cond})) {
                throw new Exception("record does not have property: $where_cond");
            }

            $query_cond[] = "$where_cond = :cond_$where_cond";
            $query_params[":cond_$where_cond"] = $record->data->{$where_cond};
        } else if (is_array($where_cond)) {
            foreach ($where_cond as $cond_key => $cond_value) {
                if (is_string($cond_key)) {
                    if (empty($cond_value)) {
                        throw new Exception("Invalid where_cond options, empty condition value");
                    }
                    $query_cond[] = "$cond_key = :cond_$cond_key";
                    $query_params[":cond_$cond_key"] = $cond_value;
                } else {
                    // in this case, record must have the columns in condition
                    if (!isset($record->data->{$cond_value})) {
                        throw new Exception("record does not have property: $cond_value");
                    }

                    // values array will be considered as key columns, like array('first', 'second')
                    $query_cond[] = "$cond_value = :cond_$cond_value";
                    $query_params[":cond_$cond_value"] = $record->data->{$cond_value};
                }
            }
        } else {
            throw new Exception("Invalid where_cond options");
        }

        $query = sprintf($tmpl, $query_options['table_name'], implode(' AND ', $query_cond));
        // delete doesn't return rows.
        $query_options = array_merge($query_options, array('no_result' => true));
        return $this->_execute($query, $query_params, $query_options);
    }

    /**
     * Runs a query. Returns an array (of rows) containing the result of the query,
     * or the number of affected rows if no_result option is enabled.
     *
     * @param  string  $query
     * @param  array   $query_params
     * @param  array   $query_options
     * @return array|int
     */
    public function query($query, $query_params = array(), $query_options = array())
    {
        return $this->_execute($query, $query_params, $query_options);
    }

    /**
     * quote value/values, we need a valid connection here.
     *
     * @param  string|array  $value
     * @param  array         $query_options
     * @return string|array
     */
    public function quote($value, $query_options = array())
    {
        // Quote using the slave connection reference if "use slave by default" is enabled
        $query_options = $this->_addUseSlaveDefault($query_options);

        $connection = $this->connect($query_options);

        if (is_array($value)) {
            $escaped = array();

            foreach ($value as $item) {
                $escaped[] = $connection->quote($item);
            }
            return $escaped;
        } else {
            return $connection->quote($value);
        }
    }

    /**
     * execute a sql statement with retry
     *  - try to execute the query $slave_retries times on slave
     *  - if still fails, try once more on master
     *
     * @param string $query
     * @param array $query_params
     * @param array $query_options
     * @return bool|integer|array
     */
    private function _executeWithSlaveRetry($query, $query_params, $query_options)
    {
        // we only retry on these errors
        $retries        = 0;
        $config_retries = isset($this->_options['slave_retries']) ? (int) $this->_options['slave_retries'] : 0;
        $slave_retries  = isset($query_options['slave_retries']) ? (int) $query_options['slave_retries'] : $config_retries;

        // on slave
        while ($retries <= $slave_retries) {
            $retries++;
            try {
                return $this->_execute($query, $query_params, $query_options);
            } catch (Exception $exception) {
                if (!$this->_shouldRetryOnLastErrorWithException($exception)) {
                    throw $exception;
                }

                if ($this->_isLoggingEnabled()) {
                    $this->_logger->warning("(retry #$retries) $query", array(
                        'query_params'  => $query_params,
                        'query_options' => $query_options,
                        'query_timing'  => array(),
                        'query_result'  => array('result'=>false,'rows'=>0),
                    ));
                }
            }
        }

        // on master
        $query_options['use_slave'] = false;
        return $this->_execute($query, $query_params, $query_options);
    }

    /**
     * Runs the query on master. If fails, try to run the query on slave.
     *
     * @param string $query
     * @param array $query_params
     * @param array $query_options
     * @return bool|integer|array
     */
    private function _executeWithSlaveFallback($query, $query_params, $query_options)
    {
        // Run the query on master
        $query_options['use_slave'] = false;

        try {
            return $this->_execute($query, $query_params, $query_options);
        } catch (Exception $exception) {
            if (!$this->_shouldRetryOnLastErrorWithException($exception)) {
                throw $exception;
            }
        }

        // Fallback to slave
        $query_options['use_slave'] = is_string($query_options['fallback_slave']) ? $query_options['fallback_slave'] : 'default';
        return $this->_execute($query, $query_params, $query_options);
    }

    /**
     * execute a sql statement
     *
     * @param string $query
     * @param array $query_params
     * @param array $query_options
     * @return bool|integer|array
     */
    private function _execute($query, $query_params, $query_options)
    {
        $resultCacheHash = null;
        $useResultCache  = isset($query_options['use_cache']) && $query_options['use_cache']
            && isset($query_options['cache_ttl']) && intval($query_options['cache_ttl']) > 0
            && $this->_resultCacheDriver instanceof CacheInterface
            && $this->_isSelectQuery($query);

        if ($useResultCache) {
            $resultCacheHash = $this->_calculateResultCacheHash($query, $query_params);
            $cachedResult    = $this->_resultCacheDriver->get($resultCacheHash);

            // cache hit
            if (!is_null($cachedResult)) {

                $rows = unserialize($cachedResult);
                if (isset($query_options['return_array']) && $query_options['return_array']) {
                    return $rows;
                }

                return $this->_hydrateResultSet($rows, $query_options);
            }
        }

        $this->_initTiming();

        // if table_name is not specified, we try to get that from sql statement
        if (!isset($query_options['table_name'])) {
            $query_options['table_name'] = $this->getTableFromSql($query);
        }
        $connection = $this->connect($query_options);

        // should re-prepare() when on different connection even if the query is the same
        $connection_hash = spl_object_hash($connection);

        $hash = md5($connection_hash . $query);
        if (!isset($this->_prepared[$hash])) {
            $this->_doTiming('prepare_start');
            $statement = $connection->prepare($query);
            $this->_doTiming('prepare_end');

            if (!$statement) {
                $errorCode = $connection->errorCode();
                $errorInfo = $connection->errorInfo();
                $this->_lastErrorInfo = $errorInfo;
                throw new PDOException("Unable to prepare the query $query because of errorCode: $errorCode, errorInfo: " . implode(',', $errorInfo), intval($errorCode));
            }

            if (count($this->_prepared) >= self::MAX_CACHED_PREPARED_ITEMS) {
                $this->_prepared = array();
            }

            $this->_prepared[$hash] = $statement;
        } else {
            $statement = $this->_prepared[$hash];
        }

        $this->_doTiming('query_start');

        // fix boolean data type of query parameters
        foreach ($query_params as $key => $val) {
            if (is_bool($val)) {
                $query_params[$key] = $val ? 't' : 'f';
            }
        }

        $result = $statement->execute($query_params);
        $this->_doTiming('query_end');
        $this->_incQueryCount($query_options);

        $query_result = array('result' => $result, 'rows' => $statement->rowCount());
        $this->_logQuery($query, $query_params, $query_options, $query_result);

        if (!$result) {
            $errorCode = $statement->errorCode();
            $errorInfo = $statement->errorInfo();
            $this->_lastErrorInfo = $errorInfo;
            throw new PDOException("Unable to execute the query $query because of errorCode: $errorCode, errorInfo: " . implode(',', $errorInfo), intval($errorCode));
        }

        // insert/update/delete ...
        if (isset($query_options['no_result'])) {
            return $statement->rowCount();
        }

        $rows = $statement->fetchAll(PDO::FETCH_ASSOC);

        // save to cache
        if ($useResultCache) {
            $cachedResult = serialize($rows);
            $cacheTTL     = intval($query_options['cache_ttl']);
            $this->_resultCacheDriver->set($resultCacheHash, $cachedResult, $cacheTTL);
        }

        if (isset($query_options['return_array']) && $query_options['return_array']) {
            return $rows;
        }

        return $this->_hydrateResultSet($rows, $query_options);
    }

    /**
     * hydrate result set
     * @param  array    $rows
     * @param  array    $query_options
     * @return array
     */
    private function _hydrateResultSet($rows, $query_options)
    {
        $class_name = null;
        if (isset($query_options['class_name'])) {
            $class_name = $query_options['class_name'];
        }

        // Read schema config for the specified class
        $schema = ($class_name && isset($this->_schemas[$class_name])) ? $this->_schemas[$class_name] : null;

        // Check whether has multiple classes
        $has_multiple_classes = $schema && isset($schema['column']) && isset($schema['classes']) && is_array($schema['classes']);

        $collection = array();

        foreach ($rows as $row) {
            // Handle schema definition
            if ($has_multiple_classes) {
                $columnValue = $row[$schema['column']];
                $class_name  = isset($schema['classes'][$columnValue]) ? $schema['classes'][$columnValue] : $class_name;
            }

            if ($class_name) {
                $collection[] = new $class_name($row);
            } else {
                $collection[] = (object) $row;
            }
        }

        return $collection;
    }

    /**
     * @param string  $query   The query statement
     * @return boolean
     */
    private function _isSelectQuery($query)
    {
        return stripos(ltrim($query), 'select') === 0;
    }

    /**
     * Calculate a cache key based upon query and parameters
     *
     * @param string   $query            The SELECT SQL query statement
     * @param array    $query_params     The parameters of the query
     * @return string
     */
    private function _calculateResultCacheHash($query, $query_params)
    {
        return md5($query . var_export($query_params, true));
    }

    /**
     * Inject database configure options, which should be an array like this:
     *
     * array(
     *    'shards' => array(
     *        'shard1' => array('master' => DSN_M, 'slaves' => array('default' => DSN_S1, 'name' => DSN_S2), 'default' => true),
     *        'shard2' => array('master' => DSN_M, 'slaves' => array('default' => DSN_S1, 'name' => DSN_S2), 'tables' => array('table1', 'table2')),
     *    )
     * )
     *
     * @param array  $databases
     * @param array  $schemas
     */
    private function _initialize($databases, $schemas = array())
    {
        if (!is_array($databases) || !isset($databases['shards']) || !is_array($databases['shards'])) {
            throw new Exception('parameter $databases should be an array that contains an array with key "shards".');
        }

        if (!is_array($schemas)) {
            throw new Exception('parameter $schemas should be an array.');
        }

        // re-structure the schemas, make inherited Model classes appear in top-level keys
        foreach ($schemas as $modelClassName => $definition) {
            if (isset($definition['classes']) && is_array($definition['classes'])) {
                foreach ($definition['classes'] as $key => $subClassName) {
                    $schemas[$subClassName] = array(
                        'tableName'  => $definition['tableName'],
                        'primaryKey' => isset($definition['primaryKey']) ? $definition['primaryKey'] : null,
                    );
                }
            }
        }

        $this->_options = $databases;
        $this->_schemas = $schemas;

        // convert the shard configuration, make it easier to use
        $default_shard = false;
        $table_shard_mapping = array();
        foreach ($databases['shards'] as $name => $shard) {
            if (isset($shard['default'])) {
                if (!$default_shard) {
                    $default_shard = $name;
                } else {
                    throw new Exception('Specified more than ONE default shards in the database configuration file!');
                }
            }

            if (isset($shard['tables'])) {
                foreach ($shard['tables'] as $table) {
                    if (!isset($table_shard_mapping[$table])) {
                        $table_shard_mapping[$table] = $name;
                    } else {
                        throw new Exception('Specified more than ONE shards for a table in the database configuration file!');
                    }
                }
            }
        }

        if (!$default_shard) {
            throw new Exception('No default shard found in the database configuration file!');
        }

        $this->_options[self::OPTION_DEFAULT_SHARD]        = $default_shard;
        $this->_options[self::OPTION_TABLE_SHARD_MAPPING]  = $table_shard_mapping;
        $this->_options[self::OPTION_USE_SLAVE_BY_DEFAULT] = false;

        // Init internal stats
        $this->resetQueryCount();
    }

    /**
     * @param string|object $parameters
     */
    private function _getConnectionHash($parameters)
    {
        if (is_object($parameters) && get_class($parameters) === 'PDO') {
            // a PDO connection object
            $hash = spl_object_hash($parameters);
        } elseif (is_object($parameters) && get_class($parameters) === 'Doctrine_Connection_Pgsql') {
            // a Doctrine connection object
            $hash = spl_object_hash($parameters);
        } else {
            // a DSN string, like pgsql:host=localhost;port=5432 ...
            $hash = md5($parameters);
        }

        return $hash;
    }

    /**
     * adjust query options from schemas definition of a Model
     *
     * @param  string   $class_name
     * @param  array    $query_options
     **/
    private function _adjustQueryOptionsFromSchema($class_name, &$query_options)
    {
        if (!isset($query_options['table_name']) || empty($query_options['table_name']) ||
            !isset($query_options['where_cond']) || empty($query_options['where_cond'])) {

            if (!isset($this->_schemas[$class_name])) {
                throw new Exception('Missing schema definition for Model:' . $class_name);
            }

            if (!isset($this->_schemas[$class_name]['tableName'])) {
                throw new Exception('Invalid schema definition for Model:' . $class_name);
            }

            if (!isset($query_options['table_name']) || empty($query_options['table_name'])) {
                $query_options['table_name']  = $this->_schemas[$class_name]['tableName'];
            }

            if (!isset($query_options['where_cond']) || empty($query_options['where_cond'])) {
                if (!empty($this->_schemas[$class_name]['primaryKey'])) {
                    $query_options['where_cond'] = $this->_schemas[$class_name]['primaryKey'];
                }
            }
        }
    }

    private function _shouldRetryOnLastErrorWithException($exception)
    {
        $errorCode = '';

        // Get the error code
        if (!is_null($this->_lastErrorInfo) && isset($this->_lastErrorInfo[0])) {
            $errorCode = $this->_lastErrorInfo[0];
            $this->_lastErrorInfo = null;
        } else {
            // connect to db fails
            if (preg_match('/SQLSTATE\[([0-9a-zA-Z]+)\]/', $exception->getMessage(), $matches)) {
                $errorCode = $matches[1];
            }
        }

        // Check if we should retry on such error
        return in_array($errorCode, self::$RETRY_ERROR_CODES);
    }

    /**
     * @return boolean
     **/
    private function _isLoggingEnabled()
    {
        return isset($this->_options['logging']) && $this->_options['logging'] === true
            && $this->_logger instanceof LoggerInterface;
    }

    private function _initTiming()
    {
        if ($this->_isLoggingEnabled()) {
            $this->_profilingData = array();
        }
    }

    /**
     * @param string   $tag
     * @return null
     **/
    private function _doTiming($tag)
    {
        if ($this->_isLoggingEnabled()) {
            $this->_profilingData[$tag] = gettimeofday(true);
        }
    }

    /**
     * @param string $query
     * @param array $query_params
     * @param array $query_options
     * @param array $query_result
     **/
    private function _logQuery($query, $query_params, $query_options, $query_result)
    {
        if ($this->_isLoggingEnabled()) {
            $this->_logger->debug($query, array(
                'query_params'  => $query_params,
                'query_options' => $query_options,
                'query_timing'  => $this->_profilingData,
                'query_result'  => $query_result,
            ));
        }
    }

    /**
     * @param  string  $dsn
     * @param  integer $default
     * @return integer
     */
    private function _getConnectTimeoutFromDsn($dsn, $default = 30)
    {
        if (!$dsn) {
            return $default;
        }

        // Parse dns
        if (preg_match('/\Wconnect_timeout=(\d+)/', $dsn, $matches) !== 1) {
            return $default;
        }

        return intval($matches[1]);
    }

    private function _addUseSlaveDefault($query_options)
    {
        // Honor manager's options
        if (!isset($query_options['use_slave']) && $this->_options[self::OPTION_USE_SLAVE_BY_DEFAULT] === true) {
            $query_options['use_slave'] = true;
        }

        return $query_options;
    }

    private function _incQueryCount($query_options)
    {
        $this->_stats['queries_count']++;

        if (isset($query_options['use_slave']) && $query_options['use_slave'] === true) {
            $this->_stats['queries_on_slave_count']++;
        } else {
            $this->_stats['queries_on_master_count']++;
        }
    }

}
