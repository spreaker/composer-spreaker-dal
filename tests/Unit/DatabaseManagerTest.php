<?php

namespace Spreaker\Dal\Tests\Unit;

use PHPUnit\Framework\TestCase;
use Spreaker\Dal\Database\DatabaseManager;
use Spreaker\Dal\Tests\Fixtures\GroupModel;
use Spreaker\Dal\Tests\Fixtures\UserModel;
use Spreaker\Dal\Tests\Unit\Common\DalArrayCache;
use Spreaker\Dal\Tests\Unit\Common\DalEchoLogger;

class DatabaseManagerTest extends TestCase
{
    protected DatabaseManager $_db;

    protected $_drop = false;

    protected array $_databases = [];
    protected array $_schemas   = [];
    protected array $_relations = [];
    protected array $_cache = [];

    protected function setUp(): void
    {
        $this->_databases = include __DIR__ . "/../configurations/databases.php";
        $this->_schemas   = include __DIR__ . "/../configurations/schemas.php";
        $this->_relations = include __DIR__ . "/../configurations/relations.php";
        $this->_cache     = include __DIR__ . "/../configurations/cache.php";
    }

    protected function tearDown(): void
    {
        $this->_dropTables();

        // destroy connection
        unset($this->_db);
    }

    public function testInitialize()
    {
        // without shards key
        $opts = array();
        $gotException = false;
        try {
            $this->_db = new DatabaseManager($opts, array());
        } catch (\Exception $e) {
            $gotException = true;
        }
        $this->assertTrue($gotException, 'Expect got an Exception when initialize without shards');

        // without shards settings
        $opts = array('shards' => array());
        $gotException = false;
        try {
            $this->_db = new DatabaseManager($opts, array());
        } catch (\Exception $e) {
            $gotException = true;
        }
        $this->assertTrue($gotException, 'Expect got an Exception when initialize without shards settings');

        // without default shard
        $opts = array('shards' => array('shard1' => array('master' => null, 'slaves' => null, 'tables' => null)));
        $gotException = false;
        try {
            $this->_db = new DatabaseManager($opts, array());
        } catch (\Exception $e) {
            $gotException = true;
        }
        $this->assertTrue($gotException, 'Expect got an Exception when initialize without a default shard');

        // with 1 default shard
        $opts = array('shards' => array('shard1' => array('master' => null, 'slaves' => null, 'default' => true)));
        $gotException = false;
        try {
            $this->_db = new DatabaseManager($opts, array());
        } catch (\Exception $e) {
            $gotException = true;
        }
        $this->assertFalse($gotException, 'Expect no Exception when initialize with a default shard');

        $dbOptions = $this->_db->getOptions();
        $this->assertEquals($dbOptions['default_shard'], 'shard1');

        // with more than 1 default shard
        $opts = array(
            'shards' => array(
                'shard1' => array('master' => null, 'slaves' => null, 'default' => true),
                'shard2' => array('master' => null, 'slaves' => null, 'default' => true),
            ),
        );
        $gotException = false;
        try {
            $this->_db = new DatabaseManager($opts, array());
        } catch (\Exception $e) {
            $gotException = true;
        }
        $this->assertTrue($gotException, 'Expect no Exception when initialize with more than 1 default shard');
    }

    public function testInitWithPDOConnection()
    {
        // with 1 default shard
        $connection = new \PDO(
            $this->_databases['OneShardWithoutSlaves']['shards']['shard1']['master']
        );

        $opts = [
            'shards' => [
                'shard1' => [
                    'master' => $connection,
                    'slaves' => null,
                    'default' => true
                ]
            ]
        ];

        $gotException = false;
        try {
            $this->_db = new DatabaseManager($opts, array());
        } catch (\Exception $e) {
            $gotException = true;
        }
        $this->assertFalse($gotException, 'Expect no Exception when initialize with a default shard');
        $this->assertEquals($this->_db->connect(), $connection);
    }

    public function testGetConnectionParameters()
    {
        $test_dsn_master = 'pgsql:port=65432;dbname=testdb;user=test;password=p4ss';
        $opts = array('shards' => array('shard1' => array('master' => $test_dsn_master, 'slaves' => null, 'default' => true)));
        $this->_db = new DatabaseManager($opts, array());

        $this->assertEquals($this->_db->getConnectionParameters(), $test_dsn_master);
    }

    public function testFetchColumnWithResultCache()
    {
        $this->_initDatabaseByName('OneShardWithoutSlaves');
        $this->_createTables();
        $this->_setResultCacheDriver();
        $cacheDriver = $this->_db->getResultCacheDriver();

        // Create fixtures
        $user_1  = new UserModel(array('user_name' => 'Marco', 'group_id' => 1));
        $user_2  = new UserModel(array('user_name' => 'Peter', 'group_id' => 2));
        $this->_db->insert($user_1, array('table_name' => 'dal_test_users'));
        $this->_db->insert($user_2, array('table_name' => 'dal_test_users'));

        // Query with result caching the 1st time
        $this->_db->resetQueryCount();
        $cacheDriver->deleteAllKeys();

        $output = $this->_db->fetchColumn('SELECT user_name FROM dal_test_users WHERE user_name = ?', array('Marco'), array('use_cache' => true, 'cache_ttl' => 3600));
        $queryCount = $this->_db->getQueryCount();

        $this->assertTrue(is_array($output),      'returns an array even if the output is a single row');
        $this->assertEquals(1, count($output),    'returns an array with 1 element, if the output is a single row');
        $this->assertEquals('Marco', $output[0],  'returned user_name ok');
        $this->assertEquals(1, $queryCount,       'check query count ok');

        // Query with result caching the 2nd time
        $this->_db->resetQueryCount();
        $output = $this->_db->fetchColumn('SELECT user_name FROM dal_test_users WHERE user_name = ?', array('Marco'), array('use_cache' => true, 'cache_ttl' => 3600));
        $queryCount = $this->_db->getQueryCount();

        $this->assertTrue(is_array($output),      'returns an array even if the output is a single row');
        $this->assertEquals(1, count($output),    'returns an array with 1 element, if the output is a single row');
        $this->assertEquals('Marco', $output[0],  'returned user_name ok');
        $this->assertEquals(0, $queryCount,       'check query count ok');

        // Query with result caching the 3rd time
        $this->_db->resetQueryCount();
        $cacheDriver->deleteAllKeys();

        $output = $this->_db->fetchColumn('SELECT user_name FROM dal_test_users WHERE user_name = ?', array('Marco'), array('use_cache' => true, 'cache_ttl' => 3600));
        $queryCount = $this->_db->getQueryCount();

        $this->assertTrue(is_array($output),      'returns an array even if the output is a single row');
        $this->assertEquals(1, count($output),    'returns an array with 1 element, if the output is a single row');
        $this->assertEquals('Marco', $output[0],  'returned user_name ok');
        $this->assertEquals(1, $queryCount,       'check query count ok');

        // Query without result caching the 4th time
        $this->_db->resetQueryCount();

        $output = $this->_db->fetchColumn('SELECT user_name FROM dal_test_users WHERE user_name = ?', array('Marco'));
        $queryCount = $this->_db->getQueryCount();

        $this->assertTrue(is_array($output),      'returns an array even if the output is a single row');
        $this->assertEquals(1, count($output),    'returns an array with 1 element, if the output is a single row');
        $this->assertEquals('Marco', $output[0],  'returned user_name ok');
        $this->assertEquals(1, $queryCount,       'check query count ok');
    }

    public function testFetchOneWithResultCache()
    {
        $this->_initDatabaseByName('OneShardWithoutSlaves');
        $this->_createTables();
        $this->_setResultCacheDriver();
        $cacheDriver = $this->_db->getResultCacheDriver();

        // Create fixtures
        $user_1  = new UserModel(array('user_name' => 'Marco', 'group_id' => 1));
        $user_2  = new UserModel(array('user_name' => 'Peter', 'group_id' => 2));
        $this->_db->insert($user_1, array('table_name' => 'dal_test_users'));
        $this->_db->insert($user_2, array('table_name' => 'dal_test_users'));

        $this->_db->resetQueryCount();
        $cacheDriver->deleteAllKeys();

        $output = $this->_db->fetchOne('SELECT * FROM dal_test_users WHERE user_name = ?', array('Marco'), array('class_name' => UserModel::class, 'use_cache' => true, 'cache_ttl' => 3600));
        $queryCount = $this->_db->getQueryCount();

        $this->assertTrue($output instanceof UserModel,     'returns an UserModel object');
        $this->assertEquals('Marco', $output->data->user_name, 'returned user_name ok');
        $this->assertEquals(1, $output->data->group_id,        'returned group_id ok');
        $this->assertEquals(1, $queryCount,                 'check query count ok');

        $this->_db->resetQueryCount();
        $output = $this->_db->fetchOne('SELECT * FROM dal_test_users WHERE user_name = ?', array('Marco'), array('class_name' => UserModel::class, 'use_cache' => true, 'cache_ttl' => 3600));
        $queryCount = $this->_db->getQueryCount();

        $this->assertTrue($output instanceof UserModel,     'returns an UserModel object');
        $this->assertEquals('Marco', $output->data->user_name, 'returned user_name ok');
        $this->assertEquals(1, $output->data->group_id,        'returned group_id ok');
        $this->assertEquals(0, $queryCount,                 'check query count ok');


        $this->_db->resetQueryCount();
        $cacheDriver->deleteAllKeys();

        $output = $this->_db->fetchOne('SELECT * FROM dal_test_users WHERE user_name = ?', array('Marco'), array('class_name' => UserModel::class, 'use_cache' => true, 'cache_ttl' => 3600));
        $queryCount = $this->_db->getQueryCount();

        $this->assertTrue($output instanceof UserModel,     'returns an UserModel object');
        $this->assertEquals('Marco', $output->data->user_name, 'returned user_name ok');
        $this->assertEquals(1, $output->data->group_id,        'returned group_id ok');
        $this->assertEquals(1, $queryCount,                 'check query count ok');


        $this->_db->resetQueryCount();
        $output = $this->_db->fetchOne('SELECT * FROM dal_test_users WHERE user_name = ?', array('Marco'), array('class_name' => UserModel::class));
        $queryCount = $this->_db->getQueryCount();

        $this->assertTrue($output instanceof UserModel,     'returns an UserModel object');
        $this->assertEquals('Marco', $output->data->user_name, 'returned user_name ok');
        $this->assertEquals(1, $output->data->group_id,        'returned group_id ok');
        $this->assertEquals(1, $queryCount,                 'check query count ok');
    }

    public function testQueryWithResultCache()
    {
        $this->_initDatabaseByName('OneShardWithoutSlaves');
        $this->_createTables();
        $this->_setResultCacheDriver();
        $cacheDriver = $this->_db->getResultCacheDriver();

        // Create fixtures
        $user_1  = new UserModel(array('user_name' => 'Marco', 'group_id' => 1));
        $user_2  = new UserModel(array('user_name' => 'Peter', 'group_id' => 2));
        $this->_db->insert($user_1, array('table_name' => 'dal_test_users'));
        $this->_db->insert($user_2, array('table_name' => 'dal_test_users'));

        // Query with result caching the 1st time
        $this->_db->resetQueryCount();
        $cacheDriver->deleteAllKeys();

        $output = $this->_db->query('SELECT * FROM dal_test_users WHERE user_name = ?', array('Marco'), array('use_cache' => true, 'cache_ttl' => 3600));
        $queryCount = $this->_db->getQueryCount();

        $this->assertTrue(is_array($output),                'returns an array even if the output is a single row');
        $this->assertEquals(1, count($output),              'returns an array with 1 element, if the output is a single row');
        $this->assertEquals('Marco', $output[0]->user_name, 'returned user_name ok');
        $this->assertEquals(1, $output[0]->group_id,        'returned group_id ok');
        $this->assertEquals(1, $queryCount,                 'check query count ok');

        // Query with result caching the 2nd time
        $this->_db->resetQueryCount();
        $output = $this->_db->query('SELECT * FROM dal_test_users WHERE user_name = ?', array('Marco'), array('use_cache' => true, 'cache_ttl' => 3600));
        $queryCount = $this->_db->getQueryCount();

        $this->assertTrue(is_array($output),                'returns an array even if the output is a single row');
        $this->assertEquals(1, count($output),              'returns an array with 1 element, if the output is a single row');
        $this->assertEquals('Marco', $output[0]->user_name, 'returned user_name ok');
        $this->assertEquals(1, $output[0]->group_id,        'returned group_id ok');
        $this->assertEquals(0, $queryCount,                 'check query count ok');

        // Query with result caching the 3rd time
        $this->_db->resetQueryCount();
        $cacheDriver->deleteAllKeys();

        $output = $this->_db->query('SELECT * FROM dal_test_users WHERE user_name = ?', array('Marco'), array('use_cache' => true, 'cache_ttl' => 3600));
        $queryCount = $this->_db->getQueryCount();

        $this->assertTrue(is_array($output),                'returns an array even if the output is a single row');
        $this->assertEquals(1, count($output),              'returns an array with 1 element, if the output is a single row');
        $this->assertEquals('Marco', $output[0]->user_name, 'returned user_name ok');
        $this->assertEquals(1, $output[0]->group_id,        'returned group_id ok');
        $this->assertEquals(1, $queryCount,                 'check query count ok');

        // Query without result caching the 4th time
        $this->_db->resetQueryCount();

        $output = $this->_db->query('SELECT * FROM dal_test_users WHERE user_name = ?', array('Marco'));
        $queryCount = $this->_db->getQueryCount();

        $this->assertTrue(is_array($output),                'returns an array even if the output is a single row');
        $this->assertEquals(1, count($output),              'returns an array with 1 element, if the output is a single row');
        $this->assertEquals('Marco', $output[0]->user_name, 'returned user_name ok');
        $this->assertEquals(1, $output[0]->group_id,        'returned group_id ok');
        $this->assertEquals(1, $queryCount,                 'check query count ok');
    }

    public function testFetchWithResultCache()
    {
        $this->_initDatabaseByName('OneShardWithoutSlaves');
        $this->_createTables();
        $this->_setResultCacheDriver();
        $cacheDriver = $this->_db->getResultCacheDriver();

        // Create fixtures
        $user_1  = new UserModel(array('user_name' => 'Marco', 'group_id' => 1));
        $user_2  = new UserModel(array('user_name' => 'Peter', 'group_id' => 2));
        $this->_db->insert($user_1, array('table_name' => 'dal_test_users'));
        $this->_db->insert($user_2, array('table_name' => 'dal_test_users'));

        // Query with result caching the 1st time
        $this->_db->resetQueryCount();
        $cacheDriver->deleteAllKeys();

        $output = $this->_db->fetch('SELECT * FROM dal_test_users WHERE user_name = ?', array('Marco'), array('use_cache' => true, 'cache_ttl' => 3600));
        $queryCount = $this->_db->getQueryCount();

        $this->assertTrue(is_array($output),                'returns an array even if the output is a single row');
        $this->assertEquals(1, count($output),              'returns an array with 1 element, if the output is a single row');
        $this->assertEquals('Marco', $output[0]->user_name, 'returned user_name ok');
        $this->assertEquals(1, $output[0]->group_id,        'returned group_id ok');
        $this->assertEquals(1, $queryCount,                 'check query count ok');

        // Query with result caching the 2nd time
        $this->_db->resetQueryCount();
        $output = $this->_db->fetch('SELECT * FROM dal_test_users WHERE user_name = ?', array('Marco'), array('use_cache' => true, 'cache_ttl' => 3600));
        $queryCount = $this->_db->getQueryCount();

        $this->assertTrue(is_array($output),                'returns an array even if the output is a single row');
        $this->assertEquals(1, count($output),              'returns an array with 1 element, if the output is a single row');
        $this->assertEquals('Marco', $output[0]->user_name, 'returned user_name ok');
        $this->assertEquals(1, $output[0]->group_id,        'returned group_id ok');
        $this->assertEquals(0, $queryCount,                 'check query count ok');

        // Query with result caching the 3rd time
        $this->_db->resetQueryCount();
        $cacheDriver->deleteAllKeys();

        $output = $this->_db->fetch('SELECT * FROM dal_test_users WHERE user_name = ?', array('Marco'), array('use_cache' => true, 'cache_ttl' => 3600));
        $queryCount = $this->_db->getQueryCount();

        $this->assertTrue(is_array($output),                'returns an array even if the output is a single row');
        $this->assertEquals(1, count($output),              'returns an array with 1 element, if the output is a single row');
        $this->assertEquals('Marco', $output[0]->user_name, 'returned user_name ok');
        $this->assertEquals(1, $output[0]->group_id,        'returned group_id ok');
        $this->assertEquals(1, $queryCount,                 'check query count ok');

        // Query without result caching the 4th time
        $this->_db->resetQueryCount();

        $output = $this->_db->fetch('SELECT * FROM dal_test_users WHERE user_name = ?', array('Marco'));
        $queryCount = $this->_db->getQueryCount();

        $this->assertTrue(is_array($output),                'returns an array even if the output is a single row');
        $this->assertEquals(1, count($output),              'returns an array with 1 element, if the output is a single row');
        $this->assertEquals('Marco', $output[0]->user_name, 'returned user_name ok');
        $this->assertEquals(1, $output[0]->group_id,        'returned group_id ok');
        $this->assertEquals(1, $queryCount,                 'check query count ok');
    }

    public function testQueryShouldExecuteQueryAndReturnTheOutputRows()
    {
        $this->_initDatabaseByName('OneShardWithoutSlaves');
        $this->_createTables();

        // Create fixtures
        $user_1  = new UserModel(array('user_name' => 'Marco', 'group_id' => 1));
        $user_2  = new UserModel(array('user_name' => 'Peter', 'group_id' => 2));
        $this->_db->insert($user_1, array('table_name' => 'dal_test_users'));
        $this->_db->insert($user_2, array('table_name' => 'dal_test_users'));

        // Test
        $output = $this->_db->query('SELECT * FROM dal_test_users WHERE user_name = ?', array('Marco'));
        $this->assertTrue(is_array($output),                'returns an array even if the output is a single row');
        $this->assertEquals(1, count($output),              'returns an array with 1 element, if the output is a single row');
        $this->assertEquals('Marco', $output[0]->user_name, 'returned user_name ok');
        $this->assertEquals(1, $output[0]->group_id,        'returned group_id ok');

        $output = $this->_db->query('SELECT * FROM dal_test_users ORDER BY user_id');
        $this->assertTrue(is_array($output),                'returns an array on multiple output rows');
        $this->assertEquals(2, count($output),              'returned array contains the expected number of rows');
        $this->assertEquals('Marco', $output[0]->user_name, '#1 returned user_name ok');
        $this->assertEquals(1, $output[0]->group_id,        '#1 returned group_id ok');
        $this->assertEquals('Peter', $output[1]->user_name, '#2 returned user_name ok');
        $this->assertEquals(2, $output[1]->group_id,        '#2 returned group_id ok');
    }

    public function testQueryShouldExecuteQueryAndReturnTheNumberOfAffectedRowsOnNoResultOptionEnabled()
    {
        $this->_initDatabaseByName('OneShardWithoutSlaves');
        $this->_createTables();

        // Create fixtures
        $user_1  = new UserModel(array('user_name' => 'Marco', 'group_id' => 1));
        $user_2  = new UserModel(array('user_name' => 'Peter', 'group_id' => 2));
        $this->_db->insert($user_1, array('table_name' => 'dal_test_users'));
        $this->_db->insert($user_2, array('table_name' => 'dal_test_users'));

        // Test
        $output = $this->_db->query('SELECT * FROM dal_test_users WHERE user_name = ?', array('Marco'));
        $this->assertEquals(1, count($output), 'returns the number of affected rows on no_result option enabled');

        $output = $this->_db->query('SELECT * FROM dal_test_users ORDER BY user_id');
        $this->assertEquals(2, count($output), 'returns the number of affected rows on no_result option enabled');
    }

    public function testFetchReturnNoRecords()
    {
        $this->_initDatabaseByName('TwoShardsWithoutSlaves');
        $this->_createTables();

        $users = $this->_db->fetch('select u.* from dal_test_users u where user_id in (?)', array(0), array('class_name'=>UserModel::class));

        $this->assertEquals($users, array(), 'Fetch return no records.');
    }

    public function testFetchReturnStdObject()
    {
        $this->_initDatabaseByName('TwoShardsWithoutSlaves');
        $this->_createTables();

        $user1  = $this->_db->fetchOne('insert into dal_test_users (user_name, group_id) values (?, ?) returning *', array('User 1', 1));
        $this->assertEquals($user1->user_name, 'User 1');
        $this->assertEquals($user1->group_id, 1);
    }

    public function testFetchReturnArray()
    {
        $this->_initDatabaseByName('TwoShardsWithoutSlaves');
        $this->_createTables();

        $user = $this->_db->fetchOne('insert into dal_test_users (user_name, group_id) values (?, ?) returning *', array('User 1', 1), array('return_array' => true));
        $this->assertTrue(is_array($user));
        $this->assertEquals($user['user_name'], 'User 1');
        $this->assertEquals($user['group_id'], 1);

        $user = $this->_db->fetch('insert into dal_test_users (user_name, group_id) values (?, ?) returning *', array('User 1', 1), array('return_array' => true));
        $this->assertTrue(is_array($user));
        $this->assertEquals($user[0]['user_name'], 'User 1');
        $this->assertEquals($user[0]['group_id'], 1);
    }

    public function testFetchReturnRecords()
    {
        $this->_initDatabaseByName('TwoShardsWithoutSlaves');
        $this->_createTables();

        $user1  = $this->_db->fetchOne('insert into dal_test_users (user_name, group_id) values (?, ?) returning *',
                                       array('User 1', 1), array('class_name'=>UserModel::class));
        $this->assertEquals($user1->data->user_name, 'User 1');
        $this->assertEquals($user1->data->group_id, 1);

        $user2  = $this->_db->fetchOne('insert into dal_test_users (user_name, group_id) values (?, ?) returning *',
                                       array('User 2', 1), array('class_name'=>UserModel::class));
        $this->assertEquals($user2->data->user_name, 'User 2');
        $this->assertEquals($user2->data->group_id, 1);

        $group1 = $this->_db->fetchOne('insert into dal_test_groups (group_name, group_type) values (?, ?) returning *',
                                       array('Group 1', 1), array('class_name'=> GroupModel::class));
        $this->assertEquals($group1->data->group_name, 'Group 1');
        $this->assertEquals($group1->data->group_type, 1);

        $users = $this->_db->fetch('select u.* from dal_test_users u where user_id in (?, ?) order by user_id asc', array(1, 2), array('class_name'=>UserModel::class));

        $this->assertEquals(count($users), 2, 'Fetch return 2 records.');

        $this->assertEquals($users[0]->data->user_id, 1);
        $this->assertEquals($users[1]->data->user_id, 2);

        $this->assertEquals($users[0]->data->user_name, 'User 1');
        $this->assertEquals($users[1]->data->user_name, 'User 2');
    }

    public function testInsertShouldThrowExceptionOnEmptyArray()
    {
        $this->expectException(\Exception::class);
        $this->expectExceptionMessage('Invalid argument: input records should be an instance of Model or an array of Model');

        $this->_initDatabaseByName('OneShardWithoutSlaves');
        $this->_createTables();

        $this->_db->insert([]);
    }

    public function testInsertShouldThrowExceptionOnFalseInput()
    {
        $this->expectException(\Exception::class);
        $this->expectExceptionMessage('Invalid argument: input records should be an instance of Model or an array of Model');

        $this->_initDatabaseByName('OneShardWithoutSlaves');
        $this->_createTables();

        $this->_db->insert(false);
    }

    public function testInsertShouldSupportSingleInsertionsAndReturnUpdatedInputModel()
    {
        $this->_initDatabaseByName('OneShardWithoutSlaves');
        $this->_createTables();

        $input  = new UserModel(array('user_name' => 'Marco', 'group_id' => 1));
        $output = $this->_db->insert($input, array('table_name' => 'dal_test_users'));

        $this->assertInstanceOf(UserModel::class, $output,   'returns an instance of the model');
        $this->assertSame($input, $output,              'returns the same input instance');
        $this->assertNotEmpty($output->data->user_id,   'returns a model with primary key set');

        // Check if records have been inserted
        $actuals = $this->_db->fetch('select * from dal_test_users order by user_id', array(), array('class_name'=>UserModel::class));
        $this->assertEquals($actuals[0]->data, $output->data,       'returned data is the same of inserted one');
        $this->assertEquals('Marco', $actuals[0]->data->user_name,  'inserted user_name ok');
        $this->assertEquals(1, $actuals[0]->data->group_id,         'inserted group_id ok');
    }

    public function testInsertShouldSupportMultipleInsertionsAndReturnMultipleModels()
    {
        $this->_initDatabaseByName('OneShardWithoutSlaves');
        $this->_createTables();

        $input_1 = new UserModel(array('user_name' => 'Marco', 'group_id' => 1));
        $input_2 = new UserModel(array('user_name' => 'Rocco', 'group_id' => 2));
        $input_3 = new UserModel(array('user_name' => 'Peter', 'group_id' => 3));

        $output = $this->_db->insert(array($input_1, $input_2, $input_3), array('table_name' => 'dal_test_users'));
        $this->assertTrue(is_array($output),    'returns an array');
        $this->assertEquals(3, count($output),  'output array contains the same number of elements of input array');
        $this->assertEquals('Marco', $output[0]->data->user_name, '#1 returned data ok');
        $this->assertEquals('Rocco', $output[1]->data->user_name, '#2 returned data ok');
        $this->assertEquals('Peter', $output[2]->data->user_name, '#3 returned data ok');
        $this->assertSame($input_1, $output[0], '#1 returned same instance');
        $this->assertSame($input_2, $output[1], '#2 returned same instance');
        $this->assertSame($input_3, $output[2], '#3 returned same instance');

        // Check if records have been inserted
        $actuals = $this->_db->fetch('select * from dal_test_users order by user_id', array(), array('class_name'=> UserModel::class));

        $this->assertEquals($actuals[0]->data, $output[0]->data,       '#1 returned data is the same of inserted one');
        $this->assertEquals('Marco', $actuals[0]->data->user_name,     '#1 inserted data ok');
        $this->assertEquals(1, $actuals[0]->data->group_id,            '#1 inserted data ok');
        $this->assertEquals($actuals[1]->data, $output[1]->data,       '#2 returned data is the same of inserted one');
        $this->assertEquals('Rocco', $actuals[1]->data->user_name,     '#2 inserted data ok');
        $this->assertEquals(2, $actuals[1]->data->group_id,            '#2 inserted data ok');
        $this->assertEquals($actuals[2]->data, $output[2]->data,       '#3 returned data is the same of inserted one');
        $this->assertEquals('Peter', $actuals[2]->data->user_name,     '#3 inserted data ok');
        $this->assertEquals(3, $actuals[2]->data->group_id,            '#3 inserted data ok');
    }

    public function testInsertShouldReturnNullOnNoResultOptionEnabled()
    {
        $this->_initDatabaseByName('OneShardWithoutSlaves');
        $this->_createTables();

        $input_1 = new UserModel(array('user_name' => 'Marco', 'group_id' => 1));
        $input_2 = new UserModel(array('user_name' => 'Rocco', 'group_id' => 2));
        $input_3 = new UserModel(array('user_name' => 'Peter', 'group_id' => 3));

        // Multi insert
        $output = $this->_db->insert($input_1, array('table_name' => 'dal_test_users', 'no_result' => true));
        $this->assertEquals(null, $output, 'returns null on single insert if no_result is true');

        // Multi insert
        $output = $this->_db->insert(array($input_1, $input_2, $input_3), array('table_name' => 'dal_test_users', 'no_result' => true));
        $this->assertEquals(null, $output, 'returns null on multi insert if no_result is true');
    }

    public function testFetchColumn()
    {
        $this->_initDatabaseByName('TwoShardsWithoutSlaves');
        $this->_createTables();

        // setup
        $input  = new UserModel(array('user_name' => "Evil '\" name", 'group_id' => 1));
        $output = $this->_db->insert($input, array('table_name' => 'dal_test_users'));

        // fetchColumn, select single column
        $user_ids = $this->_db->fetchColumn('select u.user_id from dal_test_users u');
        $this->assertTrue(is_array($user_ids));
        $this->assertEquals(count($user_ids), 1);
        $this->assertEquals($user_ids[0], $output->data->user_id);

        // fetchColumn, select multiple columns
        $user_ids = $this->_db->fetchColumn('select u.user_id, u.user_name from dal_test_users u');
        $this->assertTrue(is_array($user_ids));
        $this->assertEquals(count($user_ids), 1);
        $this->assertEquals($user_ids[0], $output->data->user_id);

        // fetchColumn, select multiple columns
        $user_names = $this->_db->fetchColumn('select u.user_name, u.user_id from dal_test_users u');
        $this->assertTrue(is_array($user_names));
        $this->assertEquals(count($user_names), 1);
        $this->assertEquals($user_names[0], $output->data->user_name);

        // fetchColumn, with valid fetch_column option
        $user_ids = $this->_db->fetchColumn('select u.user_id, u.user_name from dal_test_users u', array(), array('fetch_column' => 'user_id'));
        $this->assertTrue(is_array($user_ids));
        $this->assertEquals(count($user_ids), 1);
        $this->assertEquals($user_ids[0], $output->data->user_id);

        // fetchColumn, with valid fetch_column option
        $user_names = $this->_db->fetchColumn('select u.user_id, u.user_name from dal_test_users u', array(), array('fetch_column' => 'user_name'));
        $this->assertTrue(is_array($user_names));
        $this->assertEquals(count($user_names), 1);
        $this->assertEquals($user_names[0], $output->data->user_name);

        // fetchColumn, with invalid fetch_column option
        $gotException = false;
        try {
            $user_ids = $this->_db->fetchColumn('select u.user_id from dal_test_users u', array(), array('fetch_column' => 'user_name'));
        } catch (\Exception $e) {
            $gotException = true;
        }
        $this->assertTrue($gotException);
    }

    public function testInsertShouldQuoteInsertedData()
    {
        $this->_initDatabaseByName('TwoShardsWithoutSlaves');
        $this->_createTables();

        $input  = new UserModel(array('user_name' => "Evil '\" name", 'group_id' => 1));
        $output = $this->_db->insert($input, array('table_name' => 'dal_test_users'));

        $this->assertInstanceOf(UserModel::class, $output,   'returns an instance of the model');
        $this->assertSame($input, $output,              'returns the same input instance');
        $this->assertNotEmpty($output->data->user_id,   'returns a model with primary key set');

        // Check if records have been inserted
        $actuals = $this->_db->fetch('select * from dal_test_users order by user_id', array(), array('class_name'=>UserModel::class));
        $this->assertEquals($actuals[0]->data, $output->data,               'returned data is the same of inserted one');
        $this->assertEquals("Evil '\" name", $actuals[0]->data->user_name,  'inserted user_name ok');
        $this->assertEquals(1, $actuals[0]->data->group_id,                 'inserted group_id ok');
    }

    public function testSlaveRetry()
    {
        $this->_initDatabaseByName('TwoShardsWithSlaves');
        $this->_createTables();

        $opts = $this->_db->getOptions();

        $param = $this->_db->getConnectionParameters('dal_test_groups', true);
        $this->assertEquals($param, $opts['shards']['shard2']['slaves']['default']);

        $param = $this->_db->getConnectionParameters('dal_test_groups', false);
        $this->assertEquals($param, $opts['shards']['shard2']['master']);

        $group = new GroupModel(array('group_name' => 'group 1', 'group_type' => 1));
        $result = $this->_db->insert($group);

        $model  = $this->_db->fetchOne('select * from dal_test_groups', array(), array('use_slave'=>true));
        $this->assertEquals($model->group_id, $group->data->group_id);
        $this->assertEquals($model->group_name, $group->data->group_name);
        $this->assertEquals($model->group_type, $group->data->group_type);

        $models = $this->_db->fetch('select * from dal_test_groups', array(), array('use_slave'=>true));

        $this->assertEquals(count($models), 1);
        $this->assertEquals($models[0]->group_id, $group->data->group_id);
        $this->assertEquals($models[0]->group_name, $group->data->group_name);
        $this->assertEquals($models[0]->group_type, $group->data->group_type);
    }

    public function testUpdateWithVariousQueryOptions()
    {
        $this->_initDatabaseByName('TwoShardsWithoutSlaves');
        $this->_createTables();

        // insert/returning the record
        $record = new UserModel(array('user_name' => 'Who am I?', 'group_id' => 1), false);
        $result = $this->_db->insert($record);
        $this->assertTrue($result instanceof UserModel);
        $this->assertTrue($record->data->user_id > 0);
        $this->assertEquals($record->data->user_name, 'Who am I?');
        $this->assertEquals($record->data->group_id, 1);

        $gotException = false;
        $user = new UserModel(array('user_name' => 'joy', 'user_id' => $record->data->user_id), false);
        try {
            $retval = $this->_db->update($user);
        } catch (\Exception $e) {
            $gotException = true;
        }
        $this->assertFalse($gotException);

        $gotException = false;
        $user = new UserModel(array('user_name' => 'joy', 'user_id' => $record->data->user_id), false);
        try {
            $retval = $this->_db->update($user, array('table_name' => 'dal_test_users'));
        } catch (\Exception $e) {
            $gotException = true;
        }
        $this->assertFalse($gotException);

        $gotException = false;
        $user = new UserModel(array('user_name' => 'joy', 'user_id' => $record->data->user_id), false);
        try {
            $retval = $this->_db->update($user, array('table_name' => 'dal_test_users', 'where_cond' => 'user_id'));
        } catch (\Exception $e) {
            $gotException = true;
        }
        $this->assertFalse($gotException);

        $gotException = false;
        $user = new UserModel(array('user_name' => 'joy'), false);
        try {
            $retval = $this->_db->update($user, array('table_name' => 'dal_test_users', 'where_cond' => 'user_id', 'class_name' => UserModel::class));
        } catch (\Exception $e) {
            $gotException = true;
        }
        $this->assertTrue($gotException);
        $this->assertEquals($e->getMessage(), 'record does not have property: user_id');

        $gotException = false;
        $user = new UserModel(array('user_name' => 'joy'), false);
        try {
            $retval = $this->_db->update($user, array('table_name' => 'dal_test_users', 'where_cond' => array('user_id'), 'class_name' => UserModel::class));
        } catch (\Exception $e) {
            $gotException = true;
        }
        $this->assertTrue($gotException);
        $this->assertEquals($e->getMessage(), 'record does not have property: user_id');

        $gotException = false;
        $user = new UserModel(array('user_name' => 'joy'), false);
        try {
            $retval = $this->_db->update($user, array('table_name' => 'dal_test_users', 'where_cond' => array('user_id' => $record->data->user_id), 'class_name' => UserModel::class));
        } catch (\Exception $e) {
            $gotException = true;
        }
        $this->assertFalse($gotException);

        $gotException = false;
        $user = new UserModel(array('user_name' => 'joy', 'user_id' => $record->data->user_id), false);
        try {
            $retval = $this->_db->update($user, array('table_name' => 'dal_test_users', 'where_cond' => 'user_id', 'class_name' => UserModel::class));
        } catch (\Exception $e) {
            $gotException = true;
        }
        $this->assertFalse($gotException);

        // update PK
        $gotException = false;
        $user = new UserModel(array('user_name' => 'joy', 'user_id' => 42, 'banned' => false), false);
        try {
            $retval = $this->_db->update($user, array('table_name' => 'dal_test_users', 'where_cond' => array('user_id'=>$record->data->user_id), 'class_name' => UserModel::class, 'no_result'=>true));
        } catch (\Exception $e) {
            $gotException = true;
        }
        $this->assertFalse($gotException);
        $this->assertEquals($retval, 1);

        // fetch the record back by user_id
        $newUser = $this->_db->fetchOne('select u.* from dal_test_users u where user_id = :user_id',
            array(':user_id' => 42), array('class_name'=>UserModel::class));
        $this->assertTrue($newUser instanceof UserModel);
        $this->assertEquals($newUser->data->user_id, $user->data->user_id);
        $this->assertEquals($newUser->data->user_name, $user->data->user_name);
        $this->assertEquals($newUser->data->group_id, $record->data->group_id);
        $this->assertEquals($newUser->data->banned, $user->data->banned);
    }

    public function testDelete()
    {
        $this->_initDatabaseByName('TwoShardsWithoutSlaves');
        $this->_createTables();

        // insert/returning the record
        $record = new UserModel(array('user_name' => 'Who am I?', 'group_id' => 1), false);
        $result = $this->_db->insert($record, array('table_name' => 'dal_test_users', 'class_name' => UserModel::class));
        $this->assertTrue($result instanceof UserModel);
        $this->assertTrue($record->data->user_id > 0);
        $this->assertEquals($record->data->user_name, 'Who am I?');
        $this->assertEquals($record->data->group_id, 1);

        // delete without user_id property
        $gotException = false;
        $user = new UserModel(null, false);
        try {
            $retval = $this->_db->delete($user, array('table_name' => 'dal_test_users', 'where_cond' => 'user_id'));
        } catch (\Exception $e) {
            $gotException = true;
        }
        $this->assertTrue($gotException);
        $this->assertEquals($e->getMessage(), 'record does not have property: user_id');

        // delete without where_cond
        $gotException = false;
        $user = new UserModel(array('user_id' => $record->data->user_id), false);
        try {
            $retval = $this->_db->delete($user, array('table_name' => 'dal_test_users', 'where_cond' => ''));
        } catch (\Exception $e) {
            $gotException = true;
        }
        $this->assertFalse($gotException);
        $this->assertEquals($retval, 1);

        // delete ok
        $gotException = false;
        $user = new UserModel(array('user_id' => $record->data->user_id), false);
        try {
            $retval = $this->_db->delete($user, array('table_name' => 'dal_test_users', 'where_cond' => 'user_id'));
        } catch (\Exception $e) {
            $gotException = true;
        }
        $this->assertFalse($gotException);
        $this->assertEquals($retval, 0);

        // try fetch the record back after delete
        $newUser = $this->_db->fetchOne('select u.* from dal_test_users u where user_id = :user_id',
            array(':user_id' => $record->data->user_id), array('class_name'=>UserModel::class));
        $this->assertEquals($newUser, null);

        // insert/returning the record
        $record = new UserModel(array('user_name' => 'Who am I?', 'group_id' => 1), false);
        $result = $this->_db->insert($record, array('table_name' => 'dal_test_users', 'class_name' => UserModel::class));
        $this->assertTrue($result instanceof UserModel);
        $this->assertTrue($record->data->user_id > 1);
        $this->assertEquals($record->data->user_name, 'Who am I?');
        $this->assertEquals($record->data->group_id, 1);

        // delete by multiple column key
        $gotException = false;
        $user = new UserModel(array('user_name' => $record->data->user_name, 'group_id' => 1), false);
        try {
            $retval = $this->_db->delete($user, array('table_name' => 'dal_test_users', 'where_cond' => array('user_name', 'group_id')));
        } catch (\Exception $e) {
            $gotException = true;
        }
        $this->assertFalse($gotException);
        $this->assertEquals($retval, 1);

        // try fetch the record back after delete
        $newUser = $this->_db->fetchOne('select u.* from dal_test_users u where user_id = :user_id',
            array(':user_id' => $record->data->user_id), array('class_name'=>UserModel::class));
        $this->assertEquals($newUser, null);
    }

    /**
     * @param string $name  top level keys in configurations/databases.php
     */
    private function _initDatabaseByName($name)
    {
        $this->_db = new DatabaseManager($this->_databases[$name], $this->_schemas);
        $this->checkVerbose();
    }

    private function checkVerbose()
    {
        if ($this->_db && in_array('--verbose', $_SERVER['argv'])) {
            $this->_db->setLogger(new DalEchoLogger());
        }
    }

    private function _setResultCacheDriver()
    {
        if ($this->_db instanceof DatabaseManager) {

            // cacheDriver
            $driver = new DalArrayCache();

            // set to database manager
            $this->_db->setResultCacheDriver($driver);
        }
    }

    private function _createTables()
    {
        $queries = array();
        $queries['dal_test_users']  = 'create table if not exists dal_test_users (user_id serial not null primary key, user_name varchar(64) not null, banned bool default false, group_id integer)';
        $queries['dal_test_groups'] = 'create table if not exists dal_test_groups (group_id serial not null primary key, group_name varchar(64) not null, group_type smallint not null)';

        foreach ($queries as $table_name => $query) {
            $this->_db->query($query, array(), array('table_name' => $table_name));
        }

        $this->_drop = true;
    }

    private function _dropTables()
    {
        if (!$this->_db || !$this->_drop) {
            return;
        }

        $queries = array();
        $queries['dal_test_users']  = 'drop table if exists dal_test_users';
        $queries['dal_test_groups'] = 'drop table if exists dal_test_groups';

        foreach ($queries as $table_name => $query) {
            $this->_db->query($query, array(), array('table_name' => $table_name));
        }
    }

}
