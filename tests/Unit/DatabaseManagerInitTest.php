<?php

namespace Spreaker\Dal\Tests\Unit;

use PHPUnit\Framework\TestCase;
use Spreaker\Dal\Database\DatabaseManager;
use Spreaker\Dal\Tests\Fixtures\GroupModel;
use Spreaker\Dal\Tests\Fixtures\UserModel;
use Spreaker\Dal\Tests\Unit\Common\DalArrayCache;
use Spreaker\Dal\Tests\Unit\Common\DalEchoLogger;

class DatabaseManagerInitTest extends TestCase
{
    protected array $_databases = [];

    protected function setUp(): void
    {
        $this->_databases = include __DIR__ . "/../configurations/databases.php";
    }

    public function testShouldThrowExceptionWhenConfigMissShardsKey()
    {
        $this->expectException(\Exception::class);
        $this->expectExceptionMessage('parameter $databases should be an array that contains an array with key "shards"');

        new DatabaseManager([], []);
    }

    public function testShouldThrowExceptionWhenConfigMissShardsSettings()
    {
        $this->expectException(\Exception::class);
        $this->expectExceptionMessage('No default shard found in the database configuration file!');

        new DatabaseManager(['shards' => []], []);
    }

    public function testShouldThrowExceptionWhenConfigMissDefaultShard()
    {
        $this->expectException(\Exception::class);
        $this->expectExceptionMessage('No default shard found in the database configuration file!');

        // without default shard
        $opts = [
            'shards' => [
                'shard1' => [
                    'master' => null,
                    'slaves' => null,
                    'tables' => null
                ]
            ]
        ];

        new DatabaseManager($opts, []);
    }

    public function testShouldThrowExceptionWithMoreThanOneDefaultShard()
    {
        $this->expectException(\Exception::class);
        $this->expectExceptionMessage('Specified more than ONE default shards in the database configuration file!');

        // with more than 1 default shard
        $opts = [
            'shards' => [
                'shard1' => ['master' => null, 'slaves' => null, 'default' => true],
                'shard2' => ['master' => null, 'slaves' => null, 'default' => true],
            ],
        ];

        new DatabaseManager($opts, []);
    }

    public function testShouldCreateAnInstanceWithOneDefaultShard()
    {
        // with 1 default shard
        $opts = [
            'shards' => [
                'shard1' => [
                    'master' => null,
                    'slaves' => null,
                    'default' => true
                ]
            ]
        ];

        $db = new DatabaseManager($opts, []);

        $this->assertInstanceOf(DatabaseManager::class, $db);
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

        $db = new DatabaseManager($opts, array());

        $this->assertEquals($db->connect(), $connection);
    }

    public function testGetConnectionParameters()
    {
        $test_dsn_master = 'pgsql:port=65432;dbname=testdb;user=test;password=p4ss';
        $opts = [
            'shards' => [
                'shard1' => [
                    'master' => $test_dsn_master,
                    'slaves' => null,
                    'default' => true
                ]
            ]
        ];

        $db = new DatabaseManager($opts, []);

        $this->assertEquals($db->getConnectionParameters(), $test_dsn_master);
    }

    public function testInitWithPersistentConnections()
    {
        $db = new DatabaseManager($this->_databases['OneShardWithoutSlaves'], []);
        $pdo = $db->connect();

        $this->assertTrue($db->getOptions()['persistent_connections']);
        $this->assertTrue($pdo->getAttribute(\PDO::ATTR_PERSISTENT));
    }

    //test init with persistent connections
    public function testInitWithNoPersistentConnection()
    {
        $db = new DatabaseManager($this->_databases['TwoShardsWithoutSlaves'], []);
        $pdo = $db->connect();

        $this->assertFalse($db->getOptions()['persistent_connections']);
        $this->assertFalse($pdo->getAttribute(\PDO::ATTR_PERSISTENT));
    }

    //test init with persistent connections
    public function testInitWithoutSettingSetPersistentConnectionToFalse()
    {
        $db = new DatabaseManager($this->_databases['TwoShardsWithSlaves'], []);
        $pdo = $db->connect();

        $this->assertFalse($db->getOptions()['persistent_connections']);
        $this->assertFalse($pdo->getAttribute(\PDO::ATTR_PERSISTENT));
    }
}
