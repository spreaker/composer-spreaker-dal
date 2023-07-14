<?php

$user = 'daluser';
$password = 'test4dal';
$host = getenv('POSTGRES_HOST');
$port = getenv('POSTGRES_PORT');

return array(
    //  one shard, no slaves
    'OneShardWithoutSlaves' => [
        'persistent_connections' => true,
        'shards' => [
            'shard1' => [
                'master' => "pgsql:port=$port;host=$host;dbname=dal_test;user=$user;password=$password",
                'slaves' => [],
                'default' => true,
            ],
        ],
    ],

    // two shards, no slaves
    'TwoShardsWithoutSlaves' => [
        'persistent_connections' => false,
        'logging' => true,
        'shards' => [
            'shard1' => [
                'master' => "pgsql:port=$port;host=$host;dbname=dal_test_1;user=$user;password=$password",
                'slaves' => [],
                'default' => true,
            ],
            'shard2' => [
                'master' => "pgsql:port=$port;host=$host;dbname=dal_test_2;user=$user;password=$password",
                'slaves' => [],
                'tables' => ['dal_test_groups'],
            ],
        ],
    ],


    // two shards, with slaves
    'TwoShardsWithSlaves' => [
        'logging' => true,
        'slave_retries' => 2,
        'shards' => [
            'shard1' => [
                'master' => "pgsql:port=$port;host=$host;dbname=dal_test_1;user=$user;password=$password",
                'slaves' => [],
                'default' => true,
            ],
            'shard2' => [
                'master' => "pgsql:port=$port;host=$host;dbname=dal_test_2;user=$user;password=$password",
                'slaves' => ['default' => "pgsql:host=$host;dbname=dal_test_2;user=$user;password=$password"],
                'tables' => ['dal_test_groups'],
            ],
        ],
    ],

);
