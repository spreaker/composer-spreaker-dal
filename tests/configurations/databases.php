<?php

return array(
    //  one shard, no slaves
    'OneShardWithoutSlaves' => [
        'persistent_connections' => true,
        'shards' => [
            'shard1' => [
                'master' => 'pgsql:host=postgresql-test;dbname=dal_test;user=daluser;password=test4dal',
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
                'master' => 'pgsql:host=postgresql-test;dbname=dal_test_1;user=daluser;password=test4dal',
                'slaves' => [],
                'default' => true,
            ],
            'shard2' => [
                'master' => 'pgsql:host=postgresql-test;dbname=dal_test_2;user=daluser;password=test4dal',
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
                'master' => 'pgsql:host=postgresql-test;dbname=dal_test_1;user=daluser;password=test4dal',
                'slaves' => [],
                'default' => true,
            ],
            'shard2' => [
                'master' => 'pgsql:host=postgresql-test;dbname=dal_test_2;user=daluser;password=test4dal',
                'slaves' => ['default' => 'pgsql:host=postgresql-test;dbname=dal_test_2;user=daluser;password=test4dal'],
                'tables' => ['dal_test_groups'],
            ],
        ],
    ],

);
