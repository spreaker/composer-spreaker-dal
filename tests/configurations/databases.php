<?php

return array(
    //  one shard, no slaves
    'OneShardWithoutSlaves' => array(
        'shards' => array(
            'shard1' => array(
                'master' => 'pgsql:host=postgresql-test;dbname=dal_test;user=daluser;password=test4dal',
                'slaves' => array(),
                'default' => true,
            ),
        ),
    ),

    // two shards, no slaves
    'TwoShardsWithoutSlaves' => array(
        'logging' => true,
        'shards' => array(
            'shard1' => array(
                'master' => 'pgsql:host=postgresql-test;dbname=dal_test_1;user=daluser;password=test4dal',
                'slaves' => array(),
                'default' => true,
            ),
            'shard2' => array(
                'master' => 'pgsql:host=postgresql-test;dbname=dal_test_2;user=daluser;password=test4dal',
                'slaves' => array(),
                'tables' => array('dal_test_groups'),
            ),
        ),
    ),


    // two shards, with slaves
    'TwoShardsWithSlaves' => array(
        'logging' => true,
        'slave_retries' => 2,
        'shards' => array(
            'shard1' => array(
                'master' => 'pgsql:host=postgresql-test;dbname=dal_test_1;user=daluser;password=test4dal',
                'slaves' => array(),
                'default' => true,
            ),
            'shard2' => array(
                'master' => 'pgsql:host=postgresql-test;dbname=dal_test_2;user=daluser;password=test4dal',
                'slaves' => array('default' => 'pgsql:host=postgresql-test;dbname=dal_test_2;user=daluser;password=test4dal'),
                'tables' => array('dal_test_groups'),
            ),
        ),
    ),

);
