<?php

use Spreaker\Dal\Tests\Fixtures\BarGroupModel;
use Spreaker\Dal\Tests\Fixtures\FooGroupModel;
use Spreaker\Dal\Tests\Fixtures\GroupModel;
use Spreaker\Dal\Tests\Fixtures\UserModel;

return array(
    UserModel::class => array(
        'tableName'  => 'dal_test_users',
        'primaryKey' => array('user_id'),
    ),

    GroupModel::class => array(
        'tableName'  => 'dal_test_groups',
        'primaryKey' => array('group_id'),
        'column'  => 'group_type',
        'classes' => array(
            1 => FooGroupModel::class,
            2 => BarGroupModel::class
        )
    )
);
