<?php

return array(

    'UserModel' => array(
        'tableName'  => 'dal_test_users',
        'primaryKey' => array('user_id'),
    ),

    'GroupModel' => array(
        'tableName'  => 'dal_test_groups',
        'primaryKey' => array('group_id'),
        'column'  => 'group_type',
        'classes' => array(
            1 => 'FooGroupModel',
            2 => 'BarGroupModel',
        )
    )
);
