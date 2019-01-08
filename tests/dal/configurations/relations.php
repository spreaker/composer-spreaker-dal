<?php

return array(
    /**
     * UserModel
     */
    'user.group' => array(
        'local_key'     => 'group_id',
        'remote_key'    => 'group_id',
        'local_setter'  => 'setGroup',
        'local_getter'  => 'getGroup',
        'type'          => 'ONE',
        'fetcher'       => 'DatabaseManagerTest::getUserGroup'
    ),

    /**
     * GroupModel
     */
    'group.users'   => array(
        'local_key'     => 'group_id',
        'remote_key'    => 'group_id',
        'local_setter'  => 'setUsers',
        'local_getter'  => 'getUsers',
        'type'          => 'MANY',
        'fetcher'       => 'DatabaseManagerTest::getGroupUsers'
    ),

    /**
     * Author of Episode
     */
    'episode.author' => array(
        'local_key'     => 'user_id',
        'remote_key'    => 'user_id',
        'local_setter'  => 'setAuthor',
        'local_getter'  => 'getAuthor',
        'type'          => 'ONE',
        'fetcher'       => 'UserProfileRepository::getUsersById'
    ),

    /**
     * Episodes of User
     */
    'user.episodes'  => array(
        'local_key'     => 'user_id',
        'remote_key'    => 'user_id',
        'local_setter'  => 'setEpisodes',
        'local_getter'  => 'getEpisodes',
        'type'          => 'MANY'
    ),

);
