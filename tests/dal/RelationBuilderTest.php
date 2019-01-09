<?php

use Spreaker\Dal\Relation\RelationBuilder;

class RelationBuilderTest extends PHPUnit_Framework_TestCase
{
    protected $_relations = null;

    protected $_relationBuilder = null;

    public static function setUpBeforeClass()
    {
        require_once __DIR__ . '/../../src/Autoloader.php';
        Spreaker\Autoloader::register();
    }


    protected function setUp()
    {
        // load models/configurations
        $this->loadAllConfigurations();
        $this->loadAllModelClasses();

        // init relation builder
        $this->_relationBuilder = new RelationBuilder($this->_relations);
    }

    /**
     * get configurations of databases/relations/schemas via including an external php file.
     * @param  string $type
     * @return array
     */
    private function loadConfiguration($type)
    {
        $types = array('relations');
        if (in_array($type, $types)) {
            return include __DIR__ . "/configurations/$type.php";
        } else {
            return array();
        }
    }

    private function loadAllConfigurations()
    {
        $this->_relations = $this->loadConfiguration('relations');
    }

    private function loadModelClass($class_name)
    {
        require_once __DIR__ . "/fixtures/$class_name.php";
    }

    private function loadAllModelClasses()
    {
        $classes   = array('UserModel', 'EpisodeModel', 'ImageModel', 'ShowModel');
        foreach ($classes as $class) {
            $this->loadModelClass($class);
        }
    }

    public function testCombineWithManyOneRelationAndSingleModel()
    {
        $episode = new EpisodeModel((object) array('episode_id' => 1, 'user_id' => 1));

        $users = array(
            new UserModel((object) array('user_id' => 1, 'fullname' => 'Rocco Zanni')),
            new UserModel((object) array('user_id' => 2, 'fullname' => 'Marco Pracucci'))
        );

        $episode = $this->_relationBuilder->combine($episode, $users, 'episode.author');

        $this->assertEquals($episode->getAuthor(), $users[0], 'Relation ok');
    }

    public function testCombineWithOneManyRelationWithEmptyRemote()
    {
        $user     = new UserModel((object) array('user_id' => 1, 'fullname' => 'Rocco Zanni'));
        $episodes =  array();

        $user = $this->_relationBuilder->combine($user, $episodes, 'user.episodes');

        $this->assertEquals($user->getEpisodes(), array(), 'Empty array');
    }

    public function testCombineWithManyOneRelation()
    {
        $episodes = array(
            new EpisodeModel((object) array('episode_id' => 1, 'user_id' => 1)),
            new EpisodeModel((object) array('episode_id' => 2, 'user_id' => 1))
        );

        $users = array(
            new UserModel((object) array('user_id' => 1, 'fullname' => 'Rocco Zanni')),
            new UserModel((object) array('user_id' => 2, 'fullname' => 'Marco Pracucci'))
        );

        $episodes = $this->_relationBuilder->combine($episodes, $users, 'episode.author');

        $this->assertEquals($episodes[0]->getAuthor(), $users[0], 'Relation ok');
        $this->assertEquals($episodes[1]->getAuthor(), $users[0], 'Relation ok');
    }

    public function testCombineWithOneToManyRelationAndSingleModel()
    {
        $user     = new UserModel((object) array('user_id' => 1, 'fullname' => 'Rocco Zanni'));
        $episodes = array(
            new EpisodeModel((object) array('episode_id' => 1, 'user_id' => 1)),
            new EpisodeModel((object) array('episode_id' => 2, 'user_id' => 1))
        );

        $users = $this->_relationBuilder->combine($user, $episodes, 'user.episodes');

        $this->assertEquals($user->getEpisodes(), $episodes,  'Relation ok');
    }

    public function testCombineWithOneToManyRelation()
    {
        $users = array(
            new UserModel((object) array('user_id' => 1, 'fullname' => 'Rocco Zanni')),
            new UserModel((object) array('user_id' => 2, 'fullname' => 'Marco Pracucci'))
        );

        $episodes = array(
            new EpisodeModel((object) array('episode_id' => 1, 'user_id' => 1)),
            new EpisodeModel((object) array('episode_id' => 2, 'user_id' => 1))
        );

        $users = $this->_relationBuilder->combine($users, $episodes, 'user.episodes');

        $this->assertEquals($users[0]->getEpisodes(), $episodes,  'Relation ok');
        $this->assertEquals($users[1]->getEpisodes(), array(), 'Relation ok');
    }

    public function testEnsureRelation()
    {
        $stats = (object) array('user' => 0, 'episode' => 0); // id generator

        $relations = array(
            'episode.author' => array(
                'local_key'     => 'user_id',
                'remote_key'    => 'user_id',
                'local_setter'  => 'setAuthor',
                'local_getter'  => 'getAuthor',
                'type'          => 'ONE',
                'fetcher'       => function ($user_ids) use ($stats) {
                    $users = array();
                    foreach ($user_ids as $user_id) {
                        $users[] = new UserModel((object) array('user_id' => $user_id, 'fullname' => "User #$user_id"));
                        $stats->user += 1;
                    }
                    return $users;
                }
            ),
            'user.episodes'   => array(
                'local_key'     => 'user_id',
                'remote_key'    => 'user_id',
                'local_setter'  => 'setEpisodes',
                'local_getter'  => 'getEpisodes',
                'type'          => 'MANY',
                'fetcher'       => function ($user_ids) use ($stats) {
                    $episodes = array();
                    for ($i = 0; $i < 2; $i++) {
                        $episodes[] = new EpisodeModel((object) array('episode_id' => $i, 'user_id' => $user_ids[0]));
                        $stats->episode += 1;
                    }
                    return $episodes;
                }
            ),
        );

        $builder = new RelationBuilder($relations);

        $episodes = array(
            new EpisodeModel((object) array('episode_id' => 1, 'user_id' => 1)),
            new EpisodeModel((object) array('episode_id' => 2, 'user_id' => 3))
        );

        $episodes = $builder->ensureRelations($episodes, array('episode.author'));
        $this->assertEquals($episodes[0]->data->user_id, $episodes[0]->getAuthor()->data->user_id);
        $this->assertEquals($stats->user, 2);

        $episodes = $builder->ensureRelations($episodes, array('episode.author'));
        $this->assertEquals($episodes[0]->data->user_id, $episodes[0]->getAuthor()->data->user_id);
        $this->assertEquals($stats->user, 2);

        $episodes = $builder->mapRelations($episodes, array('episode.author'));
        $this->assertEquals($episodes[0]->data->user_id, $episodes[0]->getAuthor()->data->user_id);
        $this->assertEquals($stats->user, 4);


        $user = new UserModel((object) array('user_id' => 1, 'fullname' => 'User #1'));

        $user = $builder->ensureRelations($user, array('user.episodes'));
        $this->assertEquals(count($user->getEpisodes()), 2);
        $this->assertEquals($stats->episode, 2);

        $user = $builder->ensureRelations($user, array('user.episodes'));
        $this->assertEquals(count($user->getEpisodes()), 2);
        $this->assertEquals($stats->episode, 2);

        $user = $builder->mapRelations($user, array('user.episodes'));
        $this->assertEquals(count($user->getEpisodes()), 2);
        $this->assertEquals($stats->episode, 4);
    }

    public function testMapRelatonWithEmptyFetchers()
    {
        $relations = array(
            'episode.author' => array(
                'local_key'     => 'user_id',
                'remote_key'    => 'user_id',
                'local_setter'  => 'setAuthor',
                'local_getter'  => 'getAuthor',
                'type'          => 'ONE',
                'fetcher'       => function ($user_ids) {
                    return array();
                }
            ),
            'user.episodes'   => array(
                'local_key'     => 'user_id',
                'remote_key'    => 'user_id',
                'local_setter'  => 'setEpisodes',
                'local_getter'  => 'getEpisodes',
                'type'          => 'MANY',
                'fetcher'       => function ($user_ids) {
                    return array();
                }
            ),
        );

        $builder = new RelationBuilder($relations);

        //
        // Source: single model
        //

        $user = new UserModel((object) array('user_id' => 1, 'fullname' => 'User #1'));
        $user = $builder->mapRelations($user, array('user.episodes'));
        $this->assertEquals($user->getEpisodes(), array());

        //
        // Source: array
        //

        $episodes = array(
            new EpisodeModel((object) array('episode_id' => 1, 'user_id' => 1)),
            new EpisodeModel((object) array('episode_id' => 2, 'user_id' => 3))
        );

        $episodes = $builder->mapRelations($episodes, array('episode.author'));
        $this->assertNull($episodes[0]->getAuthor());
        $this->assertNull($episodes[1]->getAuthor());
    }

    public function testMapRelationWithSimpleRelations()
    {
        $stats = (object) array('user' => 0, 'episode' => 0); // id generator

        $relations = array(
            'episode.author' => array(
                'local_key'     => 'user_id',
                'remote_key'    => 'user_id',
                'local_setter'  => 'setAuthor',
                'local_getter'  => 'getAuthor',
                'type'          => 'ONE',
                'fetcher'       => function ($user_ids) use ($stats) {
                    $users = array();
                    foreach ($user_ids as $user_id) {
                        $stats->user += 1;
                        $users[] = new UserModel((object) array('user_id' => $user_id, 'fullname' => "User #$user_id"));
                    }
                    return $users;
                }
            ),
            'user.episodes'   => array(
                'local_key'     => 'user_id',
                'remote_key'    => 'user_id',
                'local_setter'  => 'setEpisodes',
                'local_getter'  => 'getEpisodes',
                'type'          => 'MANY',
                'fetcher'       => function ($user_ids) use ($stats) {
                    $episodes = array();
                    for ($i = 1; $i <= 2; $i++) {
                        $stats->episode += 1;
                        $episodes[] = new EpisodeModel((object) array('episode_id' => 1000 + $i, 'user_id' => $user_ids[0]));
                    }
                    return $episodes;
                }
            ),
        );

        $builder = new RelationBuilder($relations);

        //
        // Source: single model
        //

        $user     = new UserModel((object) array('user_id' => 1, 'fullname' => 'User #1'));
        $user     = $builder->mapRelations($user, array('user.episodes'));
        $episodes = $user->getEpisodes();
        $this->assertEquals(count($episodes), 2);
        $this->assertEquals($stats->episode, 2);
        $this->assertEquals($episodes[0]->data->episode_id, 1001);
        $this->assertEquals($episodes[1]->data->episode_id, 1002);

        // Fetch again
        $user     = $builder->mapRelations($user, array('user.episodes'), false);
        $episodes = $user->getEpisodes();
        $this->assertEquals(count($episodes), 2);
        $this->assertEquals($stats->episode, 2);
        $this->assertEquals($episodes[0]->data->episode_id, 1001);
        $this->assertEquals($episodes[1]->data->episode_id, 1002);

        // Fetch again: force
        $user     = $builder->mapRelations($user, array('user.episodes'), true);
        $episodes = $user->getEpisodes();
        $this->assertEquals(count($episodes), 2);
        $this->assertEquals($stats->episode, 4);
        $this->assertEquals($episodes[0]->data->episode_id, 1001);
        $this->assertEquals($episodes[1]->data->episode_id, 1002);

        //
        // Source: array
        //

        $episodes = array(
            new EpisodeModel((object) array('episode_id' => 1, 'user_id' => 1)),
            new EpisodeModel((object) array('episode_id' => 2, 'user_id' => 3))
        );

        $episodes = $builder->mapRelations($episodes, array('episode.author'));
        $this->assertEquals($episodes[0]->data->user_id, $episodes[0]->getAuthor()->data->user_id);
        $this->assertEquals($episodes[1]->data->user_id, $episodes[1]->getAuthor()->data->user_id);
        $this->assertEquals($stats->user, 2);

        // Fetch again
        $episodes = $builder->mapRelations($episodes, array('episode.author'), false);
        $this->assertEquals($episodes[0]->data->user_id, $episodes[0]->getAuthor()->data->user_id);
        $this->assertEquals($episodes[1]->data->user_id, $episodes[1]->getAuthor()->data->user_id);
        $this->assertEquals($stats->user, 2);

        // Fetch again: force
        $episodes = $builder->mapRelations($episodes, array('episode.author'), true);
        $this->assertEquals($episodes[0]->data->user_id, $episodes[0]->getAuthor()->data->user_id);
        $this->assertEquals($episodes[1]->data->user_id, $episodes[1]->getAuthor()->data->user_id);
        $this->assertEquals($stats->user, 4);
    }

    public function testMapRelationWithCombinedRelations()
    {
        $stats = (object) array('episode' => 0, 'show' => 0, 'image' => 0);

        $relations = array(
            'user.shows'   => array(
                'local_key'     => 'user_id',
                'remote_key'    => 'user_id',
                'local_setter'  => 'setShows',
                'local_getter'  => 'getShows',
                'type'          => 'MANY',
                'fetcher'       => function ($user_ids) use ($stats) {
                    $stats->show++;
                    return array(
                        new ShowModel((object) array('show_id' => 1001, 'user_id' => 1)),
                        new ShowModel((object) array('show_id' => 1002, 'user_id' => 1))
                    );
                }
            ),
            'show.episodes'   => array(
                'local_key'     => 'show_id',
                'remote_key'    => 'show_id',
                'local_setter'  => 'setEpisodes',
                'local_getter'  => 'getEpisodes',
                'type'          => 'MANY',
                'fetcher'       => function ($user_ids) use ($stats) {
                    $stats->episode++;
                    return array(
                        new EpisodeModel((object) array('episode_id' => 2001, 'user_id' => 1, 'show_id' => 1001, 'image_id' => 3001)),
                        new EpisodeModel((object) array('episode_id' => 2002, 'user_id' => 1, 'show_id' => 1001, 'image_id' => 3002)),
                        new EpisodeModel((object) array('episode_id' => 2003, 'user_id' => 1, 'show_id' => 1002, 'image_id' => 3003)),
                        new EpisodeModel((object) array('episode_id' => 2004, 'user_id' => 1, 'show_id' => 1002, 'image_id' => null))
                    );
                }
            ),
            'episode.image' => array(
                'local_key'     => 'image_id',
                'remote_key'    => 'image_id',
                'local_setter'  => 'setImage',
                'local_getter'  => 'getImage',
                'type'          => 'ONE',
                'fetcher'       => function ($image_ids) use ($stats) {
                    $stats->image++;
                    return array(
                        new ImageModel((object) array('image_id' => 3001)),
                        new ImageModel((object) array('image_id' => 3002)),
                        new ImageModel((object) array('image_id' => 3003)),
                    );
                }
            ),
        );

        $builder = new RelationBuilder($relations);

        // First invoke, nothing has been mapped yet
        $user     = new UserModel((object) array('user_id' => 1, 'fullname' => 'User #1'));
        $user     = $builder->mapRelations($user, array(
            'user.shows',
            'user.shows->show.episodes',
            'user.shows->show.episodes->episode.image',
        ));

        // Check how many times the fetcher has been accessed
        $this->assertEquals($stats->episode, 1);
        $this->assertEquals($stats->show, 1);
        $this->assertEquals($stats->image, 1);

        // Check models
        $shows = $user->getShows();
        $this->assertEquals(count($shows), 2);
        $this->assertEquals($shows[0]->data->show_id, 1001);
        $this->assertEquals($shows[1]->data->show_id, 1002);

        $episodes_1 = $shows[0]->getEpisodes();
        $this->assertEquals(count($episodes_1), 2);
        $this->assertEquals($episodes_1[0]->data->episode_id, 2001);
        $this->assertEquals($episodes_1[1]->data->episode_id, 2002);

        $episodes_2 = $shows[1]->getEpisodes();
        $this->assertEquals(count($episodes_2), 2);
        $this->assertEquals($episodes_2[0]->data->episode_id, 2003);
        $this->assertEquals($episodes_2[1]->data->episode_id, 2004);

        $image_1 = $episodes_1[0]->getImage();
        $this->assertNotNull($image_1);
        $this->assertEquals($image_1->data->image_id, 3001);

        $image_2 = $episodes_1[1]->getImage();
        $this->assertNotNull($image_2);
        $this->assertEquals($image_2->data->image_id, 3002);

        $image_3 = $episodes_2[0]->getImage();
        $this->assertNotNull($image_3);
        $this->assertEquals($image_3->data->image_id, 3003);

        $image_4 = $episodes_2[1]->getImage();
        $this->assertNull($image_4);
    }

    public function testMapRelationWithCombinedRelationsSingleSourceButNoDataFromLHS()
    {
        $relations = array(
            'user.episodes'   => array(
                'local_key'     => 'user_id',
                'remote_key'    => 'user_id',
                'local_setter'  => 'setEpisodes',
                'local_getter'  => 'getEpisodes',
                'type'          => 'MANY',
                'fetcher'       => function ($user_ids) {
                    return array();
                }
            ),
            'episode.show' => array(
                'local_key'     => 'show_id',
                'remote_key'    => 'show_id',
                'local_setter'  => 'setShow',
                'local_getter'  => 'getShow',
                'type'          => 'ONE',
                'fetcher'       => function ($show_ids) {
                    return array(
                        new ShowModel((object) array('show_id' => 2001, 'title' => "Show #2001", 'image_id' => 3001)),
                        new ShowModel((object) array('show_id' => 2002, 'title' => "Show #2002", 'image_id' => 3002))
                    );
                }
            )
        );

        $builder = new RelationBuilder($relations);

        $user     = new UserModel((object) array('user_id' => 1, 'fullname' => 'User #1'));
        $user     = $builder->mapRelations($user, array(
            'user.episodes->episode.show',
        ));

        // Check models
        $episodes = $user->getEpisodes();
        $this->assertEquals(count($episodes), 0);
    }

    public function testMapRelationWithCombinedRelationsArraySourceButNoDataFromLHS()
    {
        $relations = array(
            'episode.show' => array(
                'local_key'     => 'show_id',
                'remote_key'    => 'show_id',
                'local_setter'  => 'setShow',
                'local_getter'  => 'getShow',
                'type'          => 'ONE',
                'fetcher'       => function ($show_ids) {
                    return array();
                }
            ),
            'show.image' => array(
                'local_key'     => 'image_id',
                'remote_key'    => 'image_id',
                'local_setter'  => 'setImage',
                'local_getter'  => 'getImage',
                'type'          => 'ONE',
                'fetcher'       => function ($image_ids) {
                    return array(
                        new ImageModel((object) array('image_id' => 3001)),
                        new ImageModel((object) array('image_id' => 3002))
                    );
                }
            ),
        );
        $builder = new RelationBuilder($relations);

        $episodes = array(
            new EpisodeModel((object) array('episode_id' => 1, 'show_id' => 1001)),
            new EpisodeModel((object) array('episode_id' => 2, 'show_id' => 1002))
        );

        $episodes = $builder->mapRelations($episodes, array('episode.show->show.image'));

        // Check models
        $this->assertNull($episodes[0]->getShow());
        $this->assertNull($episodes[1]->getShow());
    }
}
