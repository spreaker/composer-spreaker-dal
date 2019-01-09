<?php

use Spreaker\Dal\Model\Model as Model;

class UserModel extends Model
{
    private $_episodes;
    private $_shows;

    public function __construct($data = null)
    {
        parent::__construct($data);

        $this->_episodes = false;
        $this->_shows    = false;
    }

    public function setEpisodes($episodes)
    {
        $this->_episodes = $episodes;
    }

    public function getEpisodes()
    {
        return $this->_episodes;
    }

    public function setShows($shows)
    {
        $this->_shows = $shows;
    }

    public function getShows()
    {
        return $this->_shows;
    }
}