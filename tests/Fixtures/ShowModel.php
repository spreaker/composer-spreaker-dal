<?php

namespace Spreaker\Dal\Tests\Fixtures;

use Spreaker\Dal\Model\Model as Model;

class ShowModel extends Model
{
    private $_author;
    private $_image;
    private $_episodes;

    public function __construct($data = null)
    {
        parent::__construct($data);

        $this->_author = false;
        $this->_image = false;
        $this->_episodes = false;
    }

    public function setAuthor($user)
    {
        $this->_author = $user;
    }

    public function getAuthor()
    {
        return $this->_author;
    }

    public function setImage($image)
    {
        $this->_image = $image;
    }

    public function getImage()
    {
        return $this->_image;
    }

    public function setEpisodes($episodes)
    {
        $this->_episodes = $episodes;
    }

    public function getEpisodes()
    {
        return $this->_episodes;
    }
}