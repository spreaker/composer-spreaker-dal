<?php

use Spreaker\Dal\Model\Model as Model;

class EpisodeModel extends Model
{
    private $_author;
    private $_show;
    private $_image;

    public function __construct($data = null)
    {
        parent::__construct($data);

        $this->_author = false;
        $this->_show   = false;
        $this->_image  = false;
    }

    public function setAuthor($user)
    {
        $this->_author = $user;
    }

    public function getAuthor()
    {
        return $this->_author;
    }

    public function setShow($show)
    {
        $this->_show = $show;
    }

    public function getShow()
    {
        return $this->_show;
    }

    public function setImage($image)
    {
        $this->_image = $image;
    }

    public function getImage()
    {
        return $this->_image;
    }

}