<?php

use Spreaker\Dal\Model\Model as Model;

class ImageModel extends Model
{

    public function getDefaults()
    {
        return array(
            'image_type' => 'png',
            'image_size' => 'medium'
        );
    }
}