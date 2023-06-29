<?php

namespace Spreaker\Dal\Tests\Unit;

use PHPUnit\Framework\TestCase;
use Spreaker\Dal\Tests\Fixtures\ImageModel;

class ModelTest extends TestCase
{
    public function testModelInitialization()
    {
        $image = new ImageModel();
        $this->assertEquals('png', $image->data->image_type);
        $this->assertEquals('medium', $image->data->image_size);

        $image = new ImageModel(array('image_type'=>'jpg'));
        $this->assertEquals('jpg', $image->data->image_type);
        $this->assertEquals('medium', $image->data->image_size);

        $image = new ImageModel(array('image_type'=>'raw', 'image_size'=>'huge'));
        $this->assertEquals('raw', $image->data->image_type);
        $this->assertEquals('huge', $image->data->image_size);

        $image = new ImageModel(array('image_type'=>'raw', 'image_size'=>'huge', 'image_title'=>'no name'));
        $this->assertEquals('raw', $image->data->image_type);
        $this->assertEquals('huge', $image->data->image_size);
        $this->assertEquals('no name', $image->data->image_title);

        $image = new ImageModel(array('image_title'=>'no name'));
        $this->assertEquals('png', $image->data->image_type);
        $this->assertEquals('medium', $image->data->image_size);
        $this->assertEquals('no name', $image->data->image_title);
    }
}
