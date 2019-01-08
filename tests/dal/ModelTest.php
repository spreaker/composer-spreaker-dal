<?php

class ModelTest extends PHPUnit_Framework_TestCase
{
    public static function setUpBeforeClass()
    {
        require_once __DIR__ . '/../../src/Spreaker/Autoloader.php';
        Spreaker\Autoloader::register();
    }

    protected function setUp()
    {
        $this->loadAllModelClasses();
    }

    private function loadModelClass($class_name)
    {
        require_once __DIR__ . "/fixtures/$class_name.php";
    }

    private function loadAllModelClasses()
    {
        $classes   = array('ImageModel');
        foreach ($classes as $class) {
            $this->loadModelClass($class);
        }
    }

    public function testModelInitialization()
    {
        $image = new ImageModel();
        $this->assertEquals($image->data->image_type, 'png');
        $this->assertEquals($image->data->image_size, 'medium');

        $image = new ImageModel(array('image_type'=>'jpg'));
        $this->assertEquals($image->data->image_type, 'jpg');
        $this->assertEquals($image->data->image_size, 'medium');

        $image = new ImageModel(array('image_type'=>'raw', 'image_size'=>'huge'));
        $this->assertEquals($image->data->image_type, 'raw');
        $this->assertEquals($image->data->image_size, 'huge');

        $image = new ImageModel(array('image_type'=>'raw', 'image_size'=>'huge', 'image_title'=>'no name'));
        $this->assertEquals($image->data->image_type, 'raw');
        $this->assertEquals($image->data->image_size, 'huge');
        $this->assertEquals($image->data->image_title, 'no name');

        $image = new ImageModel(array('image_title'=>'no name'));
        $this->assertEquals($image->data->image_type, 'png');
        $this->assertEquals($image->data->image_size, 'medium');
        $this->assertEquals($image->data->image_title, 'no name');
    }
}
