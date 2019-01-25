<?php
/**
 * Spreaker Dal    A minimal Base Model class provided for subclass-ing purpose
 *
 * @link
 * @copyright
 * @license
 */

namespace Spreaker\Dal\Model;

class Model
{
    /**
     * @var stdClass
     */
    public $data = null;

    /**
     * Constructor
     *
     * @param null|object|array   $data
     */
    public function __construct($data = null, $merge_defaults = true)
    {
        if (is_array($data)) {
            $this->data = $merge_defaults ? (object) array_merge($this->getDefaults(), $data) : (object) $data;
        } else if (is_object($data)) {
            $this->data = $merge_defaults ? (object) array_merge($this->getDefaults(), (array) $data) : $data;
        } else {
            $this->data = $merge_defaults ? (object) $this->getDefaults() : (object) array();
        }
    }

    /**
     * Get defaults values of data fields
     * @return array
     **/
    public function getDefaults()
    {
        return array();
    }

    /**
     * Update model importing data and returns the keys of changed data.
     *
     * @param  array $data
     * @param  array $filter_keys
     * @return array
     */
    public function fromArray($data, $filter_keys = null)
    {
        // Filter keys
        if (is_array($filter_keys)) {
            $data = array_intersect_key($data, array_flip($filter_keys));
        }

        if ($this->data !== null) {
            $changes    = array_keys(self::_arrayDiffAssoc($data, (array) $this->data));
            $this->data = (object) array_merge((array) $this->data, $data);
        } else {
            $this->data = (object) $data;
            $changes    = array_keys($data);
        }

        return $changes;
    }

    /**
     * @return array
     */
    public function toArray()
    {
        return $this->data !== null ? (array) $this->data : array();
    }

    /**
     * @param array $array1
     * @param array $array2
     * @return array
     */
    private static function _arrayDiffAssoc($array1, $array2) {
        $difference=array();
        foreach($array1 as $key => $value) {
            if (!array_key_exists($key,$array2)) {
                $difference[$key] = $value;
            } else if ((is_bool($value) || is_bool($array2[$key]) || is_null($value) || is_null($array2[$key])) && $array2[$key] !== $value) {
                $difference[$key] = $value;
            } else if ($array2[$key] != $value){
                $difference[$key] = $value;
            }
        }
        return $difference;
    }
}
