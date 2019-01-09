<?php
/**
 * Spreaker Dal    A minimal Base Model class provided for subclass-ing purpose
 *
 * @link
 * @copyright
 * @license
 */

namespace Spreaker\Dal\Model;

use spUtilArray;

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
            $changes    = array_keys(spUtilArray::arrayDiffAssoc($data, (array) $this->data));
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
}
