<?php
/**
 * Spreaker Dal
 *
 * @link
 * @copyright
 * @license
 */

namespace Spreaker\Dal\Relation;

use Exception;

class RelationBuilder
{
    private array $_relations;

    /**
     * @param array{ string:
     *     array {
     *          local_key: string,
     *          remote_key: string,
     *          local_setter: string,
     *          local_getter: string,
     *          type: 'ONE'|'MANY',
     *          fetcher: callable
     *     }
     * } $relations
     */
    public function __construct(array $relations)
    {
        $this->_relations = $relations;
    }

    /**
     * Combine models using a relation
     *
     * @template T
     * @param  object<T>|array<T>       $local    The "local" model, or an array containing the local models
     * @param  array<object>            $remote   The "remote" models
     * @param  string|array<string>     $relation The relation configuration or the relation name
     *
     * @return object<T>|array<T>
     */
    public function combine(object|array $local, array $remote, string|array $relation): object|array
    {
        // Read relation information from configurations
        if (is_string($relation)) {
            if (!isset($this->_relations[$relation])) {
                return $local;
            }
            $relation = $this->_relations[$relation];
        }

        // Map remote records by "remote_key" values
        $remoteMap = array();
        foreach ($remote as $remoteModel)
        {
            $key = $remoteModel->data->{$relation['remote_key']};

            if (!isset($remoteMap[$key])) {
                $remoteMap[$key] = array();
            }

            $remoteMap[$key][] = $remoteModel;
        }

        // Iterate on loca records try to mapping the relations with the remote objects
        foreach ((is_object($local) ? array($local) : $local) as $localModel)
        {
            $key = $localModel->data->{$relation['local_key']};

            // No corresponding objects found, just skip this one
            if (!isset($remoteMap[$key]) || count($remoteMap[$key]) === 0) {

                // set default value correctly
                switch ($relation['type']) {
                    case 'ONE':
                        call_user_func(array($localModel, $relation['local_setter']), null);
                        break;
                    case 'MANY':
                        call_user_func(array($localModel, $relation['local_setter']), array());
                        break;
                }

                continue;
            }

            // Assign the remote object (or objects) to the relation
            switch ($relation['type']) {
                case 'ONE':
                    call_user_func(array($localModel, $relation['local_setter']), $remoteMap[$key][0]);
                    break;
                case 'MANY':
                    call_user_func(array($localModel, $relation['local_setter']), $remoteMap[$key]);
                    break;
            }
        }

        return $local;
    }

    /**
     * Ensure the specified relations on the given models (or model) are mapped
     *
     * @template T
     * @param object<T>|array<T> $models The model to map, or an array containing a list of models
     * @param array<string> $relations An array containing the relations to map
     *
     * @return object<T>|array<T>
     *
     * @throws Exception
     */
    public function ensureRelations(object|array $models, array $relations): object|array
    {
        return $this->mapRelations($models, $relations, false);
    }

    /**
     * Map the specified relations on the given models (or model)
     *
     * @template T
     * @param object<T>|array<T> $models The model to map, or an array containing a list of models
     * @param array<string> $relations An array containing the relations to map
     * @param boolean $force true: map unconditionally, false: only map the missing relations
     *
     * @return object<T>|array<T>
     *
     * @throws Exception
     */
    public function mapRelations(object|array $models, array $relations, bool $force = true): object|array
    {
        foreach ($relations as $relation) {
            $models = $this->mapRelation($models, $relation, $force);
        }

        return $models;
    }

    /**
     * Map the specified relation on the given models (or model)
     *
     * @template T
     * @param object<T>|array<T> $models The model to map, or an array containing a list of models
     * @param string $relation The name of the relation to map
     * @param boolean $force true: map the relations unconditionally, false only map the missing relations
     *
     * @return object<T>|array<T>
     *
     * @throws Exception
     */
    public function mapRelation(object|array $models, string $relation, bool $force = true): object|array
    {
        if (strpos($relation, "->")) {
            return $this->_mapRelationComposite($models, $relation, $force);
        } else {
            return $this->_mapRelationSimple($models, $relation, $force);
        }
    }

    private function _mapRelationSimple(object|array $models, string $relation, bool $force = true): object|array
    {
        // Get relation config
        $config = $this->_relations[$relation] ?? null;
        if (!$config) {
            throw new Exception("The relation $relation does not exists");
        }

        // Get ids of related models to fetch
        $ids = array();
        foreach ((is_object($models) ? array($models) : $models) as $model) {

            if ($force) {
                if (isset($model->data->{$config['local_key']}) && $model->data->{$config['local_key']} !== null) {
                    $ids[] = $model->data->{$config['local_key']};
                }
            } else {
                // only mapped missing relations
                $local_getter = $this->_relations[$relation]['local_getter'];
                if (call_user_func(array($model, $local_getter), false) === false &&
                    isset($model->data->{$config['local_key']}) && $model->data->{$config['local_key']} !== null) {
                        $ids[] = $model->data->{$config['local_key']};
                }
            }
        }
        $ids = array_unique($ids);

        // Check if there's at least 1 model to fetch
        if (empty($ids)) {
            return $models;
        }

        $related = $this->fetchRelated($config['fetcher'], $ids);

        // Map relation
        return $this->combine($models, $related, $config);
    }

    private function _mapRelationComposite(object|array $models, string $relation, bool $force = true): object|array
    {
        // We support RECURSIVE composite relations, so every time we enter this
        // function we map only the first level and let the recursion map the
        // other ones

        // Split.
        //  From: user.episodes->episode.show->show.image
        //  To:   user.episodes / episode.show->show.image
        $parts  = explode("->", $relation);
        $lhs    = array_shift($parts);
        $rhs    = implode("->", $parts);

        // Read the relation config for the LEFT HAND SIDE relation
        $lhsConfig = $this->_relations[$lhs] ?? null;
        if (!$lhsConfig) {
            throw new Exception("The relation $lhs does not exists");
        }

        // Ensure LHS relation is mapped. Overriding the "force" parameter with false we
        // avoid double fetching
        // For example if the relations array is like this:
        //  - episode.author, episode.author->user.image
        // we episode.author relations has already been fetched by a previous iteration
        $this->_mapRelationSimple($models, $lhs, false);

        // Iterate on the model to extract the child models to use as the source for
        // the recursion.
        // Example: if the relations is this one "episode.author->user.image", the previous
        // step has already mapped the "episode.author", now we extract the list of all authors
        // that needs to receive the "user.image" mapping
        $lhsGetter = $lhsConfig['local_getter'];
        $rhsModels = array();

        foreach (is_object($models) ? array($models) : $models as $model) {

            $related = call_user_func(array($model, $lhsGetter));
            if (empty($related)) {
                continue;
            }

            if (is_object($related)) {
                $rhsModels[] = $related;
            } else {
                $rhsModels = array_merge($rhsModels, $related);
            }
        }

        // No models here means there is nothing to use as "source", so we can safely terminate here
        if (empty($rhsModels)) {
            return $models;
        }

        // Map the relation to the RHS models extracted by the previous loop
        $this->mapRelation($rhsModels, $rhs, $force);

        return $models;
    }

    protected function fetchRelated($fetcher, array $ids)
    {
        return call_user_func($fetcher, $ids);
    }
}
