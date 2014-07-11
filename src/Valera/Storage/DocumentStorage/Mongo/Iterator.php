<?php

namespace Valera\Storage\DocumentStorage\Mongo;

use Valera\Serializer\DocumentSerializer;

class Iterator extends \IteratorIterator
{
    /**
     * @var DocumentSerializer
     */
    protected $serializer;

    public function __construct(\MongoCursor $cursor, DocumentSerializer $serializer)
    {
        parent::__construct($cursor);

        $this->serializer = $serializer;
    }

    public function key()
    {
        $current = parent::current();
        $key = $current['_id'];

        return $key;
    }

    public function current()
    {
        $current = parent::current();
        $current = $this->serializer->unserialize(array_merge($current, array(
            'id' => $this->key(),
        )));

        return $current;
    }
}
