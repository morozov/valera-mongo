<?php

namespace Valera\Queue;

use MongoCursorException;
use Valera\Queueable;
use Valera\Queue;
use Valera\Queue\Exception\LogicException;
use Valera\Serializer\SerializerInterface;

/**
 * MongoDB implementation of queue
 *
 * @package Valera\Queue
 */
class Mongo implements Queue
{
    /**
     * @var string
     */
    protected $name;

    /**
     * @var \MongoDB
     */
    protected $db;

    /**
     * @var SerializerInterface
     */
    protected $serializer;

    /**
     * Constructor
     *
     * @param string              $name
     * @param \MongoDB            $db
     * @param SerializerInterface $serializer
     *
     * @throws \InvalidArgumentException
     */
    public function __construct($name, \MongoDB $db, SerializerInterface $serializer)
    {
        if (!is_string($name) || $name === '' || !ctype_alnum($name)) {
            throw new \InvalidArgumentException(
                'Queue name must be a non-empty alpha-numeric string'
            );
        }

        $this->name = $name;
        $this->db = $db;
        $this->serializer = $serializer;
        $this->setUp();
    }

    protected function setUp()
    {
        try {
            $this->db->{$this->name . '_counters'}->insert(array(
                '_id' => 'pending',
                'seq' => 0,
            ));
        } catch (MongoCursorException $e) {
            if ($e->getCode() !== 11000) {
                throw $e;
            }
        }

        $this->db->{$this->name . '_pending'}->ensureIndex(array(
            'sec' => 1,
        ));
    }

    /** @inheritDoc */
    public function enqueue(Queueable $item)
    {
        try {
            /** @var \MongoCollection $pending */
            $this->db->{$this->name . '_pending'}->insert(array(
                '_id' => $item->getHash(),
                'seq' => $this->getNextSequence('pending'),
                'data' => $this->serializer->serialize($item),
            ));
        } catch (MongoCursorException $e) {
            if ($e->getCode() !== 11000) {
                throw $e;
            }
        }
    }

    protected function getNextSequence($name)
    {
        $ret = $this->db->{$this->name . '_counters'}->findAndModify(
            array('_id' => $name),
            array('$inc' => array('seq' => 1)),
            null,
            array('new' => true)
        );

        return $ret['seq'];
    }

    /** @inheritDoc */
    public function dequeue()
    {
        $ret = $this->db->{$this->name . '_pending'}->findAndModify(
            array(),
            array(),
            null,
            array(
                'sort' => array('seq' => 1),
                'remove' => true,
            )
        );

        if (!$ret) {
            return null;
        }

        $item = $this->serializer->unserialize($ret['data']);

        /** @var \MongoCollection $pending */
        $this->addToCollection($item, 'in_progress');

        return $item;
    }

    /** @inheritDoc */
    public function resolveCompleted(Queueable $item)
    {
        $this->ensureAndRemove($item, 'in_progress');
        $this->addToCollection($item, 'completed');
    }

    /** @inheritDoc */
    public function resolveFailed(Queueable $item, $reason)
    {
        $this->ensureAndRemove($item, 'in_progress');
        $this->addToCollection($item, 'failed', array(
            'reason' => $reason,
        ));
    }

    /** {@inheritDoc} */
    public function clean()
    {
        $this->db->{$this->name . '_counters'}->drop();
        $this->db->{$this->name . '_pending'}->drop();
        $this->db->{$this->name . '_in_progress'}->drop();
        $this->db->{$this->name . '_completed'}->drop();
        $this->db->{$this->name . '_failed'}->drop();
        $this->setUp();
    }

    /** @inheritDoc */
    public function getInProgress()
    {
        return $this->getCollection('in_progress');
    }

    /** @inheritDoc */
    public function getCompleted()
    {
        return $this->getCollection('completed');
    }

    /** @inheritDoc */
    public function getFailed()
    {
        return $this->getCollection('failed');
    }

    /** @inheritDoc */
    public function count()
    {
        return $this->db->{$this->name . '_pending'}->count();
    }

    protected function getCollection($name)
    {
        $items = array();
        $collection = $this->db->{$this->name . '_' . $name}->find();
        foreach ($collection as $document) {
            $items[] = $this->serializer->unserialize($document['data']);
        }

        return $items;
    }

    /**
     * Adds item to specified collection
     *
     * @param Queueable $item
     * @param string $name
     * @param array $metadata
     */
    protected function addToCollection(
        Queueable $item,
        $name,
        array $metadata = array()
    ) {
        $document = array_merge(array(
            '_id' => $item->getHash(),
            'data' => $this->serializer->serialize($item),
        ), $metadata);
        $this->db->{$this->name . '_' . $name}->insert($document);
    }

    protected function ensureAndRemove(Queueable $item, $name)
    {
        $res = $this->db->{$this->name . '_' . $name}->findAndModify(
            array('_id' => $item->getHash()),
            array(),
            null,
            array(
                'remove' => true,
            )
        );

        if (!$res) {
            throw new LogicException('Item is not in progress');
        }
    }
}
