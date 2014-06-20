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
        if ($this->isInQueue($item)) {
            return;
        }

        $this->addToCollection($item, 'pending', array(
            'seq' => $this->getNextSequence('pending'),
        ));
    }

    protected function isInQueue(Queueable $item)
    {
        try {
            /** @var \MongoCollection $pending */
            $this->db->{$this->name . '_index'}->insert(array(
                '_id' => $item->getHash(),
            ));
        } catch (MongoCursorException $e) {
            if ($e->getCode() === 11000) {
                return true;
            } else {
                throw $e;
            }
        }

        return false;
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
        $this->dropCollection('index');
        $this->dropCollection('counters');
        $this->dropCollection('pending');
        $this->dropCollection('in_progress');
        $this->dropCollection('completed');
        $this->dropCollection('failed');
        $this->setUp();
    }

    protected function dropCollection($name)
    {
        $this->db->{$this->name . '_' . $name}->drop();
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

    /** {@inheritDoc} */
    public function reEnqueueFailed()
    {
        foreach ($this->getFailed() as $item) {
            $this->addToCollection($item, 'pending');
        }

        $this->dropCollection('failed');
    }

    /** {@inheritDoc} */
    public function reEnqueueAll()
    {
        $this->reEnqueueFailed();

        foreach ($this->getCompleted() as $item) {
            $this->addToCollection($item, 'pending');
        }

        $this->dropCollection('completed');
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
