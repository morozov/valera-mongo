<?php

namespace Valera\Storage\DocumentStorage;

use Valera\Entity\Document;
use Valera\Resource;
use Valera\Serializer\DocumentSerializer;
use Valera\Storage\BlobStorage;
use Valera\Storage\DocumentStorage;

class Mongo implements DocumentStorage
{
    /**
     * @var \MongoDB
     */
    protected $db;

    /**
     * @var DocumentSerializer
     */
    protected $serializer;

    /**
     * Constructor
     *
     * @param \MongoDB           $db
     * @param DocumentSerializer $serializer
     */
    public function __construct(\MongoDB $db, DocumentSerializer $serializer)
    {
        $this->db = $db;
        $this->serializer = $serializer;
        $this->db->documents->ensureIndex(array(
            'resources' => 1,
        ));
    }

    /**
     * {@inheritDoc}
     */
    public function create(Document $document)
    {
        try {
            $this->db->documents->insert(
                $this->formatDocument($document)
            );
        } catch (\MongoDuplicateKeyException $e) {
            throw new \DomainException('Document already exists', 0, $e);
        }
    }

    public function retrieve($id)
    {
        $document = $this->db->documents->findOne(array(
            '_id' => $id
        ));

        if ($document) {
            return $this->unserialize(array_merge($document, array(
                'id' => $id,
            )));
        }

        return null;
    }

    /**
     * {@inheritDoc}
     */
    public function findByResource(Resource $resource)
    {
        $cursor = $this->db->documents->find(array(
            'resources' => $resource->getHash(),
        ));

        return $this->getCursorIterator($cursor);
    }

    /**
     * {@inheritDoc}
     */
    public function update(Document $document)
    {
        $this->db->documents->update(array(
            '_id' => $document->getId(),
        ), $this->formatDocument($document));
    }

    /**
     * {@inheritDoc}
     */
    public function delete($id)
    {
        $this->db->documents->findAndModify(
            array(
                '_id' => $id,
            ),
            array(),
            null,
            array(
                'remove' => true,
            )
        );
    }

    /**
     * {@inheritDoc}
     */
    public function clean()
    {
        $this->db->documents->drop();
    }

    /**
     * {@inheritDoc}
     */
    public function count()
    {
        return $this->db->documents->count();
    }

    /**
     * {@inheritDoc}
     */
    public function getIterator()
    {
        $cursor = $this->db->documents->find();
        return $this->getCursorIterator($cursor);
    }

    /**
     * @param \MongoCursor $cursor
     *
     * @return Mongo\Iterator
     */
    protected function getCursorIterator($cursor)
    {
        return new Mongo\Iterator($cursor, $this->serializer);
    }

    /**
     * Serializes document before storing
     *
     * @param \Valera\Entity\Document $document
     *
     * @return array
     */
    protected function serialize(Document $document)
    {
        return $this->serializer->serialize($document);
    }

    /**
     * Unserializes document after retrieving
     *
     * @param array $params
     *
     * @return \Valera\Entity\Document
     */
    protected function unserialize(array $params)
    {
        return $this->serializer->unserialize($params);
    }

    /**
     * Formats document data for internal representation
     *
     * @param \Valera\Entity\Document $document
     *
     * @return array
     */
    protected function formatDocument(Document $document)
    {
        $serialized = $this->serialize($document);
        unset($serialized['id']);
        $formatted = array_merge(array(
            '_id' => $document->getId(),
        ), $serialized);

        $resources = $document->getResources();
        if ($resources) {
            $formatted['resources'] = array_map(function (Resource $resource) {
                return $resource->getHash();
            }, $resources);
        }

        return $formatted;
    }
}
