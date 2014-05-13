<?php

namespace Valera\Queue\Mongo;

use Valera\Content;
use Valera\Serializer\ContentSerializer as Base;
use Valera\Serializer\SerializerInterface;

/**
 * Decorates content serializer for MongoDB
 */
class ContentSerializer implements SerializerInterface
{
    /**
     * @var Base
     */
    protected $serializer;

    /**
     * Constructor
     *
     * @param Base $serializer
     */
    public function __construct(Base $serializer)
    {
        $this->serializer = $serializer;
    }

    /**
     * Creates array representation of content value object for MongoDB
     *
     * @param Content $content
     *
     * @return array
     * @throws \UnexpectedValueException
     */
    public function serialize($content)
    {
        $serialized = $this->serializer->serialize($content);
        $serialized['content'] = new \MongoBinData(
            $serialized['content'],
            \MongoBinData::GENERIC
        );

        return $serialized;
    }

    /**
     * Restores content value object from MongoDB array representation
     *
     * @param array $params
     *
     * @return Content
     * @throws \InvalidArgumentException
     * @throws \UnexpectedValueException
     */
    public function unserialize(array $params)
    {
        if (isset($params['content'])
            && $params['content'] instanceof \MongoBinData) {
            $params['content'] = $params['content']->bin;
        }

        return $this->serializer->unserialize($params);
    }
}
