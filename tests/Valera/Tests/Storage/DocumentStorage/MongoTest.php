<?php

namespace Valera\Tests\Storage\DocumentStorage;

use Valera\Storage\DocumentStorage\Mongo as Storage;
use Valera\Tests\Mongo\Helper as MongoHelper;
use Valera\Tests\Serializer\Helper as SerializerHelper;

/**
 * @requires extension mongo
 */
class MongoTest extends AbstractTest
{
    public static function setUpBeforeClass()
    {
        $db = MongoHelper::getMongo();
        $documentSerializer = SerializerHelper::getDocumentSerializer();
        self::$storage = new Storage($db, $documentSerializer);

        parent::setUpBeforeClass();
    }
}
