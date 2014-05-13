<?php

namespace Valera\Tests\Mongo;

class Helper
{
    public static function getMongo()
    {
        if (!defined('VALERA_TESTS_MONGO_DB')) {
            throw new \PHPUnit_Framework_SkippedTestSuiteError(
                'MongoDB database configuration not found'
            );
        }

        if (defined('VALERA_TESTS_MONGO_SERVER')) {
            $client = new \MongoClient(VALERA_TESTS_MONGO_SERVER);
        } else {
            $client = new \MongoClient();
        }

        $db = $client->selectDB(VALERA_TESTS_MONGO_DB);

        return $db;
    }
}
