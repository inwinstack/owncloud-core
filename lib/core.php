<?php

require_once "base.php";

use OC\DatabaseSessionHandler;

class Core extends OC {
    private static $PREFILTERS = array();

    public static function init() {
        parent::init();
        $preFilters = self::$server->getSystemConfig()->getValue("preFilters");
        self::registers($preFilters);
    }

    public static function handleRequest() {
        foreach(self::$PREFILTERS as $preFilter) {
            $preFilter->run();
        }

        parent::handleRequest();
    }

    public static function registers($preFilters) {
        foreach($preFilters as $preFilter) {
            $preFilter = new $preFilter();
            self::$PREFILTERS[] = $preFilter;
        }
    }
}

Core::init();
