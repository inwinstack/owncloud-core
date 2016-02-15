<?php
/**
 * @author Individual IT Services <info@individual-it.net>
 * @author Robin Appelman <icewind@owncloud.com>
 * @author Duncan Chiang <duncan.c@inwinstack.com>
 *
 * @copyright Copyright (c) 2016, ownCloud, Inc.
 * @license AGPL-3.0
 *
 * This code is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License, version 3,
 * along with this program.  If not, see <http://www.gnu.org/licenses/>
 *
 */

namespace OC\Lock;

use OCP\AppFramework\Utility\ITimeFactory;
use OCP\IDBConnection;
use OCP\ILogger;
use OCP\Lock\ILockingProvider;
use OCP\Lock\LockedException;
/**
 * Locking provider that stores the locks in the database
 */
class EnhanceDBLockingProvider extends DBLockingProvider {
    public function __construct(IDBConnection $connection, ILogger $logger, ITimeFactory $timeFactory) {
        $this->connection = $connection;
        $this->logger = $logger;
        $this->timeFactory = $timeFactory;
        parent::__construct($connection, $logger, $timeFactory);
    }
    
    public function acquireLock($path, $type) {
        parent::acquireLock($path, $type);
        $hostName = gethostname();
        
        $sqlCmd = "INSERT INTO `*PREFIX*host_locks` (`hostname`,`key`)
                   SELECT ?,?
                   FROM DUAL
                   WHERE EXISTS (SELECT * FROM `*PREFIX*file_locks` WHERE `key` = ? LIMIT 1)
                   AND NOT EXISTS (SELECT * FROM `*PREFIX*host_locks` WHERE `key` = ? LIMIT 1)
                  ";
        $result = $this->connection->executeUpdate($sqlCmd,[$hostName, $path, $path,$path]);
    }
    
    public function releaseAll() {
        parent::releaseAll();
        $hostName = gethostname();
        $sqlCmd = "DELETE `*PREFIX*host_locks`
    		   FROM `*PREFIX*host_locks` INNER JOIN `*PREFIX*file_locks`
    		   ON `*PREFIX*host_locks`.`key` = `*PREFIX*file_locks`.`key`
    		   AND `*PREFIX*file_locks`.`lock` = 0
    		   AND `*PREFIX*host_locks`.`hostname` = ?
	          ";
        $result = $this->connection->executeUpdate($sqlCmd,[$hostName]);
    }
}

