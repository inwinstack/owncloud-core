<?php
/**
 * @author Vincent Petry <pvince81@owncloud.com>
 *
 * @copyright Copyright (c) 2015, ownCloud, Inc.
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
namespace OCA\DAV\SystemTag;

use \Sabre\DAV\PropFind;
use \Sabre\DAV\PropPatch;

use \OCA\DAV\SystemTag\SystemTagNode;
use \OCP\SystemTag\ISystemTag;
use \OCP\SystemTag\ISystemTagManager;

class SystemTagPlugin extends \Sabre\DAV\ServerPlugin {

	// namespace
	const NS_OWNCLOUD = 'http://owncloud.org/ns';
	const ID_PROPERTYNAME = '{http://owncloud.org/ns}id';
	const DISPLAYNAME_PROPERTYNAME = '{http://owncloud.org/ns}display-name';
	const USERVISIBLE_PROPERTYNAME = '{http://owncloud.org/ns}user-visible';
	const USERASSIGNABLE_PROPERTYNAME = '{http://owncloud.org/ns}user-assignable';

	/**
	 * This initializes the plugin.
	 *
	 * This function is called by \Sabre\DAV\Server, after
	 * addPlugin is called.
	 *
	 * This method should set up the required event subscriptions.
	 *
	 * @param \Sabre\DAV\Server $server
	 * @return void
	 */
	public function initialize(\Sabre\DAV\Server $server) {

		$server->xmlNamespaces[self::NS_OWNCLOUD] = 'oc';

		$server->protectedProperties[] = self::ID_PROPERTYNAME;
		$server->protectedProperties[] = self::DISPLAYNAME_PROPERTYNAME;
		$server->protectedProperties[] = self::USERVISIBLE_PROPERTYNAME;
		$server->protectedProperties[] = self::USERASSIGNABLE_PROPERTYNAME;

		$server->on('propFind', array($this, 'handleGetProperties'));
	}

	/**
	 * Retrieves system tag properties
	 *
	 * @param PropFind $propFind
	 * @param \Sabre\DAV\INode $node
	 */
	public function handleGetProperties(
		PropFind $propFind,
		\Sabre\DAV\INode $node
	) {
		if (!($node instanceof SystemTagNode)) {
			return;
		}

		$propFind->handle(self::ID_PROPERTYNAME, function() use ($node) {
			return $node->getSystemTag()->getId();
		});

		$propFind->handle(self::DISPLAYNAME_PROPERTYNAME, function() use ($node) {
			return $node->getSystemTag()->getName();
		});

		$propFind->handle(self::USERVISIBLE_PROPERTYNAME, function() use ($node) {
			return (int)$node->getSystemTag()->isUserVisible();
		});

		$propFind->handle(self::USERASSIGNABLE_PROPERTYNAME, function() use ($node) {
			return (int)$node->getSystemTag()->isUserAssignable();
		});
	}
}
