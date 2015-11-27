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

use Sabre\DAV\Exception\Forbidden;
use Sabre\DAV\Exception\NotFound;
use Sabre\DAV\Exception\BadRequest;
use Sabre\DAV\Exception\Conflict;
use Sabre\DAV\ICollection;

use \OCP\SystemTag\ISystemTagManager;
use \OCP\SystemTag\ISystemTag;
use \OCP\SystemTag\TagNotFoundException;
use \OCP\SystemTag\TagAlreadyExistsException;
use \OC\SystemTag\SystemTag;

class SystemTagsByNameCollection implements ICollection {

	/**
	 * @var array
	 */
	private $principalInfo;

	/**
	 * @var ISystemTagManager
	 */
	private $tagManager;

	/**
	 * SystemTagsByIdCollection constructor.
	 *
	 * @param array $principalInfo
	 * @param ISystemTagManager $tagManager
	 */
	public function __construct($principalInfo, $tagManager) {
		$this->principalInfo = $principalInfo;
		$this->tagManager = $tagManager;
	}

	function createFile($name, $data = null) {
		try {
			$parts = $this->parseName($name);
			if (!$parts) {
				// invalid compound name
				throw new BadRequest('Invalid compound tag name');
			}

			$this->tagManager->createTag($parts[0], $parts[1], $parts[2]);
		} catch (TagAlreadyExistsException $e) {
			throw new Conflict('Tag with compound name ' . $name . ' already exists', 0, $e);
		}
	}
	

	function createDirectory($name) {
		throw new Forbidden('Permission denied to create collections');
	}

	function getChild($name) {
		try {
			$parts = $this->parseName($name);
			if (!$parts) {
				// invalid compound name
				throw new NotFound('Invalid compound tag name');
			}

			$tag = $this->tagManager->getTag($parts[0], $parts[1], $parts[2]);
			return $this->makeNode($tag);
		} catch (TagNotFoundException $e) {
			throw new NotFound('Tag with compound name ' . $name . ' not found', 0, $e);
		}
	}

	function getChildren() {
		// TODO: set visibility filter based on principal/permissions ?
		$tags = $this->tagManager->getAllTags(true);
		return array_map(function($tag) {
			return $this->makeNode($tag);
		}, $tags);
	}

	function childExists($name) {
		try {
			$parts = $this->parseName($name);
			if (!$parts) {
				// invalid compound name
				throw new NotFound('Invalid compound tag name');
			}

			$this->tagManager->getTag($parts[0], $parts[1], $parts[2]);
			return true;
		} catch (TagNotFoundException $e) {
			return false;
		}
	}

	function delete() {
		throw new Forbidden('Permission denied to delete this collection');
	}

	function getName() {
		return 'by-name';
	}

	function setName($name) {
		throw new Forbidden('Permission denied to rename this collection');
	}

	/**
	 * Returns the last modification time, as a unix timestamp
	 *
	 * @return int
	 */
	function getLastModified() {
		return null;
	}

	/**
	 * Parses compound tag name
	 *
	 * @param string $name compound name
	 *
	 * @return array of components or null if invalid name
	 */
	private function parseName($name) {
		$parts = explode('_', $name);
		if (count($parts) !== 3) {
			// invalid compound name
			return null;
		}

		return [
			$parts[0],
			$parts[1] === '1',
			$parts[2] === '1',
		];
	}

	/**
	 * Create a sabre node for the given system tag
	 *
	 * @param ISystemTag $tag
	 *
	 * @return SystemTagNode
	 */
	private function makeNode(ISystemTag $tag) {
		return new SystemTagNodeByName($tag, $this->tagManager);
	}
}
