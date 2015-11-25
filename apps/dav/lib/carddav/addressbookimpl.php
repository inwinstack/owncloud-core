<?php
/**
 * @author Thomas Müller <thomas.mueller@tmit.eu>
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

namespace OCA\DAV\CardDAV;

use OCP\IAddressBook;
use Sabre\VObject\Component\VCard;
use Sabre\VObject\Property\Text;
use Sabre\VObject\Reader;
use Sabre\VObject\UUIDUtil;

class AddressBookImpl implements IAddressBook {

	/** @var CardDavBackend */
	private $backend;

	/** @var array */
	private $addressBookInfo;

	/** @var DbHandler */
	private $dbHandler;

	/**
	 * AddressBookImpl constructor.
	 *
	 * @param array $addressBookInfo
	 * @param CardDavBackend $backend
	 * @param DbHandler $dbHandler
	 */
	public function __construct(array $addressBookInfo, CardDavBackend $backend, DbHandler $dbHandler) {
		$this->addressBookInfo = $addressBookInfo;
		$this->backend = $backend;
		$this->dbHandler = $dbHandler;

		/*
		 * 				'id'  => $row['id'],
				'uri' => $row['uri'],
				'principaluri' => $row['principaluri'],
				'{DAV:}displayname' => $row['displayname'],
				'{' . Plugin::NS_CARDDAV . '}addressbook-description' => $row['description'],
				'{http://calendarserver.org/ns/}getctag' => $row['synctoken'],
				'{http://sabredav.org/ns}sync-token' => $row['synctoken']?$row['synctoken']:'0',
*/
	}

	/**
	 * @return string defining the technical unique key
	 * @since 5.0.0
	 */
	public function getKey() {
		return $this->addressBookInfo['id'];
	}

	/**
	 * In comparison to getKey() this function returns a human readable (maybe translated) name
	 *
	 * @return mixed
	 * @since 5.0.0
	 */
	public function getDisplayName() {
		return $this->addressBookInfo['{DAV:}displayname'];
	}

	/**
	 * @param string $pattern which should match within the $searchProperties
	 * @param array $searchProperties defines the properties within the query pattern should match
	 * @param array $options - for future use. One should always have options!
	 * @return array an array of contacts which are arrays of key-value-pairs
	 * @since 5.0.0
	 */
	public function search($pattern, $searchProperties, $options) {
		// TODO: Implement search() method.
		$cards = $this->backend->getCards($this->addressBookInfo['id']);

		//
		// TODO: search now
		//

	}

	/**
	 * @param array $properties this array if key-value-pairs defines a contact
	 * @return array an array representing the contact just created or updated
	 * @since 5.0.0
	 */
	public function createOrUpdate($properties) {
		$update = false;
		if (!isset($properties['UID'])) { // create a new contact
			$uid = $this->createUid();
			$uri = $uid . '.vcf';
			$vCard = $this->createEmptyVCard($uid);
		} else { // update existing contact
			$uid = $properties['UID'];
			$uri = $uid . '.vcf';
			$vCardData = $this->backend->getCard($this->getKey(), $uri);
			$vCard = Reader::read($vCardData['carddata']);
			$update = true;
		}

		foreach ($properties as $key => $value) {
			$vCard->$key = $vCard->createProperty($key, $value);
		}

		if ($update) {
			$this->backend->updateCard($this->getKey(), $uri, $vCard->serialize());
		} else {
			$this->backend->createCard($this->getKey(), $uri, $vCard->serialize());
		}

		$this->dbHandler->updateProperties($this->getKey(), $uid, $properties);

		// ToDo return array representing the contact

	}

	/**
	 * @return mixed
	 * @since 5.0.0
	 */
	public function getPermissions() {
		// TODO: Implement getPermissions() method.
	}

	/**
	 * @param object $id the unique identifier to a contact
	 * @return bool successful or not
	 * @since 5.0.0
	 */
	public function delete($id) {
		$uri = $this->dbHandler->getCardUri($id);
		return $this->backend->deleteCard($this->addressBookInfo['id'], $uri);
	}

	/**
	 * create UID for contact
	 *
	 * @return string
	 */
	protected function createUid() {
		do {
			$uid = UUIDUtil::getUUID();
		} while (empty($this->dbHandler->getContact($uid . '.vcf')));

		return $uid;
	}

	/**
	 * create empty vcard
	 *
	 * @param string $uid
	 * @return VCard
	 */
	protected function createEmptyVCard($uid) {
		$vCard = new VCard();
		$vCard->add(new Text($vCard, 'UID', $uid));
		return $vCard;
	}
}
