<?php
/**
 * @author Duncan Chiang <duncan.c@inwinstack.com>
 * 
 * 
 * @copyright Copyright (c) 2016, inwinstack, Inc.
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

namespace OCA\Files_External\Lib\Backend;

use \OCP\IL10N;
use \OCA\Files_External\Lib\Backend\Backend;
use \OCA\Files_External\Lib\DefinitionParameter;
use \OCA\Files_External\Lib\Auth\AuthMechanism;
use \OCA\Files_External\Service\BackendService;
use \OCA\Files_External\Lib\LegacyDependencyCheckPolyfill;

use \OCA\Files_External\Lib\Auth\OAuth2\OAuth2;

class OneDrive extends Backend {

	use LegacyDependencyCheckPolyfill;

	public function __construct(IL10N $l, OAuth2 $legacyAuth) {
		$this
			->setIdentifier('onedrive')
			->addIdentifierAlias('\OC\Files\Storage\OneDrive') // legacy compat
			->setStorageClass('\OC\Files\Storage\OneDrive')
			->setText($l->t('One Drive'))
			->addParameters([
				// all parameters handled in OAuth2 mechanism
			    (new DefinitionParameter('rollback_url', $l->t('Rollback URL'))),
			    (new DefinitionParameter('state', $l->t('state')))
			        ->setFlag(DefinitionParameter::FLAG_OPTIONAL)
			        ->setType(DefinitionParameter::VALUE_HIDDEN),
			])
			->addAuthScheme(AuthMechanism::SCHEME_OAUTH2)
			->setLegacyAuthMechanism($legacyAuth)
		;
	}

}

