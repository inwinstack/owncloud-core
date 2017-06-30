<head>
<title><?php echo add_title(); ?></title>
</head>
<?php
/**
 * @author Duncan Chiang <duncan.c@inwinstack.com>
 *
 * @copyright Copyright (c) 2017, inwinStack, Inc.
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

OC_Util::checkLoggedIn();
function add_title(){
    $l = \OC::$server->getL10N('settings');
    $theme = new OC_Theme();
    $title = $l->t('System Information') . ' - ' . $theme->getTitle();
    return $title;
}
// Load the files we need
OC_Util::addStyle( "settings", "settings" );

$tmpl = new OC_Template( "settings", "system", "user" );
$tmpl->printPage();

