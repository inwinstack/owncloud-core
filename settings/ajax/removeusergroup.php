<?php
$userName = (string)$_POST['username'];
$groupName = (string)$_POST['group'];
$groupName = preg_replace('/\R/', '', $groupName);
if( \OC_Group::inGroup( $userName, $groupName )) {
    $result = \OC_Group::removeFromGroup( $userName, $groupName );
    if ($result){
        OC_JSON::success();
    }
    else{
        OC_JSON::error();
    }
}

