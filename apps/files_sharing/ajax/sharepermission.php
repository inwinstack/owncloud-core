<?php
$dir = isset($_GET['dir']) ? $_GET['dir'] : '';
$filename = isset($_GET['file']) ? $_GET['file'] : '';
if(!empty($filename))
{

	$path = $dir.'/'.$filename;
    $writeable = \OC\Files\Filesystem::isUpdatable($path);
    OCP\JSON::success(array(
        'writeable' => $writeable
        )
	);

} else {
    OCP\JSON::error(array('data' => array( 'message' => 'Invalid file path supplied.')));

}


?>
