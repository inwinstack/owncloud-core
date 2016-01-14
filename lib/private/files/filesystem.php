<?php

// rewrite original Filesystem method to write log

namespace OC\Files;

class Filesystem extends OCFilesystem {

	static public function touch($path, $mtime = null) {
		$result = parent::touch($path, $mtime);
		if($result === false) \OCP\Util::writeLog('activity',"user:" . \OCP\User::getDisplayName() . " action:create type:file fail", \OCP\Util::WARN);
		return $result;
	}

	static public function mkdir($path) {
		$result = parent::mkdir($path);
		if($result === false) \OCP\Util::writeLog('activity',"user:" . \OCP\User::getDisplayName() . " action:create type:dir fail", \OCP\Util::WARN);
		return $result;
	}

	static public function unlink($path) {
		$result = parent::unlink($path);
		if ($result === false) \OCP\Util::writeLog('activity',"user:" . \OCP\User::getDisplayName() . " action:delete type:file fail", \OCP\Util::WARN);
		return $result;
	}

	static public function rmdir($path) {
		$result = parent::rmdir($path);
		if ($result === false) \OCP\Util::writeLog('activity',"user:" . \OCP\User::getDisplayName() . " action:delete type:dir fail", \OCP\Util::WARN);
		return $result;
	}

	static public function rename($path1, $path2) {
		$result = parent::rename($path1, $path2);
		if($result === false) \OCP\Util::writeLog('activity',"user:" . \OCP\User::getDisplayName() . " action:rename fail", \OCP\Util::WARN);
		return $result;
	}

	static public function copy($path1, $path2) {
		$result = parent::copy($path1, $path2);
		if($result === false) \OCP\Util::writeLog('activity',"user:" . \OCP\User::getDisplayName() . " action:copy fail", \OCP\Util::WARN);
		return $result;
	}

	static public function readfile($path) {
		$result = parent::readfile($path);
		if ($result === false ) \OCP\Util::writeLog('activity',"user:" . \OCP\User::getDisplayName() . " action:read fail", \OCP\Util::WARN);
		return $result;
	}

	static public function readdir($path) {
		$result = parent::readdir($path);
		if ($result === false ) \OCP\Util::writeLog('activity',"user:" . \OCP\User::getDisplayName() . " action:read fail", \OCP\Util::WARN);
		return $result;
	}

}