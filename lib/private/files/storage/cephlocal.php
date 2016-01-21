<?php
/**
 * @author Duncan Chiang <duncan.c@inwinstack.com>
 * @author Chung-Ting kao <chungting.k@inwinstack.com>
 *
 * @copyright Copyright (c) 2015, inwinSTACK, Inc.
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

namespace OC\Files\Storage;
use OC\Files\Stream\LocalCephStream;
/**
 * for local filestore, we only have to map the paths
 */
class CephLocal extends \OC\Files\Storage\Common {
    
	protected $datadir;
	private static $connectionCountArray = array();
	
	public function __construct($arguments) {
		if ( ! in_array("localceph", stream_get_wrappers())) {
		        stream_wrapper_register('localceph', 'OC\Files\Stream\LocalCephStream');
		    }
	    $this->datadir = $arguments['datadir'];
		if (substr($this->datadir, -1) !== '/') {
			$this->datadir .= '/';
		}
		
		$systemConfig = \OC::$server->getSystemConfig();
		$cephPoolName = $systemConfig->getValue("cephpoolname","owncloud");
		
		$this->key_file = '/etc/ceph/ceph.conf';
		$this->pool_name = $cephPoolName;
		LocalCephStream::init($this->datadir,$this->pool_name,$this->key_file);
		$request = \OC::$server->getRequest();
		$reqId = $request->getId();

        if(!isset(self::$connectionCountArray)){
            self::$connectionCountArray = array();
        }
        
        if (array_key_exists($reqId,self::$connectionCountArray)){
            self::$connectionCountArray[$reqId]++;
        }
        else{
            self::$connectionCountArray[$reqId] = 1 ;
        }
	}

	public function __destruct() {
	    $request = \OC::$server->getRequest();
	    $reqId = $request->getId();
	    //Caculate current connection count by reqId.
        if (array_key_exists($reqId,self::$connectionCountArray)){
            self::$connectionCountArray[$reqId]--;
            if (self::$connectionCountArray[$reqId] <= 0) {
                unset(self::$connectionCountArray[$reqId]);
            }
        }
        
        //Check connection array whether is empty,
        //if yes, disconnect librados.
        if (empty(self::$connectionCountArray)){
            LocalCephStream::uninit();
            self::$connectionCountArray = null;
        }
	}

	public function getId() {
		return 'localceph::' . $this->datadir;
	}

	public function mkdir($path) {

		return @mkdir($this->getSourcePath($path), 0777, true);
	}

	public function rmdir($path) {
		return rmdir($this->getSourcePath($path));
	}

	public function opendir($path) {
		return opendir($this->getSourcePath($path));
	}

	public function is_dir($path) {
// 	    clearstatcache($this->getSourcePath($path));
		return is_dir($this->getSourcePath($path));
	}

	public function is_file($path) {
// 	    clearstatcache($this->getSourcePath($path));
		return is_file($this->getSourcePath($path));
	}

	public function stat($path) {
		
// 	    clearstatcache($this->getSourcePath($path));
	    clearstatcache();
	    $result = stat($this->getSourcePath($path));
	    return $result;
	}

	public function filetype($path) {
// 	    clearstatcache($this->getSourcePath($path));
		$path = $this->getSourcePath($path);
		return @filetype($path);
	}

	public function filesize($path) {
	    
		if ($this->is_dir($path)) {
			return 0;
		}
// 		clearstatcache($this->getSourcePath($path));
		$fullPath = $this->getSourcePath($path);
// 		if (PHP_INT_SIZE === 4) {
// 			$helper = new \OC\LargeFileHelper;
// 			return $helper->getFilesize($fullPath);
// 		}
		return filesize($fullPath);
	}

	public function isReadable($path) {
// 	    clearstatcache($this->getSourcePath($path));
		return is_readable($this->getSourcePath($path));
	}

	public function isUpdatable($path) {
// 	    clearstatcache($this->getSourcePath($path));
		return is_writable($this->getSourcePath($path));
	}

	public function file_exists($path) {
//  	    clearstatcache($this->getSourcePath($path));
		return file_exists($this->getSourcePath($path));
	}

	public function filemtime($path) {
		clearstatcache($this->getSourcePath($path));
		return @filemtime($this->getSourcePath($path));
	}

	public function touch($path, $mtime = null) {
		// sets the modification time of the file to the given value.
		// If mtime is nil the current time is set.
		// note that the access time of the file always changes to the current time.
		if ($this->file_exists($path) and !$this->isUpdatable($path)) {
			return false;
		}
		if (!is_null($mtime)) {
		     if ($this->filetype($path) == 'dir' && substr($path,-1) !== '/'){
		        $path = $path.'/';
		    }
			$result = touch($this->getSourcePath($path), $mtime);
		} else {
			$result = touch($this->getSourcePath($path));
		}
		if ($result) {
			clearstatcache(true, $this->getSourcePath($path));
		}

		return $result;
	}

	public function file_get_contents($path) {
		return file_get_contents($this->getSourcePath($path));
	}

	public function file_put_contents($path, $data) {
		return file_put_contents($this->getSourcePath($path), $data);
	}

	public function unlink($path) {
		if ($this->is_dir($path)) {
			return $this->rmdir($path);
		} else if ($this->is_file($path)) {
			return unlink($this->getSourcePath($path));
		} else {
			return false;
		}

	}

	public function rename($path1, $path2) {
	    $srcParent = dirname($path1);
	    $dstParent = dirname($path2);
	    if (!$this->isUpdatable($srcParent)) {
	        \OCP\Util::writeLog('core', 'unable to rename, source directory is not writable : ' . $srcParent, \OCP\Util::ERROR);
	        return false;
	    }

	    if (!$this->isUpdatable($dstParent)) {
	        \OCP\Util::writeLog('core', 'unable to rename, destination directory is not writable : ' . $dstParent, \OCP\Util::ERROR);
	        return false;
	    }

	    if (!$this->file_exists($path1)) {
	        \OCP\Util::writeLog('core', 'unable to rename, file does not exists : ' . $path1, \OCP\Util::ERROR);
	        return false;
	    }

	    if ($this->is_dir($path2)) {
	        $this->rmdir($path2);
	    } else if ($this->is_file($path2)) {
	        $this->unlink($path2);
	    }

	    if ($this->is_dir($path1)) {
	        // we cant move folders across devices, use copy instead
	        $stat1 = stat(dirname($this->getSourcePath($path1)));
	        $stat2 = stat(dirname($this->getSourcePath($path2)));
	        if ($stat1['dev'] !== $stat2['dev']) {
	            $result = $this->copy($path1, $path2);
	            if ($result) {
	                $result &= $this->rmdir($path1);
	            }
	            return $result;
	        }
	    }
	    return rename($this->getSourcePath($path1), $this->getSourcePath($path2));
	}

	public function copy($path1, $path2) {

	    return copy($this->getSourcePath($path1), $this->getSourcePath($path2));
	}

	public function fopen($path, $mode) {
	    if ($this->filetype($path) == 'dir' && substr($path,-1) !== '/'){
	        $path = $path.'/';
	    }
		return fopen($this->getSourcePath($path), $mode);
	}

	public function hash($type, $path, $raw = false) {
		return hash_file($type, $this->getSourcePath($path), $raw);
	}

	public function free_space($path) {
		$result = \OCP\Util::getPoolMaxAvailSpace($this->pool_name);
        if ($result === false) {
            return \OCP\Files\FileInfo::SPACE_UNKNOWN;
        }
        return $result;
	}

	public function search($query) {
		return $this->searchInDir($query);
	}

	public function getLocalFile($path) {
		return $this->getSourcePath($path);
	}

	public function getLocalFolder($path) {
		return $this->getSourcePath($path);
	}

	/**
	 * @param string $query
	 * @param string $dir
	 * @return array
	 */
	protected function searchInDir($query, $dir = '') {
		$files = array();
		$physicalDir = $this->getSourcePath($dir);
		foreach (scandir($physicalDir) as $item) {
			if (\OC\Files\Filesystem::isIgnoredDir($item))
				continue;
			$physicalItem = $physicalDir . '/' . $item;

			if (strstr(strtolower($item), strtolower($query)) !== false) {
				$files[] = $dir . '/' . $item;
			}
			if (is_dir($physicalItem)) {
				$files = array_merge($files, $this->searchInDir($query, $dir . '/' . $item));
			}
		}
		return $files;
	}

	/**
	 * check if a file or folder has been updated since $time
	 *
	 * @param string $path
	 * @param int $time
	 * @return bool
	 */
	public function hasUpdated($path, $time) {
		if ($this->file_exists($path)) {
			return $this->filemtime($path) > $time;
		} else {
			return true;
		}
	}

	/**
	 * Get the source path (on disk) of a given path
	 *
	 * @param string $path
	 * @return string
	 */
	public function getSourcePath($path) {
	    if ( substr($path,0,13) == 'localceph:///' ){
	        $path = substr($path,13);
	    }
		$fullPath = $this->datadir . $path;
		return 'localceph://'.$fullPath;
	}

	/**
	 * {@inheritdoc}
	 */
	public function isLocal() {
		return true;
	}

	/**
	 * get the ETag for a file or folder
	 *
	 * @param string $path
	 * @return string
	 */
	public function getETag($path) {
		if ($this->is_file($path)) {
			$stat = $this->stat($path);
			return md5(
				$stat['mtime'] .
				$stat['ino'] .
				$stat['dev'] .
				$stat['size']
			);
		} else {
			return parent::getETag($path);
		}
	}

	/**
	 * @param \OCP\Files\Storage $sourceStorage
	 * @param string $sourceInternalPath
	 * @param string $targetInternalPath
	 * @return bool
	 */
	public function copyFromStorage(\OCP\Files\Storage $sourceStorage, $sourceInternalPath, $targetInternalPath) {
		if($sourceStorage->instanceOfStorage('\OC\Files\Storage\CephLocal')){
			/**
			 * @var \OC\Files\Storage\CephLocal $sourceStorage
			 */
			$rootStorage = new CephLocal(['datadir' => '/']);
			return $rootStorage->copy($sourceStorage->getSourcePath($sourceInternalPath), $this->getSourcePath($targetInternalPath));
		} else {
			return parent::copyFromStorage($sourceStorage, $sourceInternalPath, $targetInternalPath);
		}
	}

	
	/**
	 * @param \OCP\Files\Storage $sourceStorage
	 * @param string $sourceInternalPath
	 * @param string $targetInternalPath
	 * @return bool
	 */
	public function moveFromStorage(\OCP\Files\Storage $sourceStorage, $sourceInternalPath, $targetInternalPath) {
		if ($sourceStorage->instanceOfStorage('\OC\Files\Storage\CephLocal')) {
			/**
			 * @var \OC\Files\Storage\CephLocal $sourceStorage
			 */
			$rootStorage = new CephLocal(['datadir' => '/']);
			return $rootStorage->rename($sourceStorage->getSourcePath($sourceInternalPath), $this->getSourcePath($targetInternalPath));
		} else {
			return parent::moveFromStorage($sourceStorage, $sourceInternalPath, $targetInternalPath);
		}
	}
}

