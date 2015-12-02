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
use OC\Files\Cache\HomeCache;

if (\OC_Util::runningOnWindows()) {
	class CephLocal extends MappedLocal {

	}
} else {

	/**
	 * for local filestore, we only have to map the paths
	 */
	class CephLocal extends \OC\Files\Storage\Common {
		protected $datadir;
        
		private function checkObjectExist($path){
		    if (rados_stat(LocalCephStream::getRadosCtx(),$path)){
		        return true;
		    }
		    return false;
		}
		
		private function isRoot($path) {
		    return $path === '.';
		}

		
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
			
			$this->id = 'local::'.$this->datadir;
			$this->key_file = '/etc/ceph/ceph.conf';
			$this->pool_name = $cephPoolName;
			LocalCephStream::init($this->datadir,$this->pool_name,$this->key_file);

		}

		public function __destruct() {
		}

		public function getId() {
		    return $this->id;
		}

		public function mkdir($path) {
			return @mkdir('localceph://'.$this->getSourcePath($path), 0777, true);

		}

		public function rmdir($path) {
		    $path = $this->getSourcePath($path);

		    return rmdir('localceph://'.$path);
		}

		public function opendir($path) {

		    $path = $this->getSourcePath($path);
		    try {
        		return opendir('localceph://'.$path);
		    } catch (Exception $e) {
		        \OCP\Util::logException('files_external', $e);
		        return false;
		    }
		}
		
		public function is_dir($path) {
		    return is_dir('localceph://'.$this->getSourcePath($path));
		}
		
		public function is_file($path) {
		    return is_file('localceph://'.$this->getSourcePath($path));
		}
		
		public function stat($path) {
		    clearstatcache();
		    $path = $this->getSourcePath($path);
		    $result = stat('localceph://'.$path);
		    return $result;
		}
		
		public function filetype($path) {
		    $path = $this->getSourcePath($path);
		    return @filetype('localceph://'.$path);
		}
		
		public function filesize($path) {
		    if ($this->is_dir($path)) {
		        return 0;
		    }
		    return filesize('localceph://'.$this->getSourcePath($path));
		}
		
		public function isReadable($path) {
		    return is_readable('localceph://'.$this->getSourcePath($path));
		}
		
		public function isUpdatable($path) {
		    return is_writable('localceph://'.$this->getSourcePath($path));
		}
		
		public function file_exists($path) {
		    clearstatcache($this->getSourcePath($path));
		    return file_exists('localceph://'.$this->getSourcePath($path));
		}
		
		public function filemtime($path) {
		    clearstatcache($this->getSourcePath($path));
		    return $this->stat($path)['mtime'];
		}
		
		public function touch($path, $mtime = null) {
		    try {
		        if (!is_null($mtime)) {
		                $result = touch('localceph://'.$this->getSourcePath($path), $mtime);
		        } else {
		            $result = touch('localceph://'.$this->getSourcePath($path));
		        }
	            if ($result) {
	                clearstatcache(true, $this->getSourcePath($path));
	            }
	            return $result;
		    } catch (Exception $e) {
		        \OCP\Util::logException('localceph', $e);
		        return false;
		    }
		}
		
		public function file_get_contents($path) {
		    return file_get_contents('localceph://'.$this->getSourcePath($path));
		}
		    
        public function file_put_contents($path, $data) {
            return file_put_contents('localceph://'.$this->getSourcePath($path), $data);
        }
        
        public function unlink($path) {
            if ($this->is_dir($path)) {
                return $this->rmdir($path);
            }
            try {
                $path = $this->getSourcePath($path);
                unlink('localceph://'.$path);
            } catch (Exception $e) {
                \OCP\Util::logException('files_external', $e);
                return false;
            }
            return true;
        }
        
        public function rename($path1, $path2) {
            return rename('localceph://'.$this->getSourcePath($path1), 'localceph://'.$this->getSourcePath($path2));
        }
        
        public function copy($path1, $path2) {
            if ($this->is_dir($path1)) {
                $this->remove($path2);
                return copy('localceph://'.$this->getSourcePath($path1), 'localceph://'.$this->getSourcePath($path2));
            } else {
                return copy('localceph://'.$this->getSourcePath($path1), 'localceph://'.$this->getSourcePath($path2));
            }
        }
        
        public function fopen($path, $mode) {
            if ($this->filetype($path) == 'dir'){
                $path = $path.'/';
            }
            
            return fopen('localceph://'.$this->getSourcePath($path), $mode);
        }
        
        public function hash($type, $path, $raw = false) {
            return hash_file($type, 'localceph://'.$this->getSourcePath($path), $raw);
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
		         if ($item == '.' || $item == '..'){
		             continue;
		         }
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
            $fullPath = $this->datadir . $path;
            return $fullPath;
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
                        $stat['size']);
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
            if($sourceStorage->instanceOfStorage('\OC\Files\Storage\Local')){
                /**
                 * @var \OC\Files\Storage\Local $sourceStorage
                 */

                $rootStorage = new Local(['datadir' => '/']);
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
            if ($sourceStorage->instanceOfStorage('\OC\Files\Storage\Local')) {
            /**
             * @var \OC\Files\Storage\Local $sourceStorage
             */
                $rootStorage = new Local(['datadir' => '/']);
                return $rootStorage->rename($sourceStorage->getSourcePath($sourceInternalPath), $this->getSourcePath($targetInternalPath));
            } else {
                return parent::moveFromStorage($sourceStorage, $sourceInternalPath, $targetInternalPath);
            }
        }
    }
}

