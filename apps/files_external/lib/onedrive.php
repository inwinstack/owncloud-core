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

namespace OC\Files\Storage;

use Icewind\Streams\IteratorDirectory;
use Assetic\Exception\Exception;

set_include_path(get_include_path().PATH_SEPARATOR.
\OC_App::getAppPath('files_external').'/3rdparty/onedrive-php-sdk/');

require_once 'vendor/autoload.php';


class OneDrive extends \OC\Files\Storage\Common {

	private $onedrive_client;
	private $id;
	private $driveFiles;
	private $clientId;
	private $clientSecret;
	private $state;
	private $rootDir;
    
	private static $tempFiles = array();


	public function __construct($params) {
	    
		if (isset($params['configured']) && $params['configured'] === 'true'
			&& isset($params['client_id']) && isset($params['client_secret']) 
			&& json_decode($params['state'])->token != Null
		) {
		    $this->id = 'onedrive::'.$params['client_id'];
		    $this->clientId = $params['client_id'];
		    $this->clientSecret = $params['client_secret'];
		    
		    $this->onedrive_client = new \Krizalys\Onedrive\Client(array(
		            'client_id' => $this->clientId,
		            'state'     => json_decode($params['state'])
		    ));
		    //check if token expired less than 60 second will renew token
		    if ($this->onedrive_client->getTokenExpire() < 60){
                $this->renewToken();		        
		    }
		    
		    $owner = \OC_USER::getUser();
		    $mountData = \OC_Mount_Config::readData($owner);
		    $mountDataDetail = $mountData['user'][$owner];
		    foreach ($mountDataDetail as $key => $value){
		        if(array_key_exists(('client_id'),$value['options'])){
		            if ($value['options']['client_id'] == $params['client_id']){
		                $this->rootDir = str_replace('/'.$owner.'/files/',"",$key);
		            }
		        }
		    }
		} else {
			throw new \Exception('Creating \OC\Files\Storage\OneDrive storage failed');
		}
	}

	public function getId() {
		return $this->id;
	}

	/**
	 * Get the OneDrive_Service_File object for the specified path.
	 * Returns false on failure.
	 * @param string $path
	 * @return \OneDrive_Service_File|false
	 */
	private function getDriveFile($path) {
		// Remove leading and trailing slashes
			$path = trim($path, '/');
		if (isset($this->driveFiles[$path])) {
			return $this->driveFiles[$path];
		} else if ($path === '' || $path === '.' || $path === $this->rootDir) {
			$root = $this->onedrive_client->fetchRoot();
			$this->driveFiles[$this->rootDir] = $root;
			return $root;
		} else {
			// OneDrive SDK does not have methods for retrieving files by path
			// Instead we must find the id of the parent folder of the file
			$root = $this->getDriveFile('');

			$folderNames = explode('/', $path);
			$path = '';
			// Loop through each folder of this path to get to the file
			foreach ($folderNames as $name) {
				// Reconstruct path from beginning
				if ($path === '') {
					$path .= $name;
				} else {
					$path .= '/'.$name;
				}
				if (!isset($this->driveFiles[$path])) {
				    if (dirname($path) === '.' || dirname($path) === $this->rootDir){
				        $root = $this->onedrive_client->fetchRoot();
				        $objects = $root->fetchObjects();
				        foreach($objects as $object){
				            $this->setDriveFile($object->getName(), $object);
				        }
				    }
				    else{
				        $parent = $this->getDriveFile(dirname($path));
				        $objects = $parent->fetchObjects();
				        foreach($objects as $child){
				                $this->setDriveFile(dirname($path).'/'.$child->getName(), $child);
				        }   
				    }
				}
			}
			if (isset($this->driveFiles[$path])){
			    return $this->driveFiles[$path];
			}
			return false;
		}
	}

	/**
	 * Set the OneDrive_Service_File object in the cache
	 * @param string $path
	 * @param OneDrive_Service_File|false $file
	 */
	private function setDriveFile($path, $fileObject) {
		$path = trim($path, '/');
		$this->driveFiles[$path] = $fileObject;
		if ($fileObject === false) {
		    // Set all child paths as false
		    $len = strlen($path);
		    foreach ($this->driveFiles as $key => $file) {
		        if (substr($key, 0, $len) === $path) {
		            $this->driveFiles[$key] = false;
		        }
		    }
		}
	}
    

	public function mkdir($path) {
	    $result  = false;
	    if (!$this->is_dir($path)) {
	        $parentFolder = $this->getDriveFile(dirname($path));
	        if ($parentFolder) {
			    $result = $parentFolder->createFolder(basename($path),'');
	        }
	    }
	    if ($result) {
	        $this->setDriveFile($path, $result);
	        return $result;
	    }
	    return false;
	}

	public function rmdir($path) {
	    if (!$this->isDeletable($path)) {
	        return false;
	    }
	    if (trim($path, '/') === '') {
	        $dir = $this->opendir($path);
	        if(is_resource($dir)) {
	            while (($file = readdir($dir)) !== false) {
	                if (!\OC\Files\Filesystem::isIgnoredDir($file)) {
	                    if (!$this->unlink($path.'/'.$file)) {
	                        return false;
	                    }
	                }
	            }
	            closedir($dir);
	        }
	        $this->driveFiles = array();
	        return true;
	    } else {
	        return $this->unlink($path);
	    }
	}

	public function opendir($path) {
	    $files = array();
	        
        $folder = $this->getDriveFile($path);
        if (!$folder){
            return false;
        }

        $objects = $folder->fetchObjects();
        
        if (count($objects) === 0){
            return IteratorDirectory::wrap($files);
        }
        else{
            foreach ($objects as $object){
                $this->setDriveFile($path.'/'.$object->getName(), $object);
                if ($object->isFolder()){
                    $files[] = $object->getName();
                }
                else{
                    $files[] = $object->getName();
                }
            }
        
            return IteratorDirectory::wrap($files);
        }
	}

	public function stat($path) {
	    $file = $this->getDriveFile($path);
	    if ($file) {
	        $stat = array();
	        if ($this->filetype($path) === 'dir') {
	               $stat['size'] = 0;
	        } else {
	                $stat['size'] = $file->getSize();
	        }
	        $stat['atime'] = $file->getUpdatedTime();
	        $stat['mtime'] = $file->getUpdatedTime();
	        $stat['ctime'] = $file->getUpdatedTime();
	        return $stat;
	    } else {
	        return false;
	   }
	}

	public function filetype($path) {
		if ($path === '') {
			return 'dir';
		} else {
			$fileObject = $this->getDriveFile($path);
			if ($fileObject) {
				if ($fileObject->isFolder()) {
					return 'dir';
				} 
				else {
					return 'file';
				}
			} else {
				return false;
			}
		}
	}

	
	public function isUpdatable($path) {
        return true;
	}

	public function file_exists($path) {
		return (bool)$this->getDriveFile($path);
	}

	public function unlink($path) {
	    $result = false;
		$file = $this->getDriveFile($path);
		if ($file) {
			$fileId = $file->getId();
			$this->onedrive_client->deleteObject($fileId);
			try{
			    
			    $this->onedrive_client->fetchObject($fileId);
			}
			catch (\Exception $e) {
			    $this->setDriveFile($path, false);
			    $result = true;
			}
			return (bool)$result;
		} else {
			return $result;
		}
	}

	public function rename($path1, $path2) {
	    $properties = array();
		$file = $this->getDriveFile($path1);
		if ($file) {
			if (dirname($path1) === dirname($path2)) {
			    $fileId = $file->getId();
			    $newName = basename($path2);
			    $properties['name'] = $newName;
			    try{
			        $this->onedrive_client->updateObject($fileId,$properties);
			    }
			    catch (Exception $e){
			        return false;
			    }
			} else {
			    if ($this->file_exists($path2) || !$this->file_exists(dirname($path2))){
			        return false;
			    }
			    
			    $fileId = $file->getId();
			    $newName = basename($path2);
			    $properties['name'] = $newName;
			    
			    try{
			        
			        $this->onedrive_client->updateObject($fileId,$properties);
			    }
			    catch (Exception $e){
			        return false;
			    }
			    
			    $parentFolder2 = $this->getDriveFile(dirname($path2));
			    
			    try{
			        if (dirname($path2) === '.'){
			            $file->move($parentFolder2->fetchProperties()->id);
			        }
			        else{
			            $file->move($parentFolder2->getId());
			        }
			        
			    }
			    catch (Exception $e){
			        return false;
			    }
			}
			$this->setDriveFile($path1, false);
			$this->setDriveFile($path2, $file);
			 
			return true;
		} else {
			return false;
		}
	}

	public function fopen($path, $mode) {
		$pos = strrpos($path, '.');
		if ($pos !== false) {
			$ext = substr($path, $pos);
		} else {
			$ext = '';
		}
		switch ($mode) {
			case 'r':
			case 'rb':
			    if ($this->filetype($path) == 'dir'){
			        $tmpFile = \OCP\Files::tmpFile($ext);
			        return fopen($tmpFile, $mode);
			    }
				$file = $this->getDriveFile($path);
				if ($file) {
				    $data = $file->fetchContent();
				    $tmpFile = \OCP\Files::tmpFile($ext);
				    file_put_contents($tmpFile, $data);
				    return fopen($tmpFile, $mode);
				}
				return false;
			case 'w':
			case 'wb':
			case 'a':
			case 'ab':
			case 'r+':
			case 'w+':
			case 'wb+':
			case 'a+':
			case 'x':
			case 'x+':
			case 'c':
			case 'c+':
				$tmpFile = \OCP\Files::tmpFile($ext);
				\OC\Files\Stream\Close::registerCallback($tmpFile, array($this, 'writeBack'));
				if ($this->file_exists($path)) {
					$source = $this->fopen($path, 'rb');
					file_put_contents($tmpFile, $source);
				}
				self::$tempFiles[$tmpFile] = $path;
				return fopen('close://'.$tmpFile, $mode);
		}
	}
    
	

	public function writeBack($tmpFile) {
		if (isset(self::$tempFiles[$tmpFile])) {
			$path = self::$tempFiles[$tmpFile];
			$parentFolder = $this->getDriveFile(dirname($path));
			if ($parentFolder) {
			    $data = file_get_contents($tmpFile);
			    // options behavior 1=>FAIL ; 2=>RENAME ; 3=>REPLACE
			    $options = array('name_conflict_behavior' => 3);
			    $result = false;
			    $result = $parentFolder->createFile(basename($path),$data,$options);
			    if ($result) {
			        $this->setDriveFile($path, $result);
			    }

			}
			unlink($tmpFile);
		}
	}
	
	public function getMimeType($path) {
		$file = $this->getDriveFile($path);
		if ($file){
		    if ($file->isFolder()){
		        return 'httpd/unix-directory';
		    }
		    else{
		        $mimetype = \OC::$server->getMimeTypeDetector()->detect($path);
		        return $mimetype;
		    }

		} else {
			return false;
		}
	}

	public function free_space($path) {
	    //array('quota' => 16106127360,'available' => 16106093901)
	    $about = $this->onedrive_client->fetchQuota();
	    return $about->available;

	}

	public function touch($path, $mtime = null) {
		$file = $this->getDriveFile($path);
		$result = false;
		if (is_null($mtime)) {
		    $mtime = time();
		}
		if (!$file) {
			$parentFolder = $this->getDriveFile(dirname($path));
			if ($parentFolder) {
			    $options = array('name_conflict_behavior' => 3);
			    $result = $parentFolder->createFile(basename($path),'',$options);
			}
		}
		if ($result) {
			$this->setDriveFile($path, $result);
		}
		return (bool)$result;
	}

	public function test() {
		if ($this->free_space('')) {
			return true;
		}
		return false;
	}


	/**
	 * check if curl is installed
	 */
	public static function checkDependencies() {
		return true;
	}
    
	/**
	 * Renew the onedrive service api access token when the token will expired
	 */
	private function renewToken(){
	    $user = \OC_User::getUser();
	    $mountData = \OC_Mount_Config::readData($user);
	    $mountDataDetail = $mountData['user'][$user];
	    
	    $this->onedrive_client->renewAccessToken($this->clientSecret);
	    $this->state = $this->onedrive_client->getState();
	    foreach($mountDataDetail as $key => $mountPoint){
	        if(array_key_exists(('client_id'),$mountPoint['options'])){
	            if($mountPoint['options']['client_id'] == $this->clientId){
	                $mountData['user'][$user][$key]['options']['state'] = json_encode($this->state);
	           }
	        }
	    }
	    \OC_Mount_Config::writeData($user, $mountData);
	}
}

