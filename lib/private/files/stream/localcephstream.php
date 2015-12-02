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

namespace OC\Files\Stream;

use Assetic\Exception\Exception;

class LocalCephStream {
    /* Properties */
    public $context;

    const MODE_FILE = 0100000;
    const MODE_DIR = 0040000;
    
    private static $poolName = 'owncloud';
    private static $configFile = '/etc/ceph/ceph.conf';
    private static $dataDir;
    private static $radosIoctx;
    private static $rados;
    private static $connectionCount = 0;
    private static $connectionCountArray;
    
    
    private $oid;
    private $mode;
    private $fSize;
    private $aioComp = array();
    
    private $dirIndex;
    private $dirFiles;
    
    private $position = 0;
    private $stat = ['psize' => 0];
    private $writable = true;
    
    
    /**
     * @var int $partSize The split object of part size.
     */
    private $partSize = 8388608;

    /**
	 * @var array[][] $partsInfo Store parts info in an array of [ 'partID', size, ].
	 */
    private $partsInfo = array();

    /**
	 * @var int $partIndex Current parts index.
	 */
    private $partIndex = 0;

    /**
	 * @var int $cacheHeadPos Current parts head position in file.
	 */
    private $cacheHeadPos = 0;

    /**
	 * @var string $cache Preload one part to accelerate reading and writing speed.
	 */
    private $cache = '';
    
    /**
     * @var int $getMyPid Excute the request api's process ID.
     */
    private $getMyPid = 0;
    
    /**
     * @var string $hostName The hostname to run the owncloud service.
     */
    private $hostName = '';
    
    /**
     * @var string $fileId To encode $oid by md5 encrympt.
     */
    private $fileId = 0;
    
    /**
     * @var string $fileType To point the $oid's type is dir or file.
     */
    private $fileType = false;
    
    
    
    /* =====Consul Parameters===== */
    private $consul;
    private $systemConfig;
    private $sessionID;
    
    /**
     * @var int $maxReTrycount The max failed count can tolerate to execute consul rest api.
     */
    private $maxReTrycount = 3;
    
    /**
     * @var int $retryCount The current count to retry execute consul rest api.
     */
    private $retryCount = 0;
    
    /** 
     * Prepare and initial ceph env by librados.
     * 
     * @param string $dataDir The root data folder.
     * @param string $poolName The pool name that save dataDir related objects.
     * @param string $configFile The absolute path that saved ceph config file.
     */
    public static function init($dataDir, $poolName='owncloud', $configFile='/etc/ceph/ceph.conf') {
        //expect dataDir format: /var/www/owncloud/data/admin/
        
    	self::$poolName = $poolName;
    	self::$configFile = $configFile;
    	self::$dataDir = $dataDir;
    	self::format();
    }
    public static function uninit() {
    
        rados_ioctx_destroy(LocalCephStream::$radosIoctx);
        rados_shutdown(LocalCephStream::$rados);
        LocalCephStream::$radosIoctx = null;
        LocalCephStream::$rados = null;
    }
    /** 
     * If root data folder not exists,will create.
     */
    public static function format() {
    	// Create an ioctx
    	$ioctx = LocalCephStream::getRadosCtx();
    	// create root folder if not existed
    	if (!self::checkObjectExist(self::$dataDir)){
    	   mkdir('localceph://'.self::$dataDir);
    	}
    }
    public static function getDataDir(){
        return self::$dataDir;
    }
    /**
     * Get rados connection instance.
     *
     * @return rados_create instance on success.
     * @throws \Exception
     */
    public static function getRados() {
    
        // return existed ioctx
        if (isset(LocalCephStream::$rados)) {
            return LocalCephStream::$rados;
        }
    
        // create rados
        LocalCephStream::$rados = rados_create();
    
        // read config file
    
        if (!rados_conf_read_file(LocalCephStream::$rados, LocalCephStream::$configFile)) {
            throw new Exception('Could not read rados config file.');
        }
    
        // connect
        if (!rados_connect(LocalCephStream::$rados)) {
            throw new Exception('Could not connect to rados.');
        }
        return LocalCephStream::$rados;
    }
    
    /**
     * Get rados io instance.
     * 
     * @return rados_ioctx_create instance on success.
     * @throws \Exception
     */
    public static function getRadosCtx() {
        
        // return existed ioctx
        if (isset(LocalCephStream::$radosIoctx)) {
            return LocalCephStream::$radosIoctx;
        }

        $rados = LocalCephStream::getRados();
        // create ioctx and return
        LocalCephStream::$radosIoctx = rados_ioctx_create($rados, LocalCephStream::$poolName);
        
        return LocalCephStream::$radosIoctx;
    }
    
    /**
     * Test can get pool status.
     * 
     * @return bool Returns TRUE on success or FALSE on failure.
     */
    public static function test() {
        $result = rados_ioctx_pool_stat(LocalCephStream::getRadosCtx());
        
        if (isset($result)) {
            return true;
        } else {
            return false;
        }
    }

    public function __construct() {
        
        // According to reqId, keep current Localcephstream connection count.
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
        
        //Check to unlock file.
        $this->unLockFile($reqId);
        
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
            rados_ioctx_destroy(LocalCephStream::$radosIoctx);
            rados_shutdown(LocalCephStream::$rados);
            LocalCephStream::$radosIoctx = null;
            LocalCephStream::$rados = null;
            self::$connectionCountArray = null;
            $this->consul = null;
            $this->systemConfig = null;
            $this->sessionID = null;
        }
 
    }
    
    public function unLockFile($reqId){
        //Check 1.fileId is not empty,
        //      2.getMyPid equals reqId,
        //      3.is locked by localhost, 
        //      4.and object is exist.
        //If set up, will delete consul's fileId key.
        
        if ( $this->fileId != '' && $this->getMyPid == $reqId && 
             $this->hostName == gethostname() && self::checkObjectExist($this->oid)){
            
                $deletefileLock =  $this->consul -> deleteKeyValue($this->fileId);
                $deleleFileLockHttpCode = $deletefileLock['httpCode'];
                if ($deleleFileLockHttpCode == 200){
                    $this->fileId = '';
                }
        }
    }
    
    /**
     * Update the owncloud service session ID in consul and config file.
     *
     * @return bool Returns TRUE on success or FALSE on failure.
     */
    private function renewLocalhostSessionID() {
        $count = 0;
        $max_try_count = 10;
        while($count<$max_try_count){
            
            $result = $this->consul->createServiceSession($data=Null,$allowDuplicated=false);
    
            if ($result['httpCode'] == 200){
                $this->systemConfig->setValue("localhostsession", $result['result']['ID']);
                return $this->consul->getSessionListByNode(gethostname());
            }
            elseif($result === false){
                return $this->consul->getSessionListByNode(gethostname());
            }
            
            $count+=1;
            sleep(1);
        }
        return false;
    }
    
    
    /**
     * Check oid object is exist in pool.
     *
     * @param string $oid The object name saved in ceph pool.
     * @return bool Returns TRUE on success or FALSE on failure.
     */
    public static function checkObjectExist($oid){
        //expect oid format: var/www/owncloud/data/

        if (rados_stat(LocalCephStream::getRadosCtx(),$oid)){
            return true;
        }
        return false;
    }
    
    /**
     * Parse given path and return full path.
     *
     * @param string $path The file URL which should be parsed.
     * @return string correctly encoded full path.
     */
    private static function parsePath($path) {
        //case1:
        //expect path format: localceph:///var/www/owncloud/data/admin/fname.ext
        //return '/var/www/owncloud/data/admin/fname.ext'
        //case2:
        //expect path format: localceph:///var/www/owncloud/data/admin/
        //return '/var/www/owncloud/data/admin/'
        //case3:
        //expect path format: localceph:///
        //return '/'

        $info = array();
        $info['fullPath'] = substr($path, 12);
        return $info;
    }

    /**
     *  Parse path to correct oid by type.
     *  
     *  @param string $path The path URL which should be parsed to oid.
     *  @param string $type The path type.
     *  @return string The correct oid format.
     */
    private static function pathToOid($path, $type) {
        //expect path format: /var/www/owncloud/data/admin/
        //expect type format: dir/file
        
        // append user name to the front
        /* $oid = \OC_User::getUser().$path; */
        $oid = $path;
        
        // if the target is a folder, make sure it ends with "/"
        if ($type == 'dir'&& substr($oid, -1) !== '/') {
            $oid .= '/';
        }
        
        // if the path ends with '/' while give type 'file'
        if ($type == 'file' && substr($oid, -1) == '/' ) {
            // return empty oid
            $oid = '';
        }
        
        return $oid;
    }

    /**
     * Return the oid and type of given stream path.
     * 
     * @param string $streamPath
     * @return array() return [oid, filetype] 
     */
    
    private function oidFiletype($streamPath) {
        //case1:
        //expect streamPath format: localceph:///var/www/owncloud/data/admin/
        //except return ['/var/www/owncloud/data/admin/','dir']
        //case2:
        //expect streamPath format: localceph:///var/www/owncloud/data/admin/files/a.pdf
        //except return ['/var/www/owncloud/data/admin/files/a.pdf','file']
        //case3:
        //except return false;
        
        // parse path to get the oid
        $info = self::parsePath($streamPath);
        $foid = self::pathToOid($info['fullPath'], 'file');
        $doid = self::pathToOid($info['fullPath'], 'dir');
        $ioctx = LocalCephStream::getRadosCtx();
           
        if (rados_stat($ioctx, $foid) !== false ) {
            return [$foid, 'file'];
        }
        if (rados_stat($ioctx, $doid) !== false ) {
            return [$doid, 'dir'];
        }
        return false;
    }


    /**
     * Read the whole object. 
     * 
     * @param string $oid
     * @return string
     */
    private static function readObject($oid) {
        //expect streamPath format: /var/www/owncloud/data/admin/
        //return "{"files/":"", "a.pdf":""}"
        //expect streamPath format: /var/www/owncloud/data/admin/files/a.pdf
        //return "[["38913e1d6a7b94cb0f55994f679f59561443079935",228761]]"
        
        $ioctx = LocalCephStream::getRadosCtx();
        $objStat = rados_stat($ioctx, $oid);
        return rados_read($ioctx, $oid, $objStat['psize']);
    }

    /**
     * Load file metadata and update $partsInfo
     */
    private function loadPartsInfo() {
        // load file metadata
        
        $fileMeta = self::readObject($this->oid);
        
        $this->partsInfo = json_decode($fileMeta, true);
        
    }

    /**
     * Update file metadata according to $partsInfo
     */
    private function savePartsInfo() {
        
        rados_write_full(LocalCephStream::getRadosCtx(),
                         $this->oid,
                         json_encode($this->partsInfo));
    }

    /** 
     * Load part of file into cache according to current (parts) position
     */
    private function loadPartsCache() {

        $seekEnd = 0;
        $partIndex = 0;
        $this->cacheHeadPos = 0;

        // calculate current partIndex and cacheHeadPos
        // according to current file position
        while($partIndex < count($this->partsInfo)) {

            // jump seekEnd to next part
            $seekEnd += $this->partsInfo[$partIndex][1];

            if ($seekEnd <= $this->position) {
                $partIndex ++;
                $this->cacheHeadPos = $seekEnd;
            } else {
                break;
            }
        }

        if( $partIndex == count($this->partsInfo) ) {
            // create new part
            $this->cache = '';
            $this->partIndex = $partIndex;

        } else {

            if( ($partIndex == 0 && $this->cache == '') || $partIndex > $this->partIndex ) {
                // or load current part
                $this->cache = self::readObject($this->partsInfo[$partIndex][0]);
                $this->partIndex = $partIndex;
            }
        }
    }

    /**
     * Save the cache to a new part and update partsInfo, partIndex.
     * Also clean up the cache and update cacheHeadPos.
     */
    private function saveCache() {
        


        // if cache is not empty
        if ($this->cache != '') {
            $comp = rados_aio_create_completion();
            $this->aioComp[] = $comp;
            // create oid and save the cache to a new part object

            $partID = $this->partsInfo[$this->partIndex][0];
            rados_aio_write_full(LocalCephStream::getRadosCtx(), $partID,$comp,$this->cache);
            
            // update parts info
            $this->partsInfo[$this->partIndex] = [$partID, strlen($this->cache)];
            if($this->partsInfo[$this->partIndex][1] >= $this->partSize) {
            
                $this->partIndex++;
                
                $partID =  md5(rand(0,1000)).time();
                $this->partsInfo[$this->partIndex] = array($partID, 0);
            }
            // update cache info
            $this->cacheHeadPos += $this->partSize;
            $this->cache = '';
        }
    }

    /**
     * Update upper folder metadata info.
     * 
     * @param string $action
     * @param string $type
     */
    private function updateFSMeta($action, $type) {
        //expect action format: create
        //expect type format: dir/file
        //expect action format: delete
        //expect type format: dir/file
        
        // get meta content
        
        $dirOid = dirname($this->oid).'/';

        $itemName = basename($this->oid);

        if ($type == 'dir') {
            $itemName .= '/';
        }        

        // update meta content
        if ($action == 'delete') {
            rados_rmxattr(LocalCephStream::getRadosCtx(),
                          $dirOid,
                          $itemName);
            
        } else {
            rados_setxattr(LocalCephStream::getRadosCtx(),
                          $dirOid,
                          $itemName,
                          '');
        }

    }

    /** Directory Handle **/

    /**
     * Open directory handle
     * 
     * @param string $path
     * @param int $options
     * @return bool Returns TRUE on success or FALSE on failure.
     */
    public function dir_opendir($path , $options) {
        //expect path format: /var/www/owncloud/data/admin/
        //expect options format: 0
        //expect path format: /var/www/owncloud/data/admin/files
        //expect options format: 0

        // initial files array
        $this->dirIndex = 0;
        $this->dirFiles = array();

        $info = self::parsePath($path);
        $oid = self::pathToOid($info['fullPath'],'dir');
        
        // the folder doesn't existed
        if (!self::checkObjectExist($oid)){
            return false;
        }
        
        $dirContent = rados_getxattrs(LocalCephStream::getRadosCtx(),
                $oid);

        foreach ($dirContent as $name => $value) {
            $this->dirFiles[] = basename($name);
        }
        return true;
    }

    /**
     * Read entry from directory handle.
     * 
     * @return string Should return string representing the next filename, 
     * or FALSE if there is no next file.
     */
    public function dir_readdir() {

        if ($this->dirIndex >= count($this->dirFiles)) {
            return false;
        }

        $filename = $this->dirFiles[$this->dirIndex];
        $this->dirIndex++;

        // remove tailing '/'
        return rtrim($filename,'/');
    }

    /**
     * Close directory handle.
     * 
     * @return bool Returns TRUE on success or FALSE on failure.
     */
    public function dir_closedir() {

        return true;
    }

    /**
     * Rewind directory handle.
     * 
     * @return bool Returns TRUE on success or FALSE on failure.
     */
    public function dir_rewinddir() {
        $this->dirIndex = 0;
        return true;
    }

    /**
     * Create a directory.
     * 
     * @param string $path
     * @param int $mode
     * @param int $options
     * @return bool Returns TRUE on success or FALSE on failure.
     */
    public function mkdir($path, $mode, $options) {
        //expect path format: localceph:///var/www/owncloud/data/admin/files/2
        //expect mode format: 511
        //expect options format: 8
 
        // get rados ioctx
        $ioctx = LocalCephStream::getRadosCtx();

        // parse path to get the oid
        $info = self::parsePath($path);
        $this->oid = self::pathToOid($info['fullPath'], 'dir');

        // if create root folder
        $rootOid = \OC_Config::getValue("datadirectory", \OC::$SERVERROOT . "/data")."/";
        
        // $rootOid = '/var/www/owncloud/data/';
        if ($this->oid == $rootOid) {

            // create folder object
            rados_write_full($ioctx,
                             $this->oid,
                             json_encode(array()));


        } else {
            // if parent folder existed
            if ($this->checkObjectExist($this->oid)){
                return false;
            }
            if (rados_stat($ioctx, dirname($this->oid).'/')) {

                // create folder object
                rados_write_full($ioctx,
                                 $this->oid,
                                 json_encode(array()));

                // update parent folder metadata
                $this->updateFSMeta('create', 'dir');

            } else {
                // cannot create folder recursively
                return false;
            }
        }
        return true;
    }

    /**
     * Renames a file or directory
     *
     * @param string $streamPathFrom The URL to the current file.
     * @param string $streamPathTo The URL which the streamPathFrom should be renamed to.
     * @return bool Returns TRUE on success or FALSE on failure.
     */
    public function rename($streamPathFrom, $streamPathTo) {
        //dir
        //except path format: localceph:///var/www/owncloud/data/admin/files/1
        //except path format: localceph:///var/www/owncloud/data/admin/files/2
        //file
        //except path format: localceph:///var/www/owncloud/data/admin/files/a.txt
        //except path format: localceph:///var/www/owncloud/data/admin/files/b.txt
        $optArray = stream_context_get_options($this->context);
        
        // get the source oid and source type
        list($soid, $type) = $this->oidFiletype($streamPathFrom);
        
        // prepare the ioctx
        $ioctx = LocalCephStream::getRadosCtx();
        
        
        //prepare write to new object
        $info = self::parsePath($streamPathTo);
        $toid = self::pathToOid($info['fullPath'], $type);
        
        if ($type != 'dir'){
            // get file object content
            $content = self::readObject($soid);
            
            // write to new object
            rados_write_full($ioctx, $toid, $content);
            
            // set xattrs to new object
            $fSize = intval(rados_getxattr(LocalCephStream::getRadosCtx(), $soid,'fileSize',24 ));
            rados_setxattr( LocalCephStream::getRadosCtx(),
                            $toid,
                            'fileSize',
                            strval($fSize));
        }
        else{
            // find files/folders in this folder
            $items = rados_getxattrs($ioctx,
                                     $soid);
            
            rados_write_full($ioctx, $toid, json_encode(array()));
            foreach ($items as $name => $value) {
            
                $opts = array('localceph'=>array('renameDown'=>true));
                $ctx = stream_context_create($opts);
                rados_setxattr($ioctx,$toid,$name,'');
                
                rename('localceph://'.$soid.$name,
                       'localceph://'.$toid.$name,
                       $ctx
                );
            }
        }
        // delete old object
        rados_remove($ioctx, $soid);
        
        // update parent folder meta if this is level 1
        if ( !array_key_exists('localceph', $optArray) || !array_key_exists('renameDown', $optArray['localceph'])) {

            // delete item from source path
            $this->oid = $soid;

            $this->updateFSMeta('delete',$type);
            // add item to target path
            $this->oid = $toid;
            $this->updateFSMeta('create',$type);
        }
        
        return true;
    }

    /**
     * Remove a directory.
     *
     * @param string $path The directory URL which should be removed.
     * @param int $options A bitwise mask of values, such as STREAM_MKDIR_RECURSIVE.
     * @return bool Returns TRUE on success or FALSE on failure.
     */
    public function rmdir($path, $options) {
        //expect path format: localceph:///var/www/owncloud/data/admin/files_trashbin/files/3.d1443083301
        //expect options format: 8

        // parse path to get oid
        $info = self::parsePath($path);
        $this->oid = self::pathToOid($info['fullPath'], 'dir');

        // delete files/folders in this folder

        $items = rados_getxattrs(LocalCephStream::getRadosCtx(),$this->oid);
        
        
        if( count($items) == 0) {
            // if this is an empty folder, delete folder object
            rados_remove(LocalCephStream::getRadosCtx(), $this->oid);

        } else {
            // else delete every files and folders in this folder
            foreach ($items as $name => $value) {
            
                $fPath = 'localceph://'.$this->oid.$name;
                if (substr($name,-1) == '/') {
                    // recursive remove items in folder
                    rmdir($fPath);
                
                    //delete folder object
                    rados_remove(LocalCephStream::getRadosCtx(), $this->oid);
                } else {
                    unlink($fPath);
                }
            }
        }
        // delete itself
        rados_remove(LocalCephStream::getRadosCtx(), $this->oid);
        
        // update metadata in parent folder object
        $this->updateFSMeta('delete', 'dir');
        return true;
    }

    /** File Handle **/
    
    /**
     * Opens file or URL.
     *
     * @param string $path Specifies the URL that was passed to the original function.
     * @param int $mode The mode used to open the file.
     * @param int $options Holds additional flags set by the streams API.
     * @return bool Returns TRUE on success or FALSE on failure.
     */
    public function stream_open($path, $mode, $options) {
        //expect path format: localceph:///var/www/owncloud/data/admin/files_trashbin/files/3.d1443083301
        //expect options format: 0
        //expect path format: localceph:///var/www/owncloud/data/admin/files_trashbin/files/
        //expect options format: 0
        
//         \OCP\Util::writeLog('LocalCephStream','======open $path===='.$path, \OCP\Util::ERROR);

        // parsePath and get oid
        if (substr($path,-1) == '/'){
            $this->fileType = 'dir';
        }
        else{
            $this->fileType = 'file';
        }
        
        $info = self::parsePath($path);
        $this->oid = self::pathToOid($info['fullPath'], '');
        // save mode
        $this->mode = $mode;
        
        
        $fileExisted = self::checkObjectExist($this->oid);

        // get file size from object attribute
        try{
            if ($fileExisted){
                $xattrResult = rados_getxattrs(LocalCephStream::getRadosCtx(),
                                               $this->oid);
                if (isset($xattrResult['fileSize'])){
                    $fSize = intval($xattrResult['fileSize']);
                }
                else{
                    $fSize = 0;
                }
            }
            else{
                $fSize = 0;
            }
            $this->fSize = $fSize;
            
        }catch (Exception $e) {
            return false;
        }
        
        switch ($mode) {
            
        case 'r':
        case 'r+':
        case 'rb':
        case 'rb+':
            if (!$fileExisted) {
                // if file not existed
                return false;
            }
            
            if (substr($mode,-1) == '+' && $this->fileType == 'dir'){
                return false;
            }
            
            if($this->fileType == 'dir'){
                break;
            }

            // setup writable
            if (substr($mode,-1) == '+'){
                $this->writable = true;
            }
            else{
                $this->writable = false;
            }
            // load partsInfo
            $this->loadPartsInfo();
            $this->position = 0;
            break;
        case 'a':
        case 'a+':
            if($this->fileType == 'dir'){
                return false;
            }
       
            $this->writable = true;


            if ($fSize && $this->fSize > 0) {
                $this->position = $this->fSize;
                $this->loadPartsInfo();

            }
            $this->savePartsInfo();
            $partIndex == count($this->partsInfo);
            break;
        case 'x':
        case 'x+':
            if($this->fileType == 'dir'){
                return false;
            }
            if ($fileExisted) {
                // if file existed, return false.
                return false;
            }

            $this->writable = true;
            break;
        case 'w+':
        case 'wb+':
        case 'c+':
        case 'w':
        case 'wb':
        case 'c':
            if($this->fileType == 'dir'){
                return false;
            }
            $this->writable = true;

            if ($mode[0] == 'w'){
                
                if ($fSize){
                    $this->loadPartsInfo();
                }
                if(!empty($this->partsInfo)){
                    foreach ($this->partsInfo as $i){
                        rados_remove(LocalCephStream::getRadosCtx(), $i[0]);
                    }
                }
                $partID =  md5(rand(0,1000)).time();
                $this->partIndex = 0;
                $this->partsInfo[$this->partIndex] = array($partID, 0);
            }
            
            else if ($mode[0] == 'c'){
                $this->position = 0;
                if ($fSize){
                    $this->loadPartsInfo();
                }
                else{
                    $partID =  md5(rand(0,1000)).time();
                    $this->partIndex = 0;
                    $this->partsInfo[$this->partIndex] = array($partID, 0);
                }
            }
            
            if (!$fileExisted) {
                // if file not existed, set file size to 0
                $this->fSize = 0;
                
            }
            $this->savePartsInfo();
            
            break;
        default:
            return false;
		}

		return true;
    }

    /**
     * Read from stream.
     *
     * @param int $count How many bytes of data from the current position should be returned.
     * @return string|bool If there are less than count bytes available, 
     * return as many as are available.If no more data is available, return 
     * either FALSE or an empty string.
     */
    public function stream_read($count) {
         if ($this->fileType == 'dir'){
             return "";
         }
        
        // can not return more than the file
        if ($count > $this->fSize-$this->position) {
            $count = $this->fSize-$this->position;
        }
        
        $ret = '';
        
        // read from cache and update position
        do {
            // load one part to cache
            $this->loadPartsCache();
            
            // get some data from cache
            $subString = substr( $this->cache, $this->position - $this->cacheHeadPos, $count);
            
            $ret .= $subString;
            
            // update position and count
            $count -= strlen($subString);
            $this->position += strlen($subString);
            
        } while ($count > 0);
        
        return $ret;
    }

    /**
     * Write to stream.
     * 
     * @param string $data Should be stored into the underlying stream.
     * @return int Should return the number of bytes that were successfully stored, 
     * or 0 if none could be stored.
     */
    public function stream_write($data) {
        if (!$this->writable){
            return 0;
        }

        // if the part is not full
        $dataSize = strlen($data);
        if($this->partsInfo[$this->partIndex][1] < $this->partSize) {
            // append to the cache
            $this->cache .= $data;
        
            $this->partsInfo[$this->partIndex][1] += $dataSize;
            $this->position += $dataSize;

        } else {
            // save cache
            $this->saveCache();
            
            // append to the cache
            $this->cache .= $data;
             
            $this->partsInfo[$this->partIndex][1] += $dataSize;
            $this->position += $dataSize;

            }
        return $dataSize;

    }

    /**
     * Close a resource.
     *
     */
    public function stream_close() {

        switch ( $this->mode ) {
        case 'r+':
		case 'rb+':
		case 'w+':
		case 'wb+':
		case 'x+':
		case 'xb+':
		case 'a+':
		case 'ab+':
		case 'c+':
		case 'w':
		case 'wb':
		case 'x':
		case 'xb':
		case 'a':
		case 'ab':
		case 'c':
            // save the caches
            if ($this->cache != '') {
                $this->saveCache();
                foreach($this->aioComp as $comp){
                    rados_aio_wait_for_complete($comp);
                }
                $this->savePartsInfo($this->oid);
            }
            $this->loadPartsInfo();
            
            //check file exist
            if ($this->checkObjectExist($this->oid)){
                $size = 0;
                // update partsInfo
                if (isset($this->oid)) {
                    $this->savePartsInfo($this->oid);
                }
            
                // update file size
                // calculate file size from $partsInfo
                foreach ($this->partsInfo as $record) {
                    $size += $record[1];
                }
                // save file size as an object attribute
                rados_setxattr( LocalCephStream::getRadosCtx(),
                $this->oid,
                'fileSize',
                strval($size));
                // update parent folder info
                $this->updateFSMeta('create', 'file');
            
            }

            break;
        default:
            break;
        }
    }

    /**
     * Retrieve information about a file resource.
     *
     * @param int $count 
     * @return array|bool In case of error, stat() returns FALSE.
     */
    public function stream_stat() {

        return $this->url_stat('', 0);
    }

    /**
     * Retrieve information about a file.
     * 
     * @param string $path The file path to stat.
     * @param int $flags Holds additional flags set by the streams API.
     * @return array Should return as many elements as stat() does. Unknown or unavailable values should be set to a rational value (usually 0).
     */
	public function url_stat($path, $flags) {
	    // expect path format: /var/www/owncloud/data/admin/
	    // expect flags format: 2
	    // expect path format: /var/www/owncloud/data/admin/files/b.pdf
	    // expect flags format: 0/2
		
        
        // check file type and get oid
        
        $oidFile = $this->oidFiletype($path);
        
        if (!$oidFile) {
            return false;
            
        } else {
            
            $this->oid  = $oidFile[0];            
            $fType = $oidFile[1];
            
            if ($fType == 'file') {
                $mode = self::MODE_FILE | 0777;
                $nlink = 1;
            } else {
                $mode = self::MODE_DIR | 0777;
                $nlink = 0;
            }
        }
        // get rados_stat
        $ioctx = LocalCephStream::getRadosCtx();
        $stat = rados_stat($ioctx, $this->oid);
        // get file size from object attribute
        
        $xattrResult = rados_getxattrs(LocalCephStream::getRadosCtx(),
                $this->oid);
        if (isset($xattrResult['fileSize'])){
            $fSize = intval($xattrResult['fileSize']);
        }
        else{
            $fSize = 0;
        }
        
        
        $data = array(
            'dev' => 0,
            'ino' => 0,
            'mode' => $mode,
            'nlink' => $nlink,
            'uid' => 0,
            'gid' => 0,
            'rdev' => $fType,
            'size' => $fSize,
            'atime' => time(),
            'mtime' => $stat["pmtime"],
            'ctime' => $stat["pmtime"],
            'blksize' => -1,
            'blocks' => -1,
			);
        return array_values($data) + $data;
	}

    /**
     * Tests for end-of-file on a file pointer.
     * 
     * @return bool Should return TRUE if the read/write position is at the end of the
     * stream and if no more data is available to be read, or FALSE otherwise.
     */
    public function stream_eof() {
        
        return $this->position >= $this->fSize;
    }

    /**
     * Seeks to specific location in a stream.
     * 
     * @param int $offset The stream offset to seek to.
     * @param int $whence Possible values:
     * <ul>
     *      <li>SEEK_SET - Set position equal to offset bytes.</li>
     *      <li>SEEK_CUR - Set position to current location plus offset.</li>
     *      <li>SEEK_END - Set position to end-of-file plus offset.</li>
     * </ul>
     * @return bool Return TRUE if the position was updated, FALSE otherwise.
     */
    public function stream_seek($offset, $whence = SEEK_SET) {
        
        switch ($whence) {
        case SEEK_SET:
            // if ($offset < $this->position && $offset >= 0) {
            if ($offset <= $this->fSize) {
                $this->position = $offset;
                
                // update cache according new position
                $this->loadPartsInfo();
                $this->loadPartsCache();
                
                return true;
            } else {
                return false;
            }
            break;

        case SEEK_CUR:
            if ($this->position + $offset <= $this->fSize) {
                $this->position += $offset;
                
                // update cache according new position
                $this->loadPartsInfo();
                $this->loadPartsCache();
                
                return true;
            } else {
                return false;
            }
            break;

        case SEEK_END:
            // if ($this->position + $offset >= 0) {
            if ($tihs->fSize + $offset <= $this->fSize) {
                $$this->position = $this->position + $offset;
                
                // update cache according new position
                $this->loadPartsInfo();
                $this->loadPartsCache();

                return true;
            } else {
                return false;
            }
            break;

        default:
            return false;
        }
    }

    /**
     * Retrieve the current position of a stream.
     * 
     * @return int Should return the current position of the stream.
     */
    public function stream_tell() {
        return $this->position;
    }
    
    
    /**
     * Update the file current lock status by consul rest api.
     *
     * @param int $httpCode Check the file is exist(200) or not(404).
     * @param array $data The pid,hostname,locktype infomation in a array.
     * @param string $sessionID The session ID from consul keep the owncloud service session.
     * @return bool Returns TRUE on success or FALSE on failure.
     */

    private function updateFileLockStatus($httpCode,$data,$sessionID){
        
        while($this->retryCount<$this->maxReTrycount){
        
            if ($httpCode == 200){
                $lockResult = $this->consul -> updateKeyValue($this->fileId, $data,$sessionID);
            }
            
            else{
                $lockResult = $this->consul -> createKeyValue($this->fileId, $data,$sessionID);
            }
            
            
            if ($lockResult['httpCode'] == 200){
                $this->retryCount = 0;
                return true;
            }
            
            elseif($lockResult['httpCode'] == 500){
                
                if ($this->renewLocalhostSessionID() === false){
                    return false;
                }

                $sessionID = $this->systemConfig->getValue("localhostsession", false);
                $this->retryCount+=1;
                sleep(1);
                
                return $this->updateFileLockStatus($httpCode, $data, $sessionID);
            }
        }
        return false;
        }

    
    
    /**
     * Advisory file locking
     * 
     * @param int $operation is one of the following:
     * <ul>
     *      <li>LOCK_SH(1) to acquire a shared lock (reader).</li>
     *      <li>LOCK_EX(2) to acquire an exclusive lock (writer).</li>
     *      <li>LOCK_UN(3) to release a lock (shared or exclusive).</li>
     *      <li>LOCK_NB(4) as a bitmask to one of the above operations
     *      if you don't want flock() to block while locking.</li>
     *      <li>LOCK_SH|LOCK_NB(5).</li>
     *      <li>LOCK_EX|LOCK_NB(6).</li>
     * </ul>
     * @return bool Returns TRUE on success or FALSE on failure.
     */
    public function stream_lock($operation)
    {       

        // Initial Consul instance.

        if(!isset($this->consul)){
            $this->consul = new \OCP\ConsulUtil();
            $this->consul -> init();
        }
 
        // Get localhost consul session.
        if(!isset($this->systemConfig)){
            $this->systemConfig = \OC::$server->getSystemConfig();

            // If localhost session not saved in config file or session not vaild,
            // will to create or renew localhost session.
            if (!$this->systemConfig->getValue("localhostsession", false)) {

                $renewLocalhostSessionIDResult = $this->renewLocalhostSessionID();

                if (!$renewLocalhostSessionIDResult ||
                        $renewLocalhostSessionIDResult['httpCode'] != 200){

                    return false;
                }
                else{
                    $this->sessionID = $renewLocalhostSessionIDResult['result'][0]['ID'];
                }
            }
            else{
                $this->sessionID = $this->systemConfig->getValue("localhostsession", false);
            }

            if (!$this->sessionID){
                return false;
            }
        }
        $this->hostName == gethostname();

        if (substr($this->oid,0) != '/'){
            $this->fileId = '/'.$this->oid;
        }
        else{
            $this->fileId = $this->oid;
        }

        $this->fileId = md5($this->oid);
        $getfileLockStatus = $this->consul -> getKeyValue($this->fileId);

        $getFileHttpCode = $getfileLockStatus['httpCode'];
        if ($getFileHttpCode == 404){
            $currentLock =  LOCK_UN;
            $request = \OC::$server->getRequest();
            $reqId = $request->getId();
            $this->getMyPid = $reqId;

            $this->hostName = gethostname(); 
  
        }elseif($getFileHttpCode == 200){
            $value = json_decode(base64_decode($getfileLockStatus['result'][0]['Value']),true);
            $currentLock =  $value['locktype'];
            $this->getMyPid = $value['pid'];
            $this->hostName = $value['hostname'];
        }
        else{
            return false;
        }
        


        switch ($operation){
        
        case LOCK_SH:
        case LOCK_SH|LOCK_NB:
            //nobody lock file,will create shared lock.
            if ($currentLock == LOCK_UN  || $currentLock == 0){

                $data = array(
                        'hostname' => $this->hostName,
                        'pid' => $this->getMyPid,
                        'locktype' => LOCK_SH,
                        'file' => $this->oid
                );
                return $this->updateFileLockStatus($getFileHttpCode, $data, $this->sessionID);

                
            }
            //if current lock is shared lock, always return true.
            elseif ($currentLock == LOCK_SH|LOCK_NB || $currentLock == LOCK_SH){
                return true;
            }
            else{
                return false;
            }
            break;
        case LOCK_EX:
            $request = \OC::$server->getRequest();
            $reqId = $request->getId();
            //nobody lock file,will create exclusive lock.
            if ($currentLock == LOCK_UN  || $currentLock == 0){

                $data = array(
                        'hostname' => $this->hostName,
                        'pid' => $this->getMyPid,
                        'locktype' => LOCK_EX,
                        'file' => $this->oid
                );
                return $this->updateFileLockStatus($getFileHttpCode, $data, $this->sessionID);
                
            }
            
            //if current lock is unlock, check whether is same request
            //if is, will return true.
            elseif($this->getMyPid == $reqId && $this->hostName == gethostname()){
                return true;
            }
            else{
                return false;
            }
            break;
        case LOCK_EX|LOCK_NB:
            $request = \OC::$server->getRequest();
            $reqId = $request->getId();
            //nobody lock file,will create exclusive lock.
            if ($currentLock == LOCK_UN  || $currentLock == 0){

                $data = array(
                        'hostname' => $this->hostName,
                        'pid' => $this->getMyPid,
                        'locktype' => LOCK_EX|LOCK_NB,
                        'file' => $this->oid
                );
                return $this->updateFileLockStatus($getFileHttpCode, $data, $this->sessionID);
                
            }
            //if current lock is unlock, check whether is same request
            //if is, will return true.
            elseif($this->getMyPid == $reqId && $this->hostName == gethostname()){
                return true;
            }
            else{
                return false;
            }
            break;
        case LOCK_UN:
            $request = \OC::$server->getRequest();
            $reqId = $request->getId();
            if ( $this->getMyPid == $reqId && $this->hostName == gethostname()){

                $this->consul -> deleteKeyValue($this->fileId);
            }
            return true;
        case LOCK_NB:
        default:
            return false;
        }
        
    }

    
    /**
     * Change stream options.
     *
     * @param string $path The file path or URL to set metadata.
     * @param int $option One of:
     * <ul>
     *      <li>STREAM_META_TOUCH (The method was called in response to touch()).</li>
     * </ul>
     * @param array|string|integer $value If option is:
     * <ul>
     *      <li>STREAM_META_TOUCH: Array consisting of two arguments of the touch() function.</li>
     * </ul>
     * @return bool Returns TRUE on success or FALSE on failure. If option is 
     * not implemented, FALSE should be returned.
     */
    public function stream_metadata($path, $option, $value) {
        //except path format: localceph:///var/www/owncloud/data/admin/files/Photos/San Francisco.jpg
        //except option format: 1
        //except value format: []/[1443147812,1443147812]
        switch ($option) {
        case STREAM_META_TOUCH:
            // touch an empty file       
            // parse path to get the oid
            
            
            if (substr($path,-1) == '/'){
                $this->fileType = 'dir';
            }
            else{
                $this->fileType = 'file';
            }
            
            $info = self::parsePath($path);
            
            $this->oid = self::pathToOid($info['fullPath'], $this->fileType);
            
            if (self::checkObjectExist($this->oid)){
                $this->loadPartsInfo();
                
                return true;
            }
            
            if ($this->fileType == 'dir'){
                //TODO　Ｅ＿Warning
                break;
            }
            
            
            // update partsInfo
            if (isset($this->oid)) {
                $this->savePartsInfo();
            }
            
            // save file size as an object attribute
            rados_setxattr( LocalCephStream::getRadosCtx(),
                            $this->oid,
                            'fileSize',
                            '0');
            
            // update parent folder info
            $this->updateFSMeta('create', 'file');
            return true;

        default:
            break;
        }
        
        return false;
    }

    /**
     * Change stream options.
     * 
     * @todo Not implement this function.
     *  
     * @return bool Returns TRUE on success or FALSE on failure. If option is 
     * not implemented, FALSE should be returned.
     */
    public function stream_set_option ($option ,$arg1 ,$arg2) {
        
        return false;
    }

    /**
     * Delete a file/folder.
     * 
     * @param string $path The file URL which should be deleted.
     * @return bool Returns TRUE on success or FALSE on failure.
     */
    public function unlink($path) {
        //except path format: localceph:///var/www/owncloud/data/admin/files/Photos/San Francisco.jpg
        //except path format: localceph:///var/www/owncloud/data/admin/files/1

        $ioctx = LocalCephStream::getRadosCtx();
        
        // parsePath to get oid and load parts info
        $info = self::parsePath($path);
        $this->oid = self::pathToOid($info['fullPath'], 'file');
        $this->loadPartsInfo();
        
        // remove file parts objects
        if ($this->partsInfo != Null){
	        foreach ($this->partsInfo as $i) {
	            rados_remove($ioctx, $i[0]);
	        }
        }
        $fSize = intval(rados_getxattr(LocalCephStream::getRadosCtx(), $this->oid,'fileSize',24 ));
        // remove file object
        rados_remove($ioctx, $this->oid);
        
        // update metadata in parent folder object
        $this->updateFSMeta('delete', 'file');
        return true;
    }
    
    /**
     * Combine several files into one file
     * 
     * @param string $targetPath The part target file path.
     * @param array $files The files of chunking file path.
     */
    public static function assemble_files($targetPath, $files) {
        //except $targetPath format: localceph:///var/www/owncloud/data/admin/files/sm8-setup.exe.ocTransferId3590259894.part
        //except $files format: 
        //[
        // localceph:///var/www/owncloud/data/admin/cache/sm8-setup.exe-chunking-3590259894-1,
        // localceph:///var/www/owncloud/data/admin/cache/sm8-setup.exe-chunking-3590259894-0
        //]                         
        
        // create target file metadata object
        $tFileMeta = array();
    
        // read files metadata object from $files array
        foreach($files as $f) {
            // add source file metadata object info to target file metadata object in order
            $info = self::parsePath($f);
            $sfoid = self::pathToOid($info['fullPath'], 'file');
    
            $fileMeta = self::readObject($sfoid);
            $tFileMeta = array_merge($tFileMeta, json_decode($fileMeta, true));
    
            // remove source file metadata object
            rados_remove(LocalCephStream::getRadosCtx(), $sfoid);
        }

        // update file info of target file
        $info = self::parsePath($targetPath);
        $tfoid = self::pathToOid($info['fullPath'], 'file');
        rados_write_full(LocalCephStream::getRadosCtx(),
                         $tfoid, json_encode($tFileMeta));
        

    }
}

