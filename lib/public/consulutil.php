<?php
/**
 * @author Duncan Chiang <duncan.c@inwinstack.com>
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
namespace OCP;
/*========SESSION HTTP ENDPOINT========*/
define("SESSIONLIST","/v1/session/list");
define("SESSIONLISTBYNODE","/v1/session/node");
define("SESSIONCREATE","/v1/session/create");
define("SESSIONDELETE","/v1/session/destroy");

/*========KEY/VALUE HTTP ENDPOINT========*/
define("KV","/v1/kv");

class ConsulUtil {

    private static $consulHostUrl;
    
    
    
    /**
     * Get initial curl instance. 
     *
     * @return curl instance.
     */
    private function getCurlInitResource(){
        $curlResource = curl_init();
        curl_setopt ($curlResource, CURLOPT_RETURNTRANSFER, true);
        return $curlResource;
    }
    
    
    /**
     * Excute curl instance.
     *
     * @param instance $curlResource The seted curl instance.
     *
     * @return array Returns array.
     */
    private function exec($curlResource){
        $resultArray = array();
        $result = json_decode(curl_exec($curlResource),true);
        
        $resultArray['result'] = $result;
        $resultArray['httpCode'] = curl_getinfo($curlResource)['http_code'];
        
        $this->closeCurlResource($curlResource);
        return $resultArray;
    }
    
    /**
     * Close curl instance.
     *
     * @param instance $curlResource The seted curl instance.
     *
     */
    private function closeCurlResource($curlResource){
        curl_close($curlResource);
        
    }

    /**
     * Inital consul host url.
     *
     * @param string $hostName Specified hostname to call consul http api.
     * @param integer $port Specified port to call consul http api
     *
     */
    public static function init($hostName='localhost',$port=8500) {
        self::$consulHostUrl = 'http://'.$hostName.':'.$port;
    }

    
    /**
     * Get service session array.
     *
     * @return array.
     */
    public function getSessionList(){
        //except return ['result':[['ID':'xxx-xxx-xxx','Name':''.......]],
        //              'httpCode':200]
        $curl = $this->getCurlInitResource();
        
        curl_setopt_array($curl, array(
            CURLOPT_URL =>self::$consulHostUrl.SESSIONLIST
        
        ));
        
        return $this->exec($curl);
        
    }
    
    /**
     * Get service session array by hostname.
     *
     * @return array.
     */
    public function getSessionListByNode($nodeName){
        //except return ['result':[['ID':'xxx-xxx-xxx','Name':''.......]],
        //              'httpCode':200]
        $curl = $this->getCurlInitResource();
        
        curl_setopt_array($curl, array(
            CURLOPT_URL => self::$consulHostUrl.SESSIONLISTBYNODE.'/'.$nodeName
        
        ));
        
        return $this->exec($curl);
        
    }
    
    /**
     * Create service session.
     *
     * @param array $data create session related pameters array.
     * @param bool $allowDuplicated can allow create same service name session.
     * 
     * @return array|bool Returns array on success or FALSE on failure.
     */
    public function createServiceSession($data=Null,$allowDuplicated=false){
        //except return ['result':['ID':'xxx-xxx-xxx'],
        //              'httpCode':200]
        if (!isset($data)){
            $data = array(
                    'Name' => 'OwncloudService',
                    'LockDelay' => '15s',
                    "Behavior" => "delete",
                    "Checks" => ["serfHealth","checkOwncloud"]
            
            );
        }
        
        if (!$allowDuplicated){
            $result = $this->getSessionListByNode(gethostname());
            if($result['httpCode'] != 200){
                return false;
            }
            
            foreach ($result['result'] as $sessionInfoArray){
                if ($sessionInfoArray['Name'] == $data['Name']){
                    return false;
                }
                
            }
            
        }
        
        
        $curl = $this->getCurlInitResource();
        
        curl_setopt_array($curl, array(
            CURLOPT_URL => self::$consulHostUrl.SESSIONCREATE,
            CURLOPT_CUSTOMREQUEST => "PUT",
            CURLOPT_POSTFIELDS => json_encode($data)
        ));
        
        return $this->exec($curl);
        
    }
    
    
    /**
     * Delete service session.
     *
     * @param string $sessionId specified session id.
     *
     * @return array.
     */
    public function deleteSession($sessionId){
        //Notify the result will always return true.
        //except return ['result':true,
        //              'httpCode':200]
        $curl = $this->getCurlInitResource();
        
        curl_setopt_array($curl, array(
            CURLOPT_URL => self::$consulHostUrl.SESSIONDELETE."/$sessionId",
            CURLOPT_PUT => true
        ));
        

        return $this->exec($curl);
    } 
    
    /**
     * Get consul key and values array. 
     *
     * @param string $key specified key.
     *
     * @return array.
     */
    public function getKeyValue($key=Null){
        //Notify Value is a Base64-encoded blob of data. 
        //       Note that values cannot be larger than 512kB.
        
        //except return ['result':[['Key':'xxxxx','Value':'xxxx']],
        //              'httpCode':200]
        
        $curl = $this->getCurlInitResource();

        if (isset($key)){
            curl_setopt($curl, CURLOPT_URL, self::$consulHostUrl.KV."/$key"."?consistent");
        }else{
            curl_setopt($curl, CURLOPT_URL, self::$consulHostUrl.KV.'/?recurse');
        }
        
        return $this->exec($curl);
    }
    
    /**
     * Create consul key and values.
     *
     * @param string $key specified key.
     * @param array $data to save related info into the key.
     * @param string $acquireSession the session is use to lock the key.
     *
     * @return array.
     */
    public function createKeyValue($key,$data,$acquireSession=Null){
        //$key = 'test';
        //$data = array('lock'=> 3,
        //              'pid' => 999,
        //              'hostname' => 'owncloud-latest'
        //);

        //Notify the result will always return true.
        //except return ['result':true,
        //              'httpCode':200]
        $curl = $this->getCurlInitResource();
        
        if (isset($acquireSession)){
            curl_setopt($curl, CURLOPT_URL, self::$consulHostUrl.KV."/$key"."?acquire=$acquireSession");
        }else{
            curl_setopt($curl, CURLOPT_URL, self::$consulHostUrl.KV."/$key");
        }
        
        curl_setopt_array($curl, array(
            CURLOPT_CUSTOMREQUEST => "PUT",
            CURLOPT_POSTFIELDS => json_encode($data)
        ));
        
        
        return $this->exec($curl);
    }
    
    /**
     * Release the consul key by session id.
     *
     * @param string $key specified key.
     * @param string $releaseSession the session is use to unlock the key.
     *
     * @return array.
     */
    public function releaseKeyValue($key,$releaseSession){
        //Notify the result will always return true.
        //except return ['result':true,
        //              'httpCode':200]
        
        $curl = $this->getCurlInitResource();
        curl_setopt($curl, CURLOPT_URL, self::$consulHostUrl.KV."/$key"."?release=$releaseSession");
        curl_setopt_array($curl, array(
            CURLOPT_CUSTOMREQUEST => "PUT",
        ));
        return $this->exec($curl);
    }
    
    
    /**
     * Update consul key and values.
     *
     * @param string $key specified key.
     * @param array $data to save related info into the key.
     * @param string $acquireSession the session is use to lock the key.
     *
     * @return array.
     */
    public function updateKeyValue($key,$data,$acquireSession=Null){
        //$key = 'test';
        //$data = array('lock'=> 1,
        //              'pid' => 888,
        //              'hostname' => 'owncloud-latest'
        //);
        
        //Notify the result will always return true.
        //except return ['result':true,
        //              'httpCode':200]
        $curl = $this->getCurlInitResource();
        if (isset($acquireSession)){
            curl_setopt($curl, CURLOPT_URL, self::$consulHostUrl.KV."/$key"."?release=$acquireSession");
            curl_setopt_array($curl, array(
                CURLOPT_CUSTOMREQUEST => "PUT",
            ));
            $releaseResult = $this->exec($curl);
            
        }
        return $this->createKeyValue($key, $data,$acquireSession);
    }
    
    /**
     * Delete consul key.
     *
     * @param string $key specified key.
     *
     * @return array.
     */
    public function deleteKeyValue($key){
        //Notify the result will always return true.
        //except return ['result':true,
        //              'httpCode':200]
        $curl = $this->getCurlInitResource();
        
        curl_setopt_array($curl, array(
            CURLOPT_URL => self::$consulHostUrl.KV."/$key",
            CURLOPT_CUSTOMREQUEST => "DELETE",
        ));

        return $this->exec($curl);
    }
    
    
    public function __destruct(){

    }

}
