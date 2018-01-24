<?php
if(!empty($_POST["account"]) || !empty($_POST["password"])) {
    require_once "config/config.php";
    $redirectHost = $_SERVER['SERVER_NAME'];
    $userid = $_POST["account"];
    $password = $_POST["password"];
    $ip = $_SERVER["REMOTE_ADDR"];

    $functionExists = function_exists("radius_auth_open");
    if (!$functionExists){
        $msg = '主機尚未安裝radius套件';
    }
    else{
        $res = radius_auth_open();

        if (!array_key_exists('radius_server', $CONFIG) ||
                !array_key_exists('radius_port', $CONFIG) ||
                !array_key_exists('radius_shared_secret', $CONFIG)||
                !array_key_exists('hash_key', $CONFIG)){
                    $msg = "儲存雲尚未設置TANet主機相關參數";
        }else{
            require_once 'lib/base.php';
            $agent = \OCA\Activity_Logging\UserHooks::checkAgent();

            $radserver = $CONFIG['radius_server'];
            $radport = $CONFIG['radius_port'];
            $sharedsecret = $CONFIG['radius_shared_secret'];

            radius_add_server($res, $radserver, $radport, $sharedsecret, 5, 2);
            radius_create_request($res, RADIUS_ACCESS_REQUEST);
            radius_put_string($res, RADIUS_USER_NAME, $userid);
            radius_put_string($res, RADIUS_USER_PASSWORD, $password);

            $req = radius_send_request($res);
            switch ($req) {
                case RADIUS_ACCESS_ACCEPT:
                    
                    $ch = curl_init();
                    $url = "https://". $redirectHost ."/ocs/v1.php/apps/tanet_auth/checkNeedRedirect?format=json&userid=".$userid;
                    curl_setopt($ch, CURLOPT_URL, $url);
                    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
                    curl_setopt($ch, CURLOPT_SSL_VERIFYHOST, 0);
                    curl_setopt($ch, CURLOPT_SSL_VERIFYPEER, 0);
                    $result = curl_exec($ch);
                    $result = json_decode($result,true);
                    curl_close($ch);
                    
                    if ($result['ocs']['meta']['statuscode'] == 100){
                        if ($result['ocs']['data']['result'] === true){
                            $redirectHost= $result['ocs']['data']['host'];
                        }
                        else if ($result['ocs']['data']['host'] === false){
                            \OCP\Util::writeLog("TANet_Auth", "The userID($userid) has not access permission fom $agent.", \OCP\Util::INFO);
                            $msg = "您沒有權限使用儲存應用服務!";
                            break;
                        }
                    }
                    else{
                        \OCP\Util::writeLog("TANet_Auth", "The storage api return not normal status when the userID($userid) login from $agent.", \OCP\Util::INFO);
                        $msg = "儲存雲驗證失敗";
                        break;
                    }
                    
                    $accountInfoArray = json_encode(['userid' => strtolower($userid),
                                          'time' => time(),
                                          'password' => $password,
                                          'ip' => $ip
                            
                    ]);

                    $hash = hash('SHA384', $CONFIG['hash_key'], true);
                    $aesKey = substr($hash, 0, 32);
                    $aesIv = substr($hash, 32, 16);
                    
                    $accountInfoArray = mcrypt_encrypt(MCRYPT_RIJNDAEL_128, $aesKey, $accountInfoArray, MCRYPT_MODE_CBC, $aesIv);
                    $encryptAccount = base64_encode($accountInfoArray);
                    $params["tanet"] = true;
                    $params["encrypt"] = $encryptAccount;
                    $params["check"] = md5($encryptAccount . $CONFIG['crc_key']);
                    $queryStr = "?" . http_build_query($params);

                    $redirectUrl = 'https://' . $redirectHost . '/index.php' . $queryStr;
                    \OCP\Util::writeLog("TANet_Auth", "The userID($userid) auth successed from $agent. Redirecting......", \OCP\Util::INFO);
                    header('location:' . $redirectUrl);
                    exit();
                case RADIUS_ACCESS_REJECT:
                    \OCP\Util::writeLog("TANet_Auth", "The userID($userid) auth failed from $agent.", \OCP\Util::INFO);
                    $msg = "帳號不存在或密碼錯誤";
                    break;
                default:
                    \OCP\Util::writeLog("TANet_Auth", "The userID($userid) waiting tanet auth server response timeout from $agent.", \OCP\Util::INFO);
                    $msg = "TANet 主機回傳錯誤 : ". radius_strerror($res);
                    break;
            }

        }
    }
}
?>
<html>
<head>
    <meta charset="UTF-8">
    <title>
        雲端儲存服務        </title>
    <link rel="shortcut icon" type="image/png" href="core/img/favicon.png">
    <link rel="apple-touch-icon-precomposed" href="core/img/favicon-touch.png">
    <link rel="stylesheet" href="core/css/styles.css" media="screen">
    <link rel="stylesheet" href="core/css/header.css" media="screen">
    <link rel="stylesheet" href="core/css/mobile.css" media="screen">
    <link rel="stylesheet" href="core/css/icons.css" media="screen">
    <link rel="stylesheet" href="core/css/fonts.css" media="screen">
    
    <link rel="stylesheet" href="core/css/apps.css" media="screen">
    <link rel="stylesheet" href="core/css/fixes.css" media="screen">
    <link rel="stylesheet" href="core/css/multiselect.css" media="screen">
    <link rel="stylesheet" href="core/vendor/jquery-ui/themes/base/jquery-ui.css" media="screen">
    <link rel="stylesheet" href="core/css/jquery-ui-fixes.css" media="screen">
    <link rel="stylesheet" href="core/css/tooltip.css" media="screen">
    <link rel="stylesheet" href="core/css/share.css" media="screen">
    <link rel="stylesheet" href="core/css/jquery.ocdialog.css" media="screen">
    
    <link rel="stylesheet" href="themes/MOE/core/css/styles.css" media="screen">
    <link rel="stylesheet" href="themes/MOE/core/css/header.css" media="screen">
    <link rel="stylesheet" href="themes/MOE/core/css/icons.css" media="screen">
    <link rel="stylesheet" href="themes/MOE/core/css/apps.css" media="screen">
    <!--
    <link rel="stylesheet" href="login/styles/vendor/bootstrap.css" />
    <link rel="stylesheet" href="login/styles/vendor/font-awesome.css" />
    -->
</head>
<body id="body-login">
    <div class="wrapper">
        <div class="v-align">
            <header role="banner">
                <div id="header">
                    <div class="logo svg">
                        <h1 class="hidden-visually">
                            雲端儲存服務                                </h1>
                    </div>
                    <div id="logo-claim" style="display:none;"></div>
                </div>
            </header>
            <div class="center login-text">
                使用TANet Roaming帳號進行登入
            </div>                                
            <form method="post" name="login">
                <fieldset>
                    <div id="message" class="hidden">
                        <img class="float-spinner" alt="" src="/core/img/loading-dark.gif">
                        <span id="messageText"></span>
                        <div style="clear: both;"></div>
                    </div>
                    <p class="grouptop">
                        <input type="text" name="account" id="user" placeholder="使用者名稱" value="" autofocus="" autocomplete="on" autocapitalize="off" autocorrect="off" required="">
                        <label for="user" class="infield">使用者名稱</label>
                    </p>

                    <p class="groupbottom">
                        <input type="password" name="password" id="password" value="" placeholder="密碼" autocomplete="on" autocapitalize="off" autocorrect="off" required="">
                        <label for="password" class="infield">密碼</label>
                        <!--<input type="submit" id="submit" class="login primary icon-confirm svg" title="登入" value="">-->
                    </p>
                    <p class="groupbottom center">
                        <button type="submit" id="submit" class="login-local primary" title="登入">登入</button>
                    </p>
                    <div class="remember-login-container">
                        <input type="checkbox" name="remember_login" value="1" id="remember_login" class="checkbox checkbox--white">
                        <label for="remember_login">remember</label>
                    </div>
                    <input type="hidden" name="timezone-offset" id="timezone-offset" value="8">
                    <input type="hidden" name="timezone" id="timezone" value="Asia/Shanghai">
                    <input type="hidden" name="requesttoken" value="aUsFDmwGEgcCbkMqFScfJC05JUgIfitlCkEGKDNg:02ITZ5TJC89dBkhrCts0b2q1fxLZY0">
                </fieldset>
            </form>
            <div class="push">
		<?php if(isset($msg)) echo "<p style='color:red;'>$msg</p>"; ?>
	    </div>

            <div id="other-login">
                <span class="login-text">------------ 學習資源 ------------</span>
                <input type="button" class="learning" value="操作學習地圖" onClick="window.open('http://u.camdemy.com/course/1618/intro')">
                <input type="button" class="learning" value="下載服務" onClick="window.open('./system.php')">
            </div>
        </div>
    </div>
    <footer role="contentinfo">
        <div class="footer-img"></div>
        <div style="display: inline-block">
            Copyright © Ministry of Education. All rigths reserved.
        </div>
    </footer>
<!--
<div class="container">
        <div class="row">
            <div class="col-sm-4 col-sm-offset-4">
                <form name="login" class="panel panel-primary" method="POST">
                    <input name="return_url" type="hidden" value="<?php //echo isset($_GET["returnUrl"]) ? $_GET["returnUrl"] : "";?>">
                    
                    <div class="panel-heading">
                        <header class="panel-title">Sign In</header>
                    </div>
                    
                    <div class="panel-body">
                        <div class="form-group">
                            <label for="account">Account</label>
                            <input id="account" name="account" type="text" class="form-control" required />
                        </div>
                        
                        <div class="form-group">
                            <label for="password">Password</label>
                            <input id="password" name="password" type="password" class="form-control" required />
                        </div>
                    </div>
                    
                    <div class="panel-footer">
                        <button class="btn btn-default btn-block" type="submit">LogIn</button>
                    </div>
                </form>
            </div>
        </div>
    </div>
-->
</body>
</html>
