<?php
namespace OC\Connector\Sabre;

class WebDavAuth extends Auth {
    const DAV_AUTHENTICATED = 'AUTHENTICATED_TO_DAV_BACKEND';

    protected function validateUserPass($username, $password) {
        if (\OC_User::isLoggedIn() && $this->isDavAuthenticated(\OC_User::getUser())) {
            \OC_Util::setupFS(\OC_User::getUser());
            \OC::$server->getSession()->close();
            return true;
        } 
        else {
            if(!\OC::$server->getUserManager()->get($username)->isEnabled()) {
                return false;
            }

            \OC_Util::setUpFS(); //login hooks may need early access to the filesystem

            $authMethod = \OC::$server->getAppManager()->isInstalled("singlesignon") ? "\OCA\SingleSignOn\Util::webDavLogin" : "\OC_User::login";

            if(call_user_func($authMethod, $username, $password)) {
                $ocUser = \OC_User::getUser();
                \OC_Util::setUpFS($ocUser);
                \OC::$server->getSession()->set(self::DAV_AUTHENTICATED, $ocUser);
                \OC::$server->getSession()->close();
                return true;
            }
            else {
                \OC::$server->getSession()->close();
                return false;
            }
        }
    }
}
