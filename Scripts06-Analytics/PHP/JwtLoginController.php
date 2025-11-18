<?php

namespace App\Http\Controllers;

use App\Models\JwtUser;
use Illuminate\Http\Request;
use Illuminate\Support\Facades\Auth;
use Tymon\JWTAuth\Facades\JWTAuth;
use Tymon\JWTAuth\Exceptions\JWTException;
use Exception;
use Illuminate\Support\Facades\Session;
use Illuminate\Support\Facades\Cache;
use Rdr\SnowflakeJodo\SnowflakeJodo;
use \PDO;

class JwtLoginController extends Controller
{
    protected $conn;
    protected $dbsuffix; // Define a protected property for table prefix
    public function __construct()
    {
        $this->conn = SnowflakeJodo::connect();
        $this->dbsuffix = env('DB_SUFFIX', '');
    }

    /**
     * Helper method to handle logout and redirect to invalid login
     */
    private function logoutAndRedirect()
    {
        if (Auth::guard('jwt')->check()) {
            Auth::guard('jwt')->logout();
        }
        return redirect()->route('login.invalid');
    }

    public function login(Request $request)
    {
        $providerToken = $_COOKIE['HHAX_ENT_AccessToken'] ?? null; // Provider Token
        $payerToken = $_COOKIE['HHAX_AccessToken'] ?? null; // Payer Token
        $aggregatorToken = $_COOKIE['HHAX_AGG_AccessToken'] ?? null; // Aggregator Token
        $useIdpv6 = env('IDPV6', false);
        $payloadArraySession = Session::get('jwt_attributes_auth', []);
        
        // If neither token is present, return an error
        if ((empty($providerToken) && empty($payerToken) && empty($aggregatorToken)) || empty($payloadArraySession)) {
            dd('DEBUG: No tokens or session data', [
                'providerToken' => $providerToken,
                'payerToken' => $payerToken, 
                'aggregatorToken' => $aggregatorToken,
                'payloadArraySession' => $payloadArraySession
            ]);
            return $this->logoutAndRedirect();
        }
        $token = null;
        $userType = null;
        if ($payerToken) {
            $token = $payerToken;
            $userType = 'payer';
        } else if ($providerToken) {
            $token = $providerToken;
            $userType = 'provider';
        } else if ($aggregatorToken) {
            $token = $aggregatorToken;
            $userType = 'Aggregator';
        }
        try {
            // Manually decode the JWT without verifying the signature
            $payloadArray = $this->decodeJwtWithoutVerification($token);    
            if (empty($payloadArray)) {
                dd('DEBUG: Payload array is empty', ['token' => $token, 'userType' => $userType]);
                return $this->logoutAndRedirect();
            }
            if ($userType == 'payer') {
                if (env('IDPV6', false)) {
                    if (!isset($payloadArraySession['CanViewConflictDashboard']) || $payloadArraySession['CanViewConflictDashboard'] !== 'True') {
                        dd('DEBUG: Payer missing CanViewConflictDashboard or not True - PAYER SECTION IDPV6', ['payloadArraySession' => $payloadArraySession]);
                        return $this->logoutAndRedirect();
                    }
                    if ($payloadArraySession['sub'] != $payloadArray['guid']) {
                        dd('DEBUG: Payer sub != guid mismatch - PAYER SECTION IDPV6', ['payloadArraySession' => $payloadArraySession, 'payloadArray' => $payloadArray]);
                        return $this->logoutAndRedirect();
                    }
                } else {
                    if (!isset($payloadArraySession['hha.payer.canviewconflictmanagementdashboard']) || $payloadArraySession['hha.payer.canviewconflictmanagementdashboard'] != 'True') {
                        dd('DEBUG: Payer missing hha.payer.canviewconflictmanagementdashboard or not True - PAYER SECTION LEGACY', ['payloadArraySession' => $payloadArraySession]);
                        return $this->logoutAndRedirect();
                    }
                    // Additional legacy validation
                    if ($payloadArraySession['sub'] != $payloadArray['uid']) {
                        dd('DEBUG: Payer sub != uid mismatch - PAYER SECTION LEGACY', ['payloadArraySession' => $payloadArraySession, 'payloadArray' => $payloadArray]);
                        return $this->logoutAndRedirect();
                    }
                }
                if (!isset($payloadArray['uid'])) {
                    dd('DEBUG: Payer missing uid in payload - PAYER SECTION', ['payloadArray' => $payloadArray]);
                    return $this->logoutAndRedirect();
                }
                if (!isset($payloadArray['pid'])) {
                    dd('DEBUG: Payer missing pid in payload - PAYER SECTION', ['payloadArray' => $payloadArray]);
                    return $this->logoutAndRedirect();
                }
                if (!isset($payloadArray['gpid'])) {
                    dd('DEBUG: Payer missing gpid in payload - PAYER SECTION', ['payloadArray' => $payloadArray]);
                    return $this->logoutAndRedirect();
                }
                if ($payloadArraySession['vendorid'] != $payloadArray['pid'] || $payloadArraySession['gvid'] != $payloadArray['gpid']) {
                    dd('DEBUG: Payer vendorid/gvid mismatch - PAYER SECTION', ['payloadArraySession' => $payloadArraySession, 'payloadArray' => $payloadArray]);
                    return $this->logoutAndRedirect();
                }
            } else if ($userType == 'provider') {
                if (env('IDPV6', false)) {
                    if (!isset($payloadArraySession['CanViewConflictDashboard']) ||  (string)$payloadArraySession['CanViewConflictDashboard'] !== '1') {
                        dd('DEBUG: Provider missing CanViewConflictDashboard or not 1 - PROVIDER SECTION IDPV6', ['payloadArraySession' => $payloadArraySession]);
                        return $this->logoutAndRedirect();
                    }
                    if (strtolower($payloadArraySession['sub']) != strtolower($payloadArray['guid'])) {
                        dd('DEBUG: Provider sub != guid mismatch - PROVIDER SECTION IDPV6', ['payloadArraySession' => $payloadArraySession, 'payloadArray' => $payloadArray]);
                        return $this->logoutAndRedirect();
                    }
                } else {
                    if (!isset($payloadArraySession['hha.payer.canviewconflictmanagementdashboard']) || $payloadArraySession['hha.payer.canviewconflictmanagementdashboard'] != 'True') {
                        dd('DEBUG: Provider missing hha.payer.canviewconflictmanagementdashboard or not True - PROVIDER SECTION LEGACY', ['payloadArraySession' => $payloadArraySession]);
                        return $this->logoutAndRedirect();
                    }
                    // Additional legacy validation
                    if ($payloadArraySession['sub'] != $payloadArray['uid']) {
                        dd('DEBUG: Provider sub != uid mismatch - PROVIDER SECTION LEGACY', ['payloadArraySession' => $payloadArraySession, 'payloadArray' => $payloadArray]);
                        return $this->logoutAndRedirect();
                    }
                }
                if (!isset($payloadArray['uid'])) {
                    dd('DEBUG: Provider missing uid in payload - PROVIDER SECTION', ['payloadArray' => $payloadArray]);
                    return $this->logoutAndRedirect();
                }
                if (!isset($payloadArray['pid'])) {
                    dd('DEBUG: Provider missing pid in payload - PROVIDER SECTION', ['payloadArray' => $payloadArray]);
                    return $this->logoutAndRedirect();
                }
                if (!isset($payloadArray['gvid'])) {
                    dd('DEBUG: Provider missing gvid in payload - PROVIDER SECTION', ['payloadArray' => $payloadArray]);
                    return $this->logoutAndRedirect();
                }
                if (strtolower($payloadArraySession['vendorid']) != strtolower($payloadArray['pid']) || strtolower($payloadArraySession['gvid']) != strtolower($payloadArray['gvid'])) {
                    dd('DEBUG: Provider vendorid/gvid mismatch - PROVIDER SECTION', ['payloadArraySession' => $payloadArraySession, 'payloadArray' => $payloadArray]);
                    return $this->logoutAndRedirect();
                }
            } else if($userType == 'Aggregator'){
                if (!isset($payloadArraySession['sub'])) {
                    dd('DEBUG: Aggregator missing sub in session - AGGREGATOR SECTION', ['payloadArraySession' => $payloadArraySession]);
                    return $this->logoutAndRedirect();
                }
            }else {
                dd('DEBUG: Unknown user type - ELSE SECTION', ['payloadArray' => $payloadArray, 'payloadArraySession' => $payloadArraySession, 'userType' => $userType]);
                return $this->logoutAndRedirect();
            }

            if ($userType == 'payer') {
                $attributes = [
                    'Application_Payer_Id' => $payloadArray['pid'],
                    'Payer_Id' => $payloadArray['gpid'],
                    'user_type' => 'payer',
                    'id' => $payloadArray['uid'],
                    'uid' => $payloadArray['uid'],
                    'guid' => $payloadArray['guid'],
                ];
                $providerId = $attributes['Payer_Id'];
                $providerApplicationId = $attributes['Application_Payer_Id'];

                // Cache payer info for 15 minutes
                $cacheKey = "payer_info_{$providerId}";
                $check = Cache::remember($cacheKey, 900, function() use ($providerId) {
                    $sql = "SELECT \"Payer Name\", \"Payer State\" FROM ANALYTICS".$this->dbsuffix.".BI.DIMPAYER WHERE \"Payer Id\"  = '$providerId'";
                    $checkSql = $this->conn->prepare($sql);
                    return $checkSql->fetch(PDO::FETCH_ASSOC);
                });

                if ($check && isset($check['Payer Name'])) {
                    $attributes['full_name'] = $check['Payer Name'];
                    $attributes['name'] = $check['Payer Name'];
                    $attributes['state'] = $check['Payer State'];
                }

                //get payer user email with caching
                if(!empty($payloadArray['uid']) && !empty($payloadArray['guid'])){
                    $emailCacheKey = "user_email_{$payloadArray['uid']}_{$payloadArray['guid']}";
                    $check111 = Cache::remember($emailCacheKey, 900, function() use ($payloadArray) {
                        $sql11 = "SELECT d2.\"User Email Address\" AS EMAIL FROM ANALYTICS".$this->dbsuffix.".BI.DIMUSER AS d2 WHERE \"Application User Id\" = ".$payloadArray['uid']." AND d2.\"User Id\" = '".$payloadArray['guid']."' AND \"User Email Address\" IS NOT NULL AND \"User Email Address\" !=''";
                        $checkSql111 = $this->conn->prepare($sql11);
                        return $checkSql111->fetch(PDO::FETCH_ASSOC);
                    });
                    if ($check111 && isset($check111['EMAIL'])) {
                        $attributes['email'] = $check111['EMAIL'];
                    }else{
                        $attributes['email'] = '';
                    }
                }else{
                    $attributes['email'] = '';
                }
                //jwt_attributes_auth
                Session::put('jwt_attributes', $attributes);

                $user = new JwtUser($attributes);
                $user->user_type = 'payer';
                Auth::login($user);
                // Auth::guard('jwt')->login($user);
                return redirect()->route('dashboard');
            } 
            else if($userType == 'provider'){
                $attributes = [
                    'Application_Provider_Id' => $payloadArray['pid'],
                    'Provider_Id' => $payloadArray['gvid'],
                    'user_type' => 'provider',
                    'id' => $payloadArray['uid'],
                    'uid' => $payloadArray['uid'],
                    'guid' => $payloadArray['guid']
                ];
                $providerId = $attributes['Provider_Id'];
                $providerApplicationId = $attributes['Application_Provider_Id'];

                // Cache provider info for 15 minutes
                $providerCacheKey = "provider_info_{$providerId}";
                $check = Cache::remember($providerCacheKey, 900, function() use ($providerId) {
                    $sql = "SELECT \"Provider Name\", \"Address State\" FROM ANALYTICS".$this->dbsuffix.".BI.DIMPROVIDER WHERE \"Provider Id\"  = '$providerId'";
                    $checkSql = $this->conn->prepare($sql);
                    return $checkSql->fetch(PDO::FETCH_ASSOC);
                });

                if ($check && isset($check['Provider Name'])) {
                    $attributes['full_name'] = $check['Provider Name'];
                    $attributes['name'] = $check['Provider Name'];
                    $attributes['state'] = $check['Address State'];
                }
                // Cache provider email
                if(!empty($payloadArray['uid']) && !empty($payloadArray['guid'])){
                    $providerEmailCacheKey = "provider_email_{$payloadArray['uid']}_{$payloadArray['guid']}";
                    $check111 = Cache::remember($providerEmailCacheKey, 900, function() use ($payloadArray) {
                        $sql11 = "SELECT d2.\"User Email Address\" AS EMAIL FROM ANALYTICS".$this->dbsuffix.".BI.DIMUSER AS d2 WHERE \"Application User Id\" = ".$payloadArray['uid']." AND d2.\"User Id\" = '".$payloadArray['guid']."' AND \"User Email Address\" IS NOT NULL AND \"User Email Address\" !=''";
                        $checkSql111 = $this->conn->prepare($sql11);
                        return $checkSql111->fetch(PDO::FETCH_ASSOC);
                    });
                    if ($check111 && isset($check111['EMAIL'])) {
                        $attributes['email'] = $check111['EMAIL'];
                    }else{
                        $attributes['email'] = '';
                    }
                }else{
                    $attributes['email'] = '';
                }

                // Cache office IDs
                $officeCacheKey = "office_ids_{$payloadArray['guid']}";
                $officeIds = Cache::remember($officeCacheKey, 900, function() use ($payloadArray) {
                    $getoffices = "SELECT \"Office Id\" FROM ANALYTICS".$this->dbsuffix.".BI.DIMUSEROFFICES WHERE \"User Id\" = '".$payloadArray['guid']."' AND \"Vendor Type\" = 'Vendor'";
                    $checkSql = $this->conn->prepare($getoffices);
                    $fetchalloffices = $checkSql->fetchAll(PDO::FETCH_ASSOC);
                    $officeIdsArray = array_column($fetchalloffices, 'Office Id');
                    if (empty($officeIdsArray)) {
                        return "'-999'";
                    } else {
                        return "'" . implode("','", $officeIdsArray) . "'";
                    }
                });

                $attributes['officeIds'] = $officeIds;
                //jwt_attributes_auth
                Session::put('jwt_attributes', $attributes);

                $user = new JwtUser($attributes);
                $user->user_type = 'provider';
                Auth::login($user);
                //Auth::guard('jwt')->login($user);
                return redirect()->route('dashboard');
            }
            else if($userType == 'Aggregator'){
                $attributes = [
                    'user_type' => 'Aggregator',
                    'guid' => $payloadArray['guid'],
                    'sub' => $payloadArray['uid'],
                    'first_name' => $payloadArray['firstname'] ?? '',
                    'last_name' => $payloadArray['lastname'] ?? '',
                    'full_name' => ($payloadArray['firstname'] ?? '') . ' ' . ($payloadArray['lastname'] ?? ''),
                ];           
                //get Aggregator database name with caching
                if(!empty($payloadArray['guid'])){
                    $aggregatorCacheKey = "aggregator_info_{$payloadArray['guid']}";
                    $check111 = Cache::remember($aggregatorCacheKey, 900, function() use ($payloadArray) {
                        $sql11 = "SELECT \"Application User Id\" as \"uid\",\"Aggregator Database Name\" AS AGGREGATOR_DB_NAME, \"User Fullname\" AS \"full_name\", \"User Email Address\" AS \"email\" FROM ANALYTICS".$this->dbsuffix.".BI.DIMUSER AS d2 WHERE d2.\"User Id\" = '".$payloadArray['guid']."'";
                        $checkSql111 = $this->conn->prepare($sql11);
                        return $checkSql111->fetch(PDO::FETCH_ASSOC);
                    });
                    if ($check111 && isset($check111['AGGREGATOR_DB_NAME'])) {
                        $attributes['aggregator_db_name'] = $check111['AGGREGATOR_DB_NAME'];
                    }else{
                        $attributes['aggregator_db_name'] = '';
                    }
                    if ($check111 && isset($check111['full_name'])) {
                        $attributes['full_name'] = $check111['full_name'];
                    }else{
                        $attributes['full_name'] = '';
                    }
                    if ($check111 && isset($check111['email'])) {
                        $attributes['email'] = $check111['email'];
                    }else{
                        $attributes['email'] = '';
                    }
                    if ($check111 && isset($check111['uid'])) {
                        $attributes['id'] = $check111['uid'];
                        $attributes['uid'] = $check111['uid'];
                        $attributes['providerApplicationId'] = $check111['uid'];
                    }else{
                        $attributes['id'] = '';
                        $attributes['uid'] = '';
                        $attributes['providerapplicationid'] = '';
                    }
                    $attributes['providerId'] = $payloadArray['guid'];
                }            
                //get Aggregator global payer IDs with caching
                if(!empty($attributes['aggregator_db_name'])){
                    $globalPayerCacheKey = "global_payer_ids_{$attributes['aggregator_db_name']}";
                    $globalPayerIds = Cache::remember($globalPayerCacheKey, 1800, function() use ($attributes) { // Cache for 30 minutes
                        $dbName = $attributes['aggregator_db_name'];
                        if (!str_starts_with($dbName, 'agg')) {
                            $dbName = 'agg' . $dbName;
                        }
                        $getGlobalPayerIds = "SELECT \"Global Payer ID\" FROM ".$dbName.".public.\"multipayer_payer\"";
                        $checkSql = $this->conn->prepare($getGlobalPayerIds);
                        $fetchallGlobalPayerIds = $checkSql->fetchAll(PDO::FETCH_ASSOC);
                        $globalPayerIdsArray = array_column($fetchallGlobalPayerIds, 'Global Payer ID');
                        if (empty($globalPayerIdsArray)) {
                            return "'-999'";
                        } else {
                            return "'" . implode("','", $globalPayerIdsArray) . "'";
                        }
                    });
                    $attributes['globalPayerIds'] = $globalPayerIds;
                } else {
                    $attributes['globalPayerIds'] = "'-999'";
                }
                   
                Session::put('jwt_attributes', $attributes);
                $user = new JwtUser($attributes);
                $user->user_type = 'Aggregator';
                Auth::guard('jwt')->login($user);
                return redirect()->route('newdashboard');
            }

        } catch (Exception $e) {
            return redirect()->route('login.invalid')->withErrors(['error' => 'Token could not be processed: ' . $e->getMessage()]);
        }
    }

    private function decodeJwtWithoutVerification($token)
    {
        $jwtParts = explode('.', $token);
        if (count($jwtParts) !== 3) {
            throw new Exception('Invalid JWT format');
        }
        $payload = base64_decode($jwtParts[1]);
        $payloadArray = json_decode($payload, true);

        if (json_last_error() !== JSON_ERROR_NONE) {
            throw new Exception('Invalid JSON in JWT payload');
        }
        return $payloadArray;
    }
    public function logout(Request $request)
    {
        session()->forget('jwt_attributes');
        return redirect()->route('login.invalid');
    }
    public function loginError(Request $request)
    {
        return view('errors.403');
    }

}