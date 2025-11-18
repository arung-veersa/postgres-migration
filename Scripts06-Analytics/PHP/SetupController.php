<?php

namespace App\Http\Controllers;

use Illuminate\Http\Request;
use Rdr\SnowflakeJodo\SnowflakeJodo;
use Illuminate\Support\Facades\Auth;
use PDO;
use App\Constants\SetupConstants;
use App\Constants\DatabaseConstants;
use App\Constants\MessageConstants;
class SetupController extends Controller
{
    protected $conn;
    protected $dbsuffix; // Define a protected property for table prefix
    public function __construct()
    {
        $this->conn = SnowflakeJodo::connect();
        $this->dbsuffix = env('DB_SUFFIX', '');
    }

    /**
     * Require user to have a specific role, abort with 403 if not
     */
    private function requireRole($role, $message = null)
    {
        if (!Auth::user()->hasRole($role)) {
            abort(403, $message ?? MessageConstants::UNAUTHORIZED_MESSAGE);
        }
    }

    /**
     * Get pagination parameters from request
     */
    private function getPaginationParams(Request $request)
    {
        $currentPage = $request->input('page', 1);
        $allowedPerPageOptions = [10, 50, 100, 200, 500];
        $perPage = $request->{SetupConstants::PER_PAGE};
        
        if (in_array($perPage, $allowedPerPageOptions)) {
            session([SetupConstants::PER_PAGE => $perPage]);
        } else {
            $perPage = session(SetupConstants::PER_PAGE, 10);
        }
        
        $offset = ($currentPage - 1) * $perPage;
        
        return compact('currentPage', 'perPage', 'offset');
    }

    /**
     * Create paginated results
     */
    private function createPaginatedResults($results, $rowCount, $perPage, $currentPage)
    {
        return new \Illuminate\Pagination\LengthAwarePaginator(
            $results,
            $rowCount,
            $perPage,
            $currentPage,
            ['path' => request()->url(), 'query' => request()->query()]
        );
    }

    private function getProviderIds()
    {
        $providerIds = env(DatabaseConstants::PROVIDER_IDS);
        $aproviderIds = env(DatabaseConstants::A_PROVIDER_IDS);
        
        return [
            'provider_id' => !empty($providerIds) ? $providerIds : Auth::user()->Provider_Id,
            'app_provider_id' => !empty($aproviderIds) ? $aproviderIds : Auth::user()->Application_Provider_Id
        ];
    }

    private function executeQuery($sql)
    {
        $stmt = $this->conn->prepare($sql);
        return $stmt->fetch(PDO::FETCH_ASSOC);
    }
    public function index()
    {
        $this->requireRole(SetupConstants::SUPER_ADMIN);
        $data[SetupConstants::SEO_TITLE] = 'System Settings';
        $data[SetupConstants::IS_PAGE] = 'system_settings';
        $statement = $this->conn->prepare('SELECT "ExtraDistance", "ExtraDistancePer", "NORESPONSELIMITTIME", "ID"
        FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.SETTINGS WHERE "ID" = 1');
        $data['Settings'] = $statement->fetch(PDO::FETCH_ASSOC);
        return view('setup.setting.system_settings',$data);
    }

    public function Dataupdate(Request $request)
    {
        $this->requireRole(SetupConstants::SUPER_ADMIN);
        $request->validate([
            'NoResponseLimitTime' => SetupConstants::REQUIRED_NUMERIC,
            'ExtraDistance' => SetupConstants::REQUIRED_NUMERIC,
        ], [
            'NoResponseLimitTime.required' => 'No Response Limit Time is required',
            'NoResponseLimitTime.numeric' => 'No Response Limit Time must be a number',
            'ExtraDistance.required' => 'Extra Distance is required',
            'ExtraDistance.numeric' => 'Extra Distance must be a number',
        ]);

        $NoResponseLimitTime = $request->input('NoResponseLimitTime') ?? null;
        $ExtraDistance = $request->input('ExtraDistance') ?? null;

        $ExtraDistancePer = $ExtraDistance/100+1;
        // Update data in Snowflake
        $sql = DatabaseConstants::UPDATE_CONFLICTREPORT.$this->dbsuffix.".PUBLIC.SETTINGS SET \"ExtraDistance\" = $ExtraDistance, \"ExtraDistancePer\" = $ExtraDistancePer, \"NORESPONSELIMITTIME\" = $NoResponseLimitTime WHERE \"ID\" = 1";
        $this->executeQuery($sql);
        
        session()->flash(MessageConstants::SUCCESS, 'Settings updated successfully !!');
        return redirect()->back();
    }

    public function ssnindex(Request $request)
    {
        $this->requireRole(SetupConstants::SUPER_ADMIN);

        $paginationParams = $this->getPaginationParams($request);
        extract($paginationParams);        
        $ssnFilter = $request->query('ssn');       
        $query = "SELECT ID, SSN FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.EXCLUDED_SSN";
        $countquery = DatabaseConstants::SELECT_COUNT_ID.$this->dbsuffix.".PUBLIC.EXCLUDED_SSN";
        if (!empty($ssnFilter)) {
            $query .= " WHERE SSN LIKE '%$ssnFilter%'";
            $countquery .= " WHERE SSN LIKE '%$ssnFilter%'";
        }
        $query .= " ORDER BY ID DESC LIMIT $perPage OFFSET $offset";
        $statement = $this->conn->prepare($query);
        
        $results = $statement->fetchAll(PDO::FETCH_ASSOC);
        $statement_count = $this->conn->prepare($countquery);        
        $total_results = $statement_count->fetch(PDO::FETCH_ASSOC);
        $rowCount = $total_results[DatabaseConstants::COUNT];        
        $paginatedResults = $this->createPaginatedResults($results, $rowCount, $perPage, $currentPage);
        $data[SetupConstants::RESULTS] = $paginatedResults;
        $data[SetupConstants::SEO_TITLE] = 'SSN Master';
        $data[SetupConstants::IS_PAGE] = 'ssn';
        return view('setup.ssn.index', $data);
    }

    public function SsnAdd(Request $request)
    {
        $this->requireRole(SetupConstants::SUPER_ADMIN);

        $request->validate([
            'ssn' => SetupConstants::REQUIRED,
        ],
        [
            'ssn.required' => 'The SSN field is required.',
        ]);

        $ssn = $request->ssn;

        $existingSSN = $this->conn->prepare(DatabaseConstants::SELECT_COUNT_ALL.$this->dbsuffix.".PUBLIC.EXCLUDED_SSN WHERE SSN = '$ssn'")->fetch(PDO::FETCH_ASSOC);

        if (!empty($existingSSN) && $existingSSN[DatabaseConstants::COUNT_UPPER] > 0) {
            return redirect()->back()->with(MessageConstants::ERROR, 'SSN already exists.');
        }

        $query = DatabaseConstants::INSERT_CONFLICTREPORT.$this->dbsuffix.".PUBLIC.EXCLUDED_SSN (SSN) VALUES ('$ssn')";
        $stmt = $this->conn->prepare($query);
        $stmt->fetch(PDO::FETCH_ASSOC);

        session()->flash(MessageConstants::SUCCESS, 'SSN has been created !!');

        return MessageConstants::SUCCESS;
    }


    public function GetSsnDetails(Request $request)
    {
        $this->requireRole(SetupConstants::SUPER_ADMIN);

        $id = $request->id;
        $statement = $this->conn->prepare("SELECT ID, SSN FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.EXCLUDED_SSN WHERE \"ID\" = $id");
        return $statement->fetch(PDO::FETCH_ASSOC);        
    }


    public function SsnEditSave(Request $request, $id)
    {
        $this->requireRole(SetupConstants::SUPER_ADMIN);

        $request->validate([
            'ssn' => SetupConstants::REQUIRED,
        ],
        [
            'ssn.required' => 'The SSN field is required.',
        ]);
        $ssn = $request->ssn;
        $existingSSN = $this->conn->prepare(DatabaseConstants::SELECT_COUNT_ALL.$this->dbsuffix.".PUBLIC.EXCLUDED_SSN WHERE SSN = '$ssn' AND ID != '$id'")->fetch(PDO::FETCH_ASSOC);
        if (!empty($existingSSN) && $existingSSN[DatabaseConstants::COUNT_UPPER] > 0) {
            return redirect()->back()->with(MessageConstants::ERROR, 'SSN already exists.');
        }

        $query = DatabaseConstants::UPDATE_CONFLICTREPORT.$this->dbsuffix.".PUBLIC.EXCLUDED_SSN SET SSN = '$ssn' WHERE ID = '$id'";
        $stmt = $this->conn->prepare($query); 
        $stmt->fetch(PDO::FETCH_ASSOC);

        session()->flash(MessageConstants::SUCCESS, 'SSN has been updated !!');
        return MessageConstants::SUCCESS;
    }

    public function SsnDelete(Request $request, $id)
    {
        $this->requireRole(SetupConstants::SUPER_ADMIN);
        $existingSSN = $this->conn->prepare(DatabaseConstants::SELECT_COUNT_ALL.$this->dbsuffix.".PUBLIC.EXCLUDED_SSN WHERE ID = '$id'")->fetch(PDO::FETCH_ASSOC);

        if (!empty($existingSSN) && $existingSSN[DatabaseConstants::COUNT_UPPER]==0) {
            abort(404); // Record not found, return 404 response
        }

        try {
            
            $query = DatabaseConstants::DELETE_CONFLICTREPORT.$this->dbsuffix.".PUBLIC.EXCLUDED_SSN WHERE ID = '$id'";
            $this->executeQuery($query);

            session()->flash(MessageConstants::SUCCESS, 'SSN has been deleted !!');
            return MessageConstants::SUCCESS;
        } catch (\Illuminate\Database\QueryException $e) {
            if ($e->getCode() == DatabaseConstants::SQL_INTEGRITY_CONSTRAINT_VIOLATION) { //23000 is sql code for integrity constraint violation
                // return error to user here
                return MessageConstants::ERROR;
            } else {
                return MessageConstants::ERROR2;
            }
        }
    }

    public function ContactMaintenanceIndex()
    {
        $this->requireRole(SetupConstants::PROVIDER);
        $providerIds = $this->getProviderIds();
        $ProviderID = $providerIds['provider_id'];
        $AppProviderID = $providerIds['app_provider_id'];
        $data[SetupConstants::SEO_TITLE] = 'Contact Maintenance';
        $data[SetupConstants::IS_PAGE] = 'contact_maintenance';
        $query = DatabaseConstants::SELECT_ALL_CONFLICTREPORT.$this->dbsuffix.".PUBLIC.CONTACT_MAINTENANCE WHERE \"ProviderID\" = '$ProviderID'";
        $statement = $this->conn->prepare($query);
        $contact_maintenanceresult = $statement->fetchAll(PDO::FETCH_ASSOC);
        $data['contact_maintenance'] = $contact_maintenanceresult;
        
        $pidAppPidPairs = array_map(function ($item) {
            return $item['PID'] . '~' . $item['APPLICATIONPID'];
        }, $contact_maintenanceresult);
        
        // Step 2: Format the values for the WHERE IN clause
        $pidAppPidPairsString = implode("','", $pidAppPidPairs);
        
        $query2 = "SELECT CONCAT(\"Provider Id\", '~', \"Application Provider Id\", '~', \"Provider Name\") AS \"id\", CONCAT(\"Provider Name\", ' (', \"Application Provider Id\", ')') AS \"text\" 
                   FROM ANALYTICS".$this->dbsuffix.".BI.DIMPROVIDER 
                   WHERE \"Is Active\" = TRUE 
                     AND \"Is Demo\" = FALSE";
        
        if ($provider = old('providers')) {
            // Extracting IDs from the array
            $ids = array_map(function($item) {
                return explode('~', $item)[0]; // Extracting the ID part before ~
            }, $provider);
        
            // Creating a comma-separated list of IDs for the query
            $idsList = "'" . implode("','", $ids) . "'";
        
            // Adding the WHERE condition for Provider Ids if $idsList is not empty
            if (!empty($idsList)) {
                $query2 .= " AND \"Provider Id\" IN ($idsList)";
            }
        }
        
        if (!empty($pidAppPidPairsString)) {
            $query2 .= " AND CONCAT(\"Provider Id\", '~', \"Application Provider Id\") IN ('$pidAppPidPairsString')";
        }

        $statement2 = $this->conn->prepare($query2);
        $results = $statement2->fetchAll(PDO::FETCH_ASSOC);

        $data['providerss'] = $results;

        return view('setup.contact_maintenance.index', $data);
    }

    public function ContactMaintenanceupdate(Request $request)
    {
        $this->requireRole(SetupConstants::PROVIDER);
        $request->validate([
            'providers.*' => [SetupConstants::REQUIRED, 'regex:/^[^\'\"<>]*$/'],
            'contact_name.*' => [SetupConstants::REQUIRED, 'regex:/^[a-zA-Z\s]+$/'],
            'phone.*' => [SetupConstants::REQUIRED, 'regex:/^\(\d{3}\) \d{3}-\d{4}$/'],
        ], [
            'providers.*.required' => 'Provider name is required.',
            'providers.*.regex' => 'Please select valid provider.',
            'contact_name.*.required' => 'Contact name is required.',
            'contact_name.*.regex' => 'Contact name must only contain letters and spaces.',
            'phone.*.required' => 'Phone number is required.',
            'phone.*.regex' => 'Phone number must be in the format (XXX) XXX-XXXX.',
        ]);

        $providerData = $request->providers;
        $providerIds = $this->getProviderIds();
        $ProviderID = $providerIds['provider_id'];
        $AppProviderID = $providerIds['app_provider_id'];

        // Get the list of IDs of providers from the request
        $newProviderIds = [];
        if (!empty($providerData)) {
            foreach ($providerData as $provider) {
                $parts = explode('~', $provider);
                $newProviderIds[] = $parts[0];
            }
        }
        

        // Fetch existing contact maintenance records for the user
        $query = DatabaseConstants::SELECT_ALL_CONFLICTREPORT.$this->dbsuffix.".PUBLIC.CONTACT_MAINTENANCE 
                WHERE \"ProviderID\" = '$ProviderID'";
        $statement = $this->conn->prepare($query);
        $existingContacts = $statement->fetchAll(PDO::FETCH_ASSOC);

        // Check and delete any records that are not in the new provider list
        foreach ($existingContacts as $existingContact) {
            $existingPid = $existingContact['PID'];
            $existingApplicationPid = $existingContact['APPLICATIONPID'];
            if (!in_array($existingPid, $newProviderIds)) {
                // Delete the row from the database
                $deleteQuery = DatabaseConstants::DELETE_CONFLICTREPORT.$this->dbsuffix.".PUBLIC.CONTACT_MAINTENANCE 
                                WHERE \"ProviderID\" = '$ProviderID'
                                AND \"PID\" = '$existingPid' 
                                AND \"APPLICATIONPID\" = '$existingApplicationPid'";
                $this->executeQuery($deleteQuery);
            }
        }
        if (!empty($providerData)) {
            foreach ($providerData as $index => $provider) {
                $parts = explode('~', $provider);
                $pId = $parts[0];
                $applicationPId = $parts[1];
               
                $query = DatabaseConstants::SELECT_ALL_CONFLICTREPORT.$this->dbsuffix.".PUBLIC.CONTACT_MAINTENANCE WHERE \"ProviderID\" = '$ProviderID' AND \"PID\" = '$pId' AND \"APPLICATIONPID\" = '".$applicationPId."'";
                $statement = $this->conn->prepare($query);
                $contact_maintenance = $statement->fetch(PDO::FETCH_ASSOC); 
                $contact_name = $request->input('contact_name')[$index] ?? null;
                $phone = $request->input('phone')[$index] ?? null;
                $UPDATED_BY = Auth::user()->id;
                
                $isUpdate = !empty($contact_maintenance);
                $sql = $isUpdate 
                    ? DatabaseConstants::UPDATE_CONFLICTREPORT.$this->dbsuffix.".PUBLIC.CONTACT_MAINTENANCE SET \"CONTACT_NAME\" = '$contact_name', \"PHONE\" = '$phone', \"ProviderID\" = '$ProviderID', \"AppProviderID\" = '$AppProviderID', \"UPDATED_BY\" = '$UPDATED_BY', \"UPDATED_AT\" = CURRENT_TIMESTAMP WHERE \"ProviderID\" = '$ProviderID' and \"PID\" = '$pId' AND \"APPLICATIONPID\" = '".$applicationPId."'"
                    : DatabaseConstants::INSERT_CONFLICTREPORT.$this->dbsuffix.".PUBLIC.CONTACT_MAINTENANCE (\"CONTACT_NAME\", \"PHONE\", \"ProviderID\", \"AppProviderID\", \"UPDATED_BY\", \"UPDATED_AT\", \"PID\", \"APPLICATIONPID\") VALUES ('$contact_name', '$phone', '$ProviderID', '$AppProviderID', '$UPDATED_BY', CURRENT_TIMESTAMP,'$pId','$applicationPId')";
                
                $this->executeQuery($sql);    
            }
        }
        
        session()->flash(MessageConstants::SUCCESS, 'Contact Maintenance updated successfully !!');
        return redirect()->back();
    }


    //Reason Maintenance
    public function ReasonMaintenanceIndex(Request $request)
    {
        $this->requireRole(SetupConstants::SUPER_ADMIN, MessageConstants::UNAUTHORIZED_MESSAGE_ALT);

        $paginationParams = $this->getPaginationParams($request);
        extract($paginationParams);        
        $rnFilter = $request->query('rn');       
        $query = "SELECT ID, \"Title\", \"Description\" FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.NORESPONSEREASONS";
        $countquery = DatabaseConstants::SELECT_COUNT_ID.$this->dbsuffix.".PUBLIC.NORESPONSEREASONS";
        if (!empty($rnFilter)) {
            $query .= " WHERE \"Title\" LIKE '%$rnFilter%'";
            $countquery .= " WHERE \"Title\" LIKE '%$rnFilter%'";
        }
        $query .= " ORDER BY ID DESC LIMIT $perPage OFFSET $offset";
        $statement = $this->conn->prepare($query);
        
        $results = $statement->fetchAll(PDO::FETCH_ASSOC);
        $statement_count = $this->conn->prepare($countquery);        
        $total_results = $statement_count->fetch(PDO::FETCH_ASSOC);
        $rowCount = $total_results[DatabaseConstants::COUNT];        
        $paginatedResults = $this->createPaginatedResults($results, $rowCount, $perPage, $currentPage);
        $data[SetupConstants::RESULTS] = $paginatedResults;
        $data[SetupConstants::SEO_TITLE] = 'Reason Maintenance';
        $data[SetupConstants::IS_PAGE] = 'reason-maintenance';
        return view('setup.reason-maintenance.index', $data);
    }

    public function GetreasonDetails(Request $request)
    {
        $this->requireRole(SetupConstants::SUPER_ADMIN, MessageConstants::UNAUTHORIZED_MESSAGE_ALT);
        $id = $request->id;
        $statement = $this->conn->prepare("SELECT ID, \"Title\", \"Description\" FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.NORESPONSEREASONS WHERE \"ID\" = $id");
        return $statement->fetch(PDO::FETCH_ASSOC);        
    }

    public function ReasonAdd(Request $request)
    {
        $this->requireRole(SetupConstants::SUPER_ADMIN, MessageConstants::UNAUTHORIZED_MESSAGE_ALT);

        $request->validate([
            'reason' => SetupConstants::REQUIRED_STRING,
            'description' => SetupConstants::REQUIRED_STRING,
        ], [
            'reason.required' => 'The Reason field is required.',
            'reason.string' => 'The Reason field must be a string.',
            'description.required' => 'The Description field is required.',
            'description.string' => 'The Description field must be a string.',
        ]);        

        $reason = $request->reason;
        $description = $request->description;

        $existingreason = $this->conn->prepare(DatabaseConstants::SELECT_COUNT_ALL.$this->dbsuffix.".PUBLIC.NORESPONSEREASONS WHERE \"Title\" = '$reason'")->fetch(PDO::FETCH_ASSOC);
        if (!empty($existingreason) && $existingreason[DatabaseConstants::COUNT_UPPER] > 0) {
            return 'already exists';
        }

        $query = DatabaseConstants::INSERT_CONFLICTREPORT.$this->dbsuffix.".PUBLIC.NORESPONSEREASONS (\"Title\",\"Description\") VALUES ('$reason','$description')";
        $stmt = $this->conn->prepare($query);
        $stmt->fetch(PDO::FETCH_ASSOC);

        session()->flash(MessageConstants::SUCCESS, 'Reason has been created !!');

        return MessageConstants::SUCCESS;
    }
    
    public function ReasonEditSave(Request $request, $id)
    {
        $this->requireRole(SetupConstants::SUPER_ADMIN, MessageConstants::UNAUTHORIZED_MESSAGE_ALT);

        $request->validate([
            'reason' => SetupConstants::REQUIRED_STRING,
            'description' => SetupConstants::REQUIRED_STRING,
        ], [
            'reason.required' => 'The Reason field is required.',
            'reason.string' => 'The Reason field must be a string.',
            'description.required' => 'The Description field is required.',
            'description.string' => 'The Description field must be a string.',
        ]);
        $reason = $request->reason;
        $description = $request->description;
        $existingSSN = $this->conn->prepare(DatabaseConstants::SELECT_COUNT_ALL.$this->dbsuffix.".PUBLIC.NORESPONSEREASONS WHERE \"Title\" = '$reason' AND ID != '$id'")->fetch(PDO::FETCH_ASSOC);
        if (!empty($existingSSN) && $existingSSN[DatabaseConstants::COUNT_UPPER] > 0) {
            return 'already exists';
        }

        $query = DatabaseConstants::UPDATE_CONFLICTREPORT.$this->dbsuffix.".PUBLIC.NORESPONSEREASONS SET \"Title\" = '$reason', \"Description\" = '$description' WHERE ID = '$id'";
        $stmt = $this->conn->prepare($query); 
        $stmt->fetch(PDO::FETCH_ASSOC);

        session()->flash(MessageConstants::SUCCESS, 'Reason has been updated !!');
        return MessageConstants::SUCCESS;
    }


    public function ReasonDelete (Request $request, $id)
    {
        $this->requireRole(SetupConstants::SUPER_ADMIN, MessageConstants::UNAUTHORIZED_MESSAGE_ALT);

        $id = $request->id;
        $existingReason = $this->conn->prepare(DatabaseConstants::SELECT_COUNT_ALL.$this->dbsuffix.".PUBLIC.NORESPONSEREASONS WHERE ID = '$id'")->fetch(PDO::FETCH_ASSOC);
        
        if (!empty($existingReason) && $existingReason[DatabaseConstants::COUNT_UPPER] == 0) {
            return MessageConstants::ERROR;
        }

        try {
            
            $query = DatabaseConstants::DELETE_CONFLICTREPORT.$this->dbsuffix.".PUBLIC.NORESPONSEREASONS WHERE ID = '$id'";
            $stmt = $this->conn->prepare($query);
            $stmt->fetch(PDO::FETCH_ASSOC);

            session()->flash(MessageConstants::SUCCESS, 'Reason has been deleted !!');
            return MessageConstants::SUCCESS;
        } catch (\Illuminate\Database\QueryException $e) {
            // Return ERROR for both integrity constraint violation and other exceptions
            return $e->getCode() == DatabaseConstants::SQL_INTEGRITY_CONSTRAINT_VIOLATION 
                ? MessageConstants::ERROR 
                : MessageConstants::ERROR2;
        }
    }


    //MPH
    private function validateMPHData(Request $request)
    {
        return $request->validate([
            'types.*' => SetupConstants::REQUIRED_STRING,
            'from.*' => SetupConstants::REQUIRED_NUMERIC,
            'to.*' => SetupConstants::REQUIRED_NUMERIC,
            SetupConstants::AVERAGE_MPH.'.*' => SetupConstants::REQUIRED_NUMERIC,
        ], [
            'types.*.required' => 'The type field is required.',
            'types.*.string' => 'The type field must be a string.',
            'from.*.required' => 'The from field is required.',
            'from.*.numeric' => 'The from field must be a number.',
            'to.*.required' => 'The to field is required.',
            'to.*.numeric' => 'The to field must be a number.',
            'average_mph.*.required' => 'The average miles per hour field is required.',
            'average_mph.*.numeric' => 'The average miles per hour field must be a number.',
        ]);
    }

    private function validateMPHRanges($validatedData)
    {
        $fromValues = $validatedData['from'];
        $toValues = $validatedData['to'];
        $checkedValues = [];

        foreach ($fromValues as $index => $from) {
            $to = $toValues[$index];

            if ($from >= $to) {
                return redirect()->back()->withErrors(['from.'.$index => 'The from value should be smaller than the to value.'])->withInput();
            }

            foreach ($checkedValues as $checked) {
                if (($from >= $checked['from'] && $from <= $checked['to']) || ($to >= $checked['from'] && $to <= $checked['to'])) {
                    return redirect()->back()->withErrors(['from.'.$index => 'The from and to values should not overlap with existing entries.'])->withInput();
                }
            }

            if (in_array(['from' => $from, 'to' => $to], $checkedValues)) {
                return redirect()->back()->withErrors(['from.'.$index => 'The from and to values should not be repeated.'])->withInput();
            }

            $checkedValues[] = ['from' => $from, 'to' => $to];
        }
        
        return null;
    }

    private function prepareMPHOperations($validatedData)
    {
        $stmt = $this->conn->prepare(DatabaseConstants::SELECT_ALL_CONFLICTREPORT.$this->dbsuffix.".PUBLIC.MPH");
        $existingRecords = $stmt->fetchAll(PDO::FETCH_ASSOC);
        
        $existingValues = [];
        foreach ($existingRecords as $record) {
            $existingValues[$record['To']] = $record;
        }

        $toAdd = [];
        $toUpdate = [];
        $toDelete = array_keys($existingValues);

        foreach ($validatedData['from'] as $index => $from) {
            $to = $validatedData['to'][$index];
            $type = $validatedData['types'][$index];
            $average_mph = $validatedData['average_mph'][$index];

            if (isset($existingValues[$to])) {
                $existingRecord = $existingValues[$to];
                $isUnchanged = $existingRecord['From'] == $from && 
                              $existingRecord['TYPE'] == $type && 
                              $existingRecord['AverageMilesPerHour'] == $average_mph;
                
                if ($isUnchanged) {
                    $toDelete = array_diff($toDelete, [$to]);
                } else {
                    $toUpdate[] = ['type' => $type, 'from' => $from, 'to' => $to, 'average_mph' => $average_mph];
                    $toDelete = array_diff($toDelete, [$to]);
                }
            } else {
                $toAdd[] = ['type' => $type, 'from' => $from, 'to' => $to, 'average_mph' => $average_mph];
            }
        }

        return ['add' => $toAdd, 'update' => $toUpdate, 'delete' => $toDelete];
    }

    private function executeMPHOperations($operations)
    {
        foreach ($operations['add'] as $record) {
            $sql = DatabaseConstants::INSERT_CONFLICTREPORT.$this->dbsuffix.".PUBLIC.MPH (\"TYPE\", \"From\", \"To\", \"AverageMilesPerHour\") VALUES ('{$record['type']}', {$record['from']}, {$record['to']}, {$record['average_mph']})";
            $this->executeQuery($sql);
        }

        foreach ($operations['update'] as $record) {
            $sql = DatabaseConstants::UPDATE_CONFLICTREPORT.$this->dbsuffix.".PUBLIC.MPH SET \"TYPE\" = '{$record['type']}', \"AverageMilesPerHour\" = {$record['average_mph']} WHERE \"From\" = {$record['from']} AND \"To\" = {$record['to']}";
            $this->executeQuery($sql);
        }   

        foreach ($operations['delete'] as $to) {
            $sql = DatabaseConstants::DELETE_CONFLICTREPORT.$this->dbsuffix.".PUBLIC.MPH WHERE \"To\" = {$to}";
            $this->executeQuery($sql);
        }
    }

    public function MPHIndex(Request $request)
    {
        $this->requireRole(SetupConstants::SUPER_ADMIN);

        $query = "SELECT ID, \"TYPE\", \"From\",\"To\",\"AverageMilesPerHour\" FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.MPH";
        $statement = $this->conn->prepare($query);
        
        $results = $statement->fetchAll(PDO::FETCH_ASSOC);
        $data['mph_data'] = $results;
        $data[SetupConstants::SEO_TITLE] = 'MPH Formula';
        $data[SetupConstants::IS_PAGE] = 'mph-formula';
        return view('setup.mph.index', $data);
    }

    public function MPHSave(Request $request)
    {
        $this->requireRole(SetupConstants::SUPER_ADMIN);

        $validatedData = $this->validateMPHData($request);
        
        $validationError = $this->validateMPHRanges($validatedData);
        if ($validationError) {
            return $validationError;
        }

            try {
                $operations = $this->prepareMPHOperations($validatedData);
                $this->executeMPHOperations($operations);

        session()->flash(MessageConstants::SUCCESS, 'MPG has been updated !!');
        return redirect()->route('mph-formula');
    } catch (Exception $e) {
        return redirect()->back()->withErrors(['error' => 'An error occurred while saving the data. Please try again.'])->withInput();
    }
}


    //Agency

    public function agencyindex(Request $request)
    {
        $this->requireRole(SetupConstants::SUPER_ADMIN);

        $paginationParams = $this->getPaginationParams($request);
        extract($paginationParams);        
        $agencyFilter = $request->query(SetupConstants::AGENCY);       
        $query = DatabaseConstants::SELECT_ALL_CONFLICTREPORT.$this->dbsuffix.".PUBLIC.EXCLUDED_AGENCY";
        $countquery = DatabaseConstants::SELECT_COUNT_ID.$this->dbsuffix.".PUBLIC.EXCLUDED_AGENCY";
        if (!empty($agencyFilter)) {
            $query .= " WHERE \"AgencyName\" LIKE '%$agencyFilter%'";
            $countquery .= " WHERE \"AgencyName\" LIKE '%$agencyFilter%'";
        }
        $query .= " ORDER BY ID DESC LIMIT $perPage OFFSET $offset";
        $statement = $this->conn->prepare($query);
        
        $results = $statement->fetchAll(PDO::FETCH_ASSOC);
        $statement_count = $this->conn->prepare($countquery);        
        $total_results = $statement_count->fetch(PDO::FETCH_ASSOC);
        $rowCount = $total_results[DatabaseConstants::COUNT];        
        $paginatedResults = $this->createPaginatedResults($results, $rowCount, $perPage, $currentPage);
        $data[SetupConstants::RESULTS] = $paginatedResults;
        $data[SetupConstants::SEO_TITLE] = 'Agency Master';
        $data[SetupConstants::IS_PAGE] = 'ssn';
        return view('setup.agency.index', $data);
    }

    public function AgencyAdd(Request $request)
    {
        $this->requireRole(SetupConstants::SUPER_ADMIN);

        $request->validate([
            SetupConstants::AGENCY => SetupConstants::REQUIRED,
        ],
        [
            'agency.required' => 'The field is required.',
        ]);

        $agency = $request->{SetupConstants::AGENCY};
        $agencyIDs = explode('~', $agency);


        $existingSSN = $this->conn->prepare(DatabaseConstants::SELECT_COUNT_ALL.$this->dbsuffix.".PUBLIC.EXCLUDED_AGENCY WHERE \"ProviderID\" = '".$agencyIDs[0]."' AND \"AppProviderID\" = '".$agencyIDs[1]."'")->fetch(PDO::FETCH_ASSOC);

        if (!empty($existingSSN) && $existingSSN[DatabaseConstants::COUNT_UPPER] > 0) {
            return redirect()->back()->with(MessageConstants::ERROR, 'Agency already exists.');
        }
        $query = DatabaseConstants::INSERT_CONFLICTREPORT.$this->dbsuffix.".PUBLIC.EXCLUDED_AGENCY (\"AgencyName\", \"ProviderID\", \"AppProviderID\") VALUES ('".$agencyIDs[2]."', '".$agencyIDs[0]."', '".$agencyIDs[1]."')";
        $stmt = $this->conn->prepare($query);
        $stmt->fetch(PDO::FETCH_ASSOC);

        session()->flash(MessageConstants::SUCCESS, 'Agency to exclude has been created !!');

        return MessageConstants::SUCCESS;
    }


    public function GetAgencyDetails(Request $request)
    {
        $this->requireRole(SetupConstants::SUPER_ADMIN);
        $id = $request->id;
        $statement = $this->conn->prepare("SELECT EA.*, CONCAT(EA.\"ProviderID\", '~', EA.\"AppProviderID\", '~', EA.\"AgencyName\") AS \"AgencyFullName\" FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.EXCLUDED_AGENCY AS EA WHERE \"ID\" = $id");
        return $statement->fetch(PDO::FETCH_ASSOC);
    }


    public function AgencyEditSave(Request $request, $id)
    {
        $this->requireRole(SetupConstants::SUPER_ADMIN);

        $request->validate([
            SetupConstants::AGENCY => SetupConstants::REQUIRED,
        ],
        [
            'agency.required' => 'The field is required.',
        ]);

        $agency = $request->{SetupConstants::AGENCY};
        $agencyIDs = explode('~', $agency);


        $existingSSN = $this->conn->prepare(DatabaseConstants::SELECT_COUNT_ALL.$this->dbsuffix.".PUBLIC.EXCLUDED_AGENCY WHERE \"ProviderID\" = '".$agencyIDs[0]."' AND \"AppProviderID\" = '".$agencyIDs[1]."' AND ID != '$id'")->fetch(PDO::FETCH_ASSOC);

        if (!empty($existingSSN) && $existingSSN[DatabaseConstants::COUNT_UPPER] > 0) {
            return redirect()->back()->with(MessageConstants::ERROR, 'Agency already exists.');
        }

        $query = DatabaseConstants::UPDATE_CONFLICTREPORT.$this->dbsuffix.".PUBLIC.EXCLUDED_AGENCY SET \"AgencyName\" = '".$agencyIDs[2]."', \"ProviderID\" = '".$agencyIDs[0]."', \"AppProviderID\" = '".$agencyIDs[1]."' WHERE ID = '$id'";
        $stmt = $this->conn->prepare($query); 
        $stmt->fetch(PDO::FETCH_ASSOC);

        session()->flash(MessageConstants::SUCCESS, 'Agency has been updated !!');
        return MessageConstants::SUCCESS;
    }

    public function AgencyDelete(Request $request, $id)
    {
        $this->requireRole(SetupConstants::SUPER_ADMIN);

        $existingSSN = $this->conn->prepare(DatabaseConstants::SELECT_COUNT_ALL.$this->dbsuffix.".PUBLIC.EXCLUDED_AGENCY WHERE ID = '$id'")->fetch(PDO::FETCH_ASSOC);

        if (!empty($existingSSN) && $existingSSN[DatabaseConstants::COUNT_UPPER]==0) {
            abort(404); // Record not found, return 404 response
        }

        try {
            
            $query = DatabaseConstants::DELETE_CONFLICTREPORT.$this->dbsuffix.".PUBLIC.EXCLUDED_AGENCY WHERE ID = '$id'";
            $stmt = $this->conn->prepare($query);
            $stmt->fetch(PDO::FETCH_ASSOC);

            session()->flash(MessageConstants::SUCCESS, 'Agency has been deleted !!');
            return MessageConstants::SUCCESS;
        } catch (\Illuminate\Database\QueryException $e) {
            if ($e->getCode() == DatabaseConstants::SQL_INTEGRITY_CONSTRAINT_VIOLATION) { //23000 is sql code for integrity constraint violation
                // return error to user here
                return MessageConstants::ERROR;
            } else {
                return MessageConstants::ERROR2;
            }
        }
    }


    //Notifications

    public function NotificationIndex(Request $request)
    {
        $this->requireRole(SetupConstants::PROVIDER);
        $providerIds = $this->getProviderIds();
        $ProviderID = $providerIds['provider_id'];
        $AppProviderID = $providerIds['app_provider_id'];


        $UserID = Auth::user()->id;

        $paginationParams = $this->getPaginationParams($request);
        extract($paginationParams);        
        $NotificationType = $request->query('NotificationType');       
        $query = "SELECT N.* FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.NOTIFICATIONS AS N";
        $query .= " INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS CVM ON CVM.CONFLICTID=N.CONFLICTID";
        $countquery = "SELECT COUNT(N.ID) AS \"count\" FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.NOTIFICATIONS AS N";
        $countquery .= " INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS CVM ON CVM.CONFLICTID=N.CONFLICTID";
        $query .= " WHERE CVM.\"ProviderID\" = '".$ProviderID."'";
        $countquery .= " WHERE CVM.\"ProviderID\" = '".$ProviderID."'";
        if($ofcquery = ofcquery()){
            $query .= " AND CVM.\"OfficeID\" IN (".$ofcquery.")";
            $countquery .= " AND CVM.\"OfficeID\" IN (".$ofcquery.")";
        }
        if (!empty($NotificationType)) {
            $query .= " AND N.\"NotificationType\" = '$NotificationType'";
            $countquery .= " AND N.\"NotificationType\" = '$NotificationType'";
        }
        $query .= " ORDER BY N.ID DESC LIMIT $perPage OFFSET $offset";
        $statement = $this->conn->prepare($query);
        
        $results = $statement->fetchAll(PDO::FETCH_ASSOC);
        $statement_count = $this->conn->prepare($countquery);        
        $total_results = $statement_count->fetch(PDO::FETCH_ASSOC);
        $rowCount = $total_results[DatabaseConstants::COUNT];        
        $paginatedResults = $this->createPaginatedResults($results, $rowCount, $perPage, $currentPage);
        $data[SetupConstants::RESULTS] = $paginatedResults;
        $data[SetupConstants::SEO_TITLE] = 'Notifications';
        $data[SetupConstants::IS_PAGE] = 'notifications';
        return view('setup.notifications.index', $data);
    }


    //Payer Notifications Reminder Setup

    public function NotificationRemindersIndex(Request $request)
    {
        $this->requireRole('Payer');

        if (app()->environment('local')) 
        {
            $UserID = env('DEBUG_USER_ID');
            $PayerID = env('PAYERIDS');
            $AppPayerID = env('APAYERIDS');
        } 
        else 
        {
            $PayerID = Auth::user()->Payer_Id;
            $AppPayerID = Auth::user()->Application_Payer_Id;
            $UserID = Auth::user()->id;
        }

        $paginationParams = $this->getPaginationParams($request);
        extract($paginationParams);        
        $ProviderName = $request->query('ProviderName');       
        $query = DatabaseConstants::SELECT_ALL_CONFLICTREPORT.$this->dbsuffix.".PUBLIC.PAYER_PROVIDER_REMINDERS";
        $countquery = DatabaseConstants::SELECT_COUNT_ID.$this->dbsuffix.".PUBLIC.PAYER_PROVIDER_REMINDERS";
        $query .= " WHERE \"PayerID\" = '".$PayerID."'";
        $countquery .= " WHERE \"PayerID\" = '".$PayerID."'";
        if (!empty($ProviderName)) {
            $query .= " AND \"ProviderName\" LIKE '%$ProviderName%'";
            $countquery .= " AND \"ProviderName\" LIKE '%$ProviderName%'";
        }
        $query .= " ORDER BY \"ProviderName\" ASC LIMIT $perPage OFFSET $offset";
        $statement = $this->conn->prepare($query);
        
        $results = $statement->fetchAll(PDO::FETCH_ASSOC);
        $statement_count = $this->conn->prepare($countquery);        
        $total_results = $statement_count->fetch(PDO::FETCH_ASSOC);
        $rowCount = $total_results[DatabaseConstants::COUNT];        
        $paginatedResults = $this->createPaginatedResults($results, $rowCount, $perPage, $currentPage);
        $data[SetupConstants::RESULTS] = $paginatedResults;
        $data[SetupConstants::SEO_TITLE] = 'Notification Reminders';
        $data[SetupConstants::IS_PAGE] = 'notification-reminders';
        return view('setup.notifications.notification-reminders', $data);
    }

    public function NotificationRemindersUpdate(Request $request)
    {
        $this->requireRole('Payer', MessageConstants::UNAUTHORIZED_MESSAGE);
        $PayerID = Auth::user()->Payer_Id;
        $AppPayerID = Auth::user()->Application_Payer_Id;
        $UserID = Auth::user()->id;

        $id = $request->id;
        $valup = ($request->valup) ? "'".$request->valup."'" : 'NULL';
        $existingData = $this->conn->prepare("SELECT COUNT(*) as count FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.PAYER_PROVIDER_REMINDERS WHERE ID = '$request->id' AND \"PayerID\" = '".$PayerID."'")->fetch(PDO::FETCH_ASSOC);

        if (!empty($existingData) && $existingData[DatabaseConstants::COUNT_UPPER]==0) {
            abort(404); // Record not found, return 404 response
        }
        try {
            
            $query = "UPDATE CONFLICTREPORT".$this->dbsuffix.".PUBLIC.PAYER_PROVIDER_REMINDERS SET \"NumberOfDays\" = ".$valup." WHERE ID = '$id'";
            $stmt = $this->conn->prepare($query);
            $stmt->fetch(PDO::FETCH_ASSOC);
            return 'success';
        } catch (\Illuminate\Database\QueryException $e) {
            if ($e->getCode() == "23000") { //23000 is sql code for integrity constraint violation
                // return error to user here
                return 'error';
            } else {
                return 'error2';
            }
        }
    }
}