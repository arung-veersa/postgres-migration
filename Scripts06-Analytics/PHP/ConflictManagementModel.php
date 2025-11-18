<?php

namespace App\Models;

use Illuminate\Database\Eloquent\Factories\HasFactory;
use Illuminate\Database\Eloquent\Model;
use Rdr\SnowflakeJodo\SnowflakeJodo;
use PDO;
use Illuminate\Support\Facades\Auth;
use App\Helpers\ConflictTypeHelper;

class ConflictManagementModel extends Model
{
    use HasFactory;
    protected $conn;
    protected $dbsuffix; // Define a protected property for table prefix
    public function __construct()
    {
        // Snowflake connection
        $this->conn = SnowflakeJodo::connect();
        $this->dbsuffix = env('DB_SUFFIX', '');
    }

    public function getVisits($request, $ProviderID, $AppProviderID, $UserID, $all = '', $CONFLICTIDCUR=''){
        $queryParams = [];
        $nextprev = false;
        if(($all=='Next' || $all=='Prev') && !empty($CONFLICTIDCUR)){
            $queryParams = session('visit_query_params', []);
            $nextprev = true;
        }
        $currentPage = $request->input('page', 1);
        // Define the number of items per page
        // $perPage = $request->per_page ?? 10;

        // Define allowed values for pagination
        $allowedPerPageOptions = [10, 50, 100, 200, 500];

        // Get the per_page value from the request or session
        $perPage = $request->per_page;

        // Check if per_page is a valid numeric value in the allowed options
        if (in_array($perPage, $allowedPerPageOptions)) {
            // Store the per_page value in the session if it's valid
            session(['per_page' => $perPage]);
        } else {
            // Retrieve the per_page from session or default to 10
            $perPage = session('per_page', 10);
        }
        $offset = ($currentPage - 1) * $perPage;
        $TOPL = '';
        if($all == '-1')
        {
            $TOPL = ' TOP 200';
        }else if(in_array($all, ['Next', 'Prev']))
        {
            $TOPL = ' TOP 1';
        }        
        $sortableLinks = [
            'conflictid' => 'V1."CONFLICTID"',
            'office' => 'V1."Office"',
            'contract' => 'V1."Contract"',
            'parentstatusflag' => 'ParentStatusFlag',
            'aidecode' => 'V1."AideCode"',
            'aidelname' => 'V1."AideLName"',
            'aidefname' => 'V1."AideFName"',
            'ssn' => 'COALESCE(V1."AideSSN", V1."SSN")',
            'padmissionid' => 'V1."P_PAdmissionID"',
            'plname' => 'V1."P_PLName"',
            'pfname' => 'V1."P_PFName"',
            'pmedicaidnumber' => 'V1."P_PMedicaidNumber"',
            'visitdate' => 'V1."VisitDate"',
            'billeddate' => 'V1."BilledDate"',
            'schedulehours' => 'ScheduleHours',
            'billedhours' => 'V1."BilledHours"'
        ];
        $sortableLinksAD = [
            'asc' => 'asc',
            'desc' => 'desc'
        ];
        if(isset($queryParams['sort']) && isset($sortableLinks[strtolower($queryParams['sort'])]) && $nextprev==true){
            $SortByField = $sortableLinks[strtolower($queryParams['sort'])];
        }else if($request->sort && isset($sortableLinks[strtolower($request->sort)])){
            $SortByField = $sortableLinks[strtolower($request->sort)];
        }else{
            $SortByField = 'CONFLICTID';
        }
        if(isset($queryParams['direction']) && isset($sortableLinks[strtolower($queryParams['direction'])]) && $nextprev==true){
            $SortByAD = $sortableLinksAD[strtolower($queryParams['direction'])];
        }else if($request->direction && isset($sortableLinksAD[strtolower($request->direction)])){
            $SortByAD = strtoupper($sortableLinksAD[strtolower($request->direction)]);
        }else{
            $SortByAD = 'DESC';
        }
        $query = "SELECT DISTINCT".$TOPL." V1.\"CONFLICTID\", V1.\"SSN\", V1.\"ProviderID\", V1.\"AppProviderID\", V1.\"ProviderName\", V1.\"VisitID\", V1.\"AppVisitID\", V1.\"VisitDate\", V1.\"SchStartTime\", V1.\"SchEndTime\", V1.\"VisitStartTime\", V1.\"VisitEndTime\", V1.\"EVVStartTime\", V1.\"EVVEndTime\", V1.\"CaregiverID\", V1.\"AppCaregiverID\", V1.\"AideCode\", V1.\"AideName\", V1.\"AideFName\", V1.\"AideLName\", COALESCE(V1.\"AideSSN\", V1.\"SSN\") AS \"AideSSN\", V1.\"OfficeID\", V1.\"AppOfficeID\", V1.\"Office\",
        V1.\"P_PatientID\", V1.\"P_AppPatientID\", V1.\"P_PAdmissionID\", V1.\"P_PName\", V1.\"P_PFName\", V1.\"P_PLName\", V1.\"P_PMedicaidNumber\", V1.\"P_PAddressID\", V1.\"P_PAppAddressID\", V1.\"P_PAddressL1\", V1.\"P_PAddressL2\", V1.\"P_PCity\", V1.\"P_PAddressState\", V1.\"P_PZipCode\", V1.\"P_PCounty\",
        
        V1.\"PLongitude\", V1.\"PLatitude\", V1.\"PayerID\", V1.\"AppPayerID\", V1.\"Contract\", V1.\"BilledDate\", V1.\"BilledHours\", V1.\"Billed\", V1.\"ServiceCodeID\", V1.\"AppServiceCodeID\", V1.\"RateType\", V1.\"ServiceCode\", TIMESTAMPDIFF(MINUTE, V1.\"SchStartTime\", V1.\"SchEndTime\") / 60 AS \"sch_hours\",
        V1.\"RateType\",      
        CASE
           WHEN V2.\"StatusFlag\" IN ('D', 'R') THEN 'R'
           ELSE V2.\"StatusFlag\"
        END AS \"ParentStatusFlag\",
        V2.\"StatusFlag\" AS \"OrgParentStatusFlag\",
        V2.\"NoResponseFlag\",
        CONCAT(V1.\"VisitID\", '~',V1.\"AppVisitID\") as \"VAPPID\",        
        CONCAT(V1.\"VisitID\", '~',V1.\"AppVisitID\") as \"APatientAPPID\",
        DATEDIFF(DAY, V1.\"CRDATEUNIQUE\", GETDATE()) AS \"AgeInDays\",
        CASE 
            WHEN DATEDIFF(DAY, V1.\"CRDATEUNIQUE\", GETDATE()) > SETT.NORESPONSELIMITTIME THEN TRUE
            ELSE FALSE
        END AS ALLOWDELETE,
        V2.\"FlagForReview\"
         FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS V1 INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTS AS V2 ON V2.\"CONFLICTID\"=V1.\"CONFLICTID\" CROSS JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.\"SETTINGS\" AS SETT";
        $countquery = "SELECT COUNT(DISTINCT V1.\"CONFLICTID\") AS \"count\" FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS V1 INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTS AS V2 ON V2.\"CONFLICTID\"=V1.\"CONFLICTID\"";
        $query .= " WHERE V1.\"ProviderID\" = '".$ProviderID."'";
        $countquery .= " WHERE V1.\"ProviderID\" = '".$ProviderID."'";
        if($ofcquery = ofcquery()){
            $query .= " AND V1.\"OfficeID\" IN (".$ofcquery.")";
            $countquery .= " AND V1.\"OfficeID\" IN (".$ofcquery.")";
        }
        // $query .= " AND CONCAT(\"P_PatientID\", '~', \"P_AppPatientID\") != '00000000-0000-0000-0000-db4ceb2647f0~0'";
        // $countquery .= " AND CONCAT(\"P_PatientID\", '~', \"P_AppPatientID\") != '00000000-0000-0000-0000-db4ceb2647f0~0'";
        if(isset($queryParams['PAdmissionID']) && $nextprev==true){
            $PAdmissionID = $queryParams['PAdmissionID'];
        }else if($request->PAdmissionID){
            $PAdmissionID = $request->PAdmissionID;
        }else{
            $PAdmissionID = '';
        }
        if ($PAdmissionID) {
            $query .= " AND V1.\"P_PAdmissionID\" ILIKE '%$PAdmissionID%'";
            $countquery .= " AND V1.\"P_PAdmissionID\" ILIKE '%$PAdmissionID%'";
        }
        if(isset($queryParams['MedicaidID']) && $nextprev==true){
            $MedicaidID = $queryParams['MedicaidID'];
        }else if($request->MedicaidID){
            $MedicaidID = $request->MedicaidID;
        }else{
            $MedicaidID = '';
        }
        if ($MedicaidID) {
            $query .= " AND V1.\"P_PMedicaidNumber\" ILIKE '%$MedicaidID%'";
            $countquery .= " AND V1.\"P_PMedicaidNumber\" ILIKE '%$MedicaidID%'";
        }
        if(isset($queryParams['PLName']) && $nextprev==true){
            $PLName = $queryParams['PLName'];
        }else if($request->PLName){
            $PLName = $request->PLName;
        }else{
            $PLName = '';
        }
        if ($PLName) {
            $query .= " AND V1.\"P_PLName\" ILIKE '%$PLName%'";
            $countquery .= " AND V1.\"P_PLName\" ILIKE '%$PLName%'";
        }
        if(isset($queryParams['PFName']) && $nextprev==true){
            $PFName = $queryParams['PFName'];
        }else if($request->PFName){
            $PFName = $request->PFName;
        }else{
            $PFName = '';
        }
        if ($PFName) {
            $query .= " AND V1.\"P_PFName\" ILIKE '%$PFName%'";
            $countquery .= " AND V1.\"P_PFName\" ILIKE '%$PFName%'";
        }
        if(isset($queryParams['AideCode']) && $nextprev==true){
            $AideCode = $queryParams['AideCode'];
        }else if($request->AideCode){
            $AideCode = $request->AideCode;
        }else{
            $AideCode = '';
        }
        if ($AideCode) {
            $query .= " AND V1.\"AideCode\" ILIKE '%$AideCode%'";
            $countquery .= " AND V1.\"AideCode\" ILIKE '%$AideCode%'";
        }
        if(isset($queryParams['AideLName']) && $nextprev==true){
            $AideLName = $queryParams['AideLName'];
        }else if($request->AideLName){
            $AideLName = $request->AideLName;
        }else{
            $AideLName = '';
        }
        if ($AideLName) {
            $query .= " AND V1.\"AideLName\" ILIKE '%$AideLName%'";
            $countquery .= " AND V1.\"AideLName\" ILIKE '%$AideLName%'";
        }
        if(isset($queryParams['AideFName']) && $nextprev==true){
            $AideFName = $queryParams['AideFName'];
        }else if($request->AideFName){
            $AideFName = $request->AideFName;
        }else{
            $AideFName = '';
        }
        if ($AideFName) {
            $query .= " AND V1.\"AideFName\" ILIKE '%$AideFName%'";
            $countquery .= " AND V1.\"AideFName\" ILIKE '%$AideFName%'";
        }
        $statusFlags = [];
        if(isset($queryParams['status_flags']) && is_array($queryParams['status_flags']) && $nextprev==true){
            $statusFlags = $queryParams['status_flags'];
        }else if($request->status_flags && is_array($request->status_flags)){
            $statusFlags = $request->status_flags;
        }else if(isset($queryParams['ConflictStatusFlag']) && $nextprev==true){
            $statusFlags = [$queryParams['ConflictStatusFlag']];
        }else if($request->ConflictStatusFlag){
            $statusFlags = [$request->ConflictStatusFlag];
        }
        
        if (!empty($statusFlags)) {
            if (in_array('R', $statusFlags)) {
                $statusFlags[] = 'D';
                $statusFlags = array_unique($statusFlags);
            }
            $statusFlagsList = "'" . implode("','", array_map('addslashes', $statusFlags)) . "'";
            $query .= " AND V2.\"StatusFlag\" IN ({$statusFlagsList})";
            $countquery .= " AND V2.\"StatusFlag\" IN ({$statusFlagsList})";
        }
        if(isset($queryParams['NoResponse']) && $nextprev==true){
            $NoResponse = $queryParams['NoResponse'];
        }else if($request->NoResponse){
            $NoResponse = $request->NoResponse;
        }else{
            $NoResponse = '';
        }
        if ($NoResponse) {
            if($NoResponse=='Yes'){
                $query .= " AND V2.\"NoResponseFlag\" = '$NoResponse'";
                $countquery .= " AND V2.\"NoResponseFlag\" = '$NoResponse'";
            }else{
                $query .= " AND V2.\"NoResponseFlag\" IS NULL";
                $countquery .= " AND V2.\"NoResponseFlag\" IS NULL";
            }
        }
        if(request()->route()->getName() == 'flag-for-review'){
            $FlagForReview = 'Yes';
        }else if(isset($queryParams['FlagForReview']) && $nextprev==true){
            $FlagForReview = $queryParams['FlagForReview'];
        }else if($request->FlagForReview){
            $FlagForReview = $request->FlagForReview;
        }else{
            $FlagForReview = '';
        }
        if ($FlagForReview) {
            if($FlagForReview=='Yes'){
                $query .= " AND V2.\"FlagForReview\" = '$FlagForReview'";
                $countquery .= " AND V2.\"FlagForReview\" = '$FlagForReview'";
            }else{
                $query .= " AND (V2.\"FlagForReview\" IS NULL OR V2.\"FlagForReview\" = 'No')";
                $countquery .= " AND (V2.\"FlagForReview\" IS NULL OR V2.\"FlagForReview\" = 'No')";
            }
        }
        if(isset($queryParams['CONFLICTID'])){
            $CONFLICTID = $queryParams['CONFLICTID'];
        }else if($request->CONFLICTID){
            $CONFLICTID = $request->CONFLICTID;
        }else{
            $CONFLICTID = '';
        }
        if ($CONFLICTID) {
            $query .= " AND V2.\"CONFLICTID\" = '$CONFLICTID'";
            $countquery .= " AND V2.\"CONFLICTID\" = '$CONFLICTID'";
        }
        if(isset($queryParams['RefConflictID'])){
            $RefConflictID = $queryParams['RefConflictID'];
        }else if($request->RefConflictID){
            $RefConflictID = $request->RefConflictID;
        }else{
            $RefConflictID = '';
        }
        if ($RefConflictID) {
            $query .= " AND V1.\"GroupID\" = '$RefConflictID'";
            $countquery .= " AND V1.\"GroupID\" = '$RefConflictID'";
        }
        if ($nextprev==true) {
            if($SortByField && $SortByAD){
                if($all=='Prev' && $SortByAD=='DESC'){
                    $query .= " AND V2.\"CONFLICTID\" > $CONFLICTIDCUR";
                    $countquery .= " AND V2.\"CONFLICTID\" > $CONFLICTIDCUR";
                }else if($all=='Next' && $SortByAD=='DESC'){
                    $query .= " AND V2.\"CONFLICTID\" < $CONFLICTIDCUR";
                    $countquery .= " AND V2.\"CONFLICTID\" < $CONFLICTIDCUR";
                }else if($all=='Prev' && $SortByAD=='ASC'){
                    $query .= " AND V2.\"CONFLICTID\" < $CONFLICTIDCUR";
                    $countquery .= " AND V2.\"CONFLICTID\" < $CONFLICTIDCUR";
                }else if($all=='Next' && $SortByAD=='ASC'){
                    $query .= " AND V2.\"CONFLICTID\" > $CONFLICTIDCUR";
                    $countquery .= " AND V2.\"CONFLICTID\" > $CONFLICTIDCUR";
                }
            }else{
                if($all=='Prev'){
                    $query .= " AND V2.\"CONFLICTID\" > $CONFLICTIDCUR";
                    $countquery .= " AND V2.\"CONFLICTID\" > $CONFLICTIDCUR";
                }else if($all=='Next'){
                    $query .= " AND V2.\"CONFLICTID\" < $CONFLICTIDCUR";
                    $countquery .= " AND V2.\"CONFLICTID\" < $CONFLICTIDCUR";
                }
            }
        }
        if(isset($queryParams['OverlapTimeMin']) && $nextprev==true){
            $OverlapTimeMin = $queryParams['OverlapTimeMin'];
        }else if($request->OverlapTimeMin){
            $OverlapTimeMin = $request->OverlapTimeMin;
        }else{
            $OverlapTimeMin = '';
        }
        if(isset($queryParams['OverlapTimeMax']) && $nextprev==true){
            $OverlapTimeMax = $queryParams['OverlapTimeMax'];
        }else if($request->OverlapTimeMax){
            $OverlapTimeMax = $request->OverlapTimeMax;
        }else{
            $OverlapTimeMax = '';
        }
        if ($OverlapTimeMin && $OverlapTimeMax) {
            $queryC = " AND (CASE
                WHEN V1.\"CShVTSTTime\" >= V1.\"ShVTSTTime\" AND V1.\"CShVTSTTime\" <= V1.\"ShVTENTime\" AND V1.\"CShVTENTime\" > V1.\"ShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"CShVTSTTime\", V1.\"ShVTENTime\")
                WHEN V1.\"ShVTSTTime\" >= V1.\"CShVTSTTime\" AND V1.\"ShVTSTTime\" <= V1.\"CShVTENTime\" AND V1.\"ShVTENTime\" > V1.\"CShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"ShVTSTTime\", V1.\"CShVTENTime\")
                WHEN V1.\"CShVTSTTime\" >= V1.\"ShVTSTTime\" AND V1.\"CShVTENTime\" <= V1.\"ShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"CShVTSTTime\", V1.\"CShVTENTime\")
                WHEN V1.\"ShVTSTTime\" >= V1.\"CShVTSTTime\" AND V1.\"ShVTENTime\" <= V1.\"CShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"ShVTSTTime\", V1.\"ShVTENTime\")
                WHEN V1.\"CShVTSTTime\" < V1.\"ShVTSTTime\" AND V1.\"CShVTENTime\" > V1.\"ShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"ShVTSTTime\", V1.\"ShVTENTime\")
                WHEN V1.\"ShVTSTTime\" < V1.\"CShVTSTTime\" AND V1.\"ShVTENTime\" > V1.\"CShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"CShVTSTTime\", V1.\"CShVTENTime\")
                ELSE 0
            END) BETWEEN '$OverlapTimeMin' AND '$OverlapTimeMax'";
            $query .= $queryC;
            $countquery .= $queryC;
        }else if (!$OverlapTimeMin && $OverlapTimeMax) {
            $OverlapTimeMin = 1;
            $queryC = " AND (CASE
                WHEN V1.\"CShVTSTTime\" >= V1.\"ShVTSTTime\" AND V1.\"CShVTSTTime\" <= V1.\"ShVTENTime\" AND V1.\"CShVTENTime\" > V1.\"ShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"CShVTSTTime\", V1.\"ShVTENTime\")
                WHEN V1.\"ShVTSTTime\" >= V1.\"CShVTSTTime\" AND V1.\"ShVTSTTime\" <= V1.\"CShVTENTime\" AND V1.\"ShVTENTime\" > V1.\"CShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"ShVTSTTime\", V1.\"CShVTENTime\")
                WHEN V1.\"CShVTSTTime\" >= V1.\"ShVTSTTime\" AND V1.\"CShVTENTime\" <= V1.\"ShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"CShVTSTTime\", V1.\"CShVTENTime\")
                WHEN V1.\"ShVTSTTime\" >= V1.\"CShVTSTTime\" AND V1.\"ShVTENTime\" <= V1.\"CShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"ShVTSTTime\", V1.\"ShVTENTime\")
                WHEN V1.\"CShVTSTTime\" < V1.\"ShVTSTTime\" AND V1.\"CShVTENTime\" > V1.\"ShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"ShVTSTTime\", V1.\"ShVTENTime\")
                WHEN V1.\"ShVTSTTime\" < V1.\"CShVTSTTime\" AND V1.\"ShVTENTime\" > V1.\"CShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"CShVTSTTime\", V1.\"CShVTENTime\")
                ELSE 0
            END) BETWEEN '$OverlapTimeMin' AND '$OverlapTimeMax'";
            $query .= $queryC;
            $countquery .= $queryC;
        }else if ($OverlapTimeMin && !$OverlapTimeMax) {
            $queryC = " AND (CASE
                WHEN V1.\"CShVTSTTime\" >= V1.\"ShVTSTTime\" AND V1.\"CShVTSTTime\" <= V1.\"ShVTENTime\" AND V1.\"CShVTENTime\" > V1.\"ShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"CShVTSTTime\", V1.\"ShVTENTime\")
                WHEN V1.\"ShVTSTTime\" >= V1.\"CShVTSTTime\" AND V1.\"ShVTSTTime\" <= V1.\"CShVTENTime\" AND V1.\"ShVTENTime\" > V1.\"CShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"ShVTSTTime\", V1.\"CShVTENTime\")
                WHEN V1.\"CShVTSTTime\" >= V1.\"ShVTSTTime\" AND V1.\"CShVTENTime\" <= V1.\"ShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"CShVTSTTime\", V1.\"CShVTENTime\")
                WHEN V1.\"ShVTSTTime\" >= V1.\"CShVTSTTime\" AND V1.\"ShVTENTime\" <= V1.\"CShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"ShVTSTTime\", V1.\"ShVTENTime\")
                WHEN V1.\"CShVTSTTime\" < V1.\"ShVTSTTime\" AND V1.\"CShVTENTime\" > V1.\"ShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"ShVTSTTime\", V1.\"ShVTENTime\")
                WHEN V1.\"ShVTSTTime\" < V1.\"CShVTSTTime\" AND V1.\"ShVTENTime\" > V1.\"CShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"CShVTSTTime\", V1.\"CShVTENTime\")
                ELSE 0
            END) >= '$OverlapTimeMin'";
            $query .= $queryC;
            $countquery .= $queryC;
        }
        if(isset($queryParams['PayerID']) && $nextprev==true){
            $PayerIDAppID = $queryParams['PayerID'];
        }else if($request->PayerID){
            $PayerIDAppID = $request->PayerID;
        }else{
            $PayerIDAppID = '';
        }
        if ($PayerIDAppID) {
            $PayerID = '-999';
            $ApplicationPayerId = '-999';
            if(!empty($PayerIDAppID)){
                $PayerIDAppIDArr = explode('~', $PayerIDAppID);
                if(!empty($PayerIDAppIDArr) && sizeof($PayerIDAppIDArr)==2){
                    $PayerID = $PayerIDAppIDArr[0];
                    $ApplicationPayerId = $PayerIDAppIDArr[1];
                }else if(!empty($PayerIDAppIDArr) && sizeof($PayerIDAppIDArr)==1){
                    $PayerID = $PayerIDAppIDArr[0];
                }
            }
            $query .= " AND V1.\"PayerID\" = '$PayerID'";
            $countquery .= " AND V1.\"PayerID\" = '$PayerID'";
        }
        if(isset($queryParams['ConPayerID']) && $nextprev==true){
            $ConPayerIDAppID = $queryParams['ConPayerID'];
        }else if($request->ConPayerID){
            $ConPayerIDAppID = $request->ConPayerID;
        }else{
            $ConPayerIDAppID = '';
        }
        if ($ConPayerIDAppID) {
            $ConPayerID = '-999';
            $ApplicationConPayerId = '-999';
            if(!empty($ConPayerIDAppID)){
                $ConPayerIDAppIDArr = explode('~', $ConPayerIDAppID);
                if(!empty($ConPayerIDAppIDArr) && sizeof($ConPayerIDAppIDArr)==2){
                    $ConPayerID = $ConPayerIDAppIDArr[0];
                    $ApplicationConPayerId = $ConPayerIDAppIDArr[1];
                }else if(!empty($ConPayerIDAppIDArr) && sizeof($ConPayerIDAppIDArr)==1){
                    $ConPayerID = $ConPayerIDAppIDArr[0];
                }
            }
            $query .= " AND V1.\"ConPayerID\" = '$ConPayerID'";
            $countquery .= " AND V1.\"ConPayerID\" = '$ConPayerID'";
        }
        if(isset($queryParams['OfficeID']) && $nextprev==true){
            $OfficeIDAppID = $queryParams['OfficeID'];
        }else if($request->OfficeID){
            $OfficeIDAppID = $request->OfficeID;
        }else{
            $OfficeIDAppID = '';
        }
        if ($OfficeIDAppID) {
            $OfficeID = '-999';
            $ApplicationOfficeID = '-999';
            if(!empty($OfficeIDAppID)){
                $OfficeIDAppIDArr = explode('~', $OfficeIDAppID);
                if(!empty($OfficeIDAppIDArr) && sizeof($OfficeIDAppIDArr)==2){
                    $OfficeID = $OfficeIDAppIDArr[0];
                    $ApplicationOfficeID = $OfficeIDAppIDArr[1];
                }else if(!empty($OfficeIDAppIDArr) && sizeof($OfficeIDAppIDArr)==1){
                    $OfficeID = $OfficeIDAppIDArr[0];
                }
            }
            $query .= " AND V1.\"OfficeID\" = '$OfficeID'";
            $countquery .= " AND V1.\"OfficeID\" = '$OfficeID'";
        }
        if(isset($queryParams['ConProviderID']) && $nextprev==true){
            $ConProviderIDAppID = $queryParams['ConProviderID'];
        }else if($request->ConProviderID){
            $ConProviderIDAppID = $request->ConProviderID;
        }else{
            $ConProviderIDAppID = '';
        }
        if ($ConProviderIDAppID) {
            $ConProviderID = '-999';
            $ApplicationConProviderID = '-999';
            if(!empty($ConProviderIDAppID)){
                $ConProviderIDAppIDArr = explode('~', $ConProviderIDAppID);
                if(!empty($ConProviderIDAppIDArr) && sizeof($ConProviderIDAppIDArr)==2){
                    $ConProviderID = $ConProviderIDAppIDArr[0];
                    $ApplicationConProviderID = $ConProviderIDAppIDArr[1];
                }else if(!empty($ConProviderIDAppIDArr) && sizeof($ConProviderIDAppIDArr)==1){
                    $ConProviderID = $ConProviderIDAppIDArr[0];
                }
            }
            $query .= " AND V1.\"ConProviderID\" = '$ConProviderID'";
            $countquery .= " AND V1.\"ConProviderID\" = '$ConProviderID'";
        }
        if(isset($queryParams['ProviderID']) && $nextprev==true){
            $ProviderIDAppID = $queryParams['ProviderID'];
        }else if($request->ProviderID){
            $ProviderIDAppID = $request->ProviderID;
        }else{
            $ProviderIDAppID = '';
        }
        if ($ProviderIDAppID) {
            $PProviderID = '-999';
            $PApplicationProviderID = '-999';
            if(!empty($ProviderIDAppID)){
                $ProviderIDAppIDArr = explode('~', $ProviderIDAppID);
                if(!empty($ProviderIDAppIDArr) && sizeof($ProviderIDAppIDArr)==2){
                    $PProviderID = $ProviderIDAppIDArr[0];
                    $PApplicationProviderID = $ProviderIDAppIDArr[1];
                }else if(!empty($ProviderIDAppIDArr) && sizeof($ProviderIDAppIDArr)==1){
                    $PProviderID = $ProviderIDAppIDArr[0];
                }
            }
            $query .= " AND V1.\"ProviderID\" = '$PProviderID'";
            $countquery .= " AND V1.\"ProviderID\" = '$PProviderID'";
        }
        if(isset($queryParams['ConflictType']) && $nextprev==true){
            $ConflictType = $queryParams['ConflictType'];
        }else if($request->ConflictType){
            $ConflictType = $request->ConflictType;
        }else{
            $ConflictType = '';
        }
        if ($ConflictType) {
            if($ConflictType==1){//Same Sch Time
                $query .= " AND V1.\"SameSchTimeFlag\" = 'Y'";
                $countquery .= " AND V1.\"SameSchTimeFlag\" = 'Y'";
            }else if($ConflictType==2){//Same Visit Time
                $query .= " AND V1.\"SameVisitTimeFlag\" = 'Y'";
                $countquery .= " AND V1.\"SameVisitTimeFlag\" = 'Y'";
            }else if($ConflictType==3){//Sch And Visit Time Same
                $query .= " AND V1.\"SchAndVisitTimeSameFlag\" = 'Y'";
                $countquery .= " AND V1.\"SchAndVisitTimeSameFlag\" = 'Y'";
            }else if($ConflictType==4){//Sch Over Another Sch Time
                $query .= " AND V1.\"SchOverAnotherSchTimeFlag\" = 'Y'";
                $countquery .= " AND V1.\"SchOverAnotherSchTimeFlag\" = 'Y'";
            }else if($ConflictType==5){//Visit Time Over Another Visit Time Type
                $query .= " AND V1.\"VisitTimeOverAnotherVisitTimeFlag\" = 'Y'";
                $countquery .= " AND V1.\"VisitTimeOverAnotherVisitTimeFlag\" = 'Y'";
            }else if($ConflictType==6){//Sch Time Over Visit Time
                $query .= " AND V1.\"SchTimeOverVisitTimeFlag\" = 'Y'";
                $countquery .= " AND V1.\"SchTimeOverVisitTimeFlag\" = 'Y'";
            }else if($ConflictType==7){//Distance
                $query .= " AND V1.\"DistanceFlag\" = 'Y'";
                $countquery .= " AND V1.\"DistanceFlag\" = 'Y'";
            }else if($ConflictType==8){//In-Service
                $query .= " AND V1.\"InServiceFlag\" = 'Y'";
                $countquery .= " AND V1.\"InServiceFlag\" = 'Y'";
            }else if($ConflictType==9){//PTO
                $query .= " AND V1.\"PTOFlag\" = 'Y'";
                $countquery .= " AND V1.\"PTOFlag\" = 'Y'";
            }
            // else{
            //     $query .= " AND (V1.\"SchOverAnotherSchTimeFlag\" = 'Y' OR V1.\"VisitTimeOverAnotherVisitTimeFlag\" = 'Y')";
            //     $countquery .= " AND (V1.\"SchOverAnotherSchTimeFlag\" = 'Y' OR V1.\"VisitTimeOverAnotherVisitTimeFlag\" = 'Y')"; 
            // }
        }
        // else{
        //     $query .= " AND (V1.\"SchOverAnotherSchTimeFlag\" = 'Y' OR V1.\"VisitTimeOverAnotherVisitTimeFlag\" = 'Y')";
        //     $countquery .= " AND (V1.\"SchOverAnotherSchTimeFlag\" = 'Y' OR V1.\"VisitTimeOverAnotherVisitTimeFlag\" = 'Y')"; 
        // }
        if(isset($queryParams['VisitStartDate']) && $nextprev==true){
            $VisitStartDate = $queryParams['VisitStartDate'];
        }else if($request->VisitStartDate){
            $VisitStartDate = $request->VisitStartDate;
        }else{
            $VisitStartDate = '';
        }
        if(isset($queryParams['VisitEndDate']) && $nextprev==true){
            $VisitEndDate = $queryParams['VisitEndDate'];
        }else if($request->VisitEndDate){
            $VisitEndDate = $request->VisitEndDate;
        }else{
            $VisitEndDate = '';
        }
        if ($VisitStartDate && $VisitEndDate) {
            $query .= " AND V1.\"VisitDate\" BETWEEN '$VisitStartDate' AND '$VisitEndDate'";
            $countquery .= " AND V1.\"VisitDate\" BETWEEN '$VisitStartDate' AND '$VisitEndDate'";
        }else if ($VisitStartDate && !$VisitEndDate) {
            $query .= " AND V1.\"VisitDate\" >= '$VisitStartDate'";
            $countquery .= " AND V1.\"VisitDate\" >= '$VisitStartDate'";
        }else if (!$VisitStartDate && $VisitEndDate) {
            $query .= " AND V1.\"VisitDate\" <= '$VisitEndDate'";
            $countquery .= " AND V1.\"VisitDate\" <= '$VisitEndDate'";
        }
        if(isset($queryParams['BilledStartDate']) && $nextprev==true){
            $BilledStartDate = $queryParams['BilledStartDate'];
        }else if($request->BilledStartDate){
            $BilledStartDate = $request->BilledStartDate;
        }else{
            $BilledStartDate = '';
        }
        if(isset($queryParams['BilledEndDate']) && $nextprev==true){
            $BilledEndDate = $queryParams['BilledEndDate'];
        }else if($request->BilledEndDate){
            $BilledEndDate = $request->BilledEndDate;
        }else{
            $BilledEndDate = '';
        }
        if ($BilledStartDate && $BilledEndDate) {
            $query .= " AND TO_CHAR(V1.\"BilledDate\", 'YYYY-MM-DD') BETWEEN '$BilledStartDate' AND '$BilledEndDate'";
            $countquery .= " AND TO_CHAR(V1.\"BilledDate\", 'YYYY-MM-DD') BETWEEN '$BilledStartDate' AND '$BilledEndDate'";
        }else if ($BilledStartDate && !$BilledEndDate) {
            $query .= " AND TO_CHAR(V1.\"BilledDate\", 'YYYY-MM-DD') >= '$BilledStartDate'";
            $countquery .= " AND TO_CHAR(V1.\"BilledDate\", 'YYYY-MM-DD') >= '$BilledStartDate'";
        }else if (!$BilledStartDate && $BilledEndDate) {
            $query .= " AND TO_CHAR(V1.\"BilledDate\", 'YYYY-MM-DD') <= '$BilledEndDate'";
            $countquery .= " AND TO_CHAR(V1.\"BilledDate\", 'YYYY-MM-DD') <= '$BilledEndDate'";
        }
        //CReportedStartDate=2024-06-05&CReportedEndDate
        if(isset($queryParams['CReportedStartDate']) && $nextprev==true){
            $CReportedStartDate = $queryParams['CReportedStartDate'];
        }else if($request->CReportedStartDate){
            $CReportedStartDate = $request->CReportedStartDate;
        }else{
            $CReportedStartDate = '';
        }
        if(isset($queryParams['CReportedEndDate']) && $nextprev==true){
            $CReportedEndDate = $queryParams['CReportedEndDate'];
        }else if($request->CReportedEndDate){
            $CReportedEndDate = $request->CReportedEndDate;
        }else{
            $CReportedEndDate = '';
        }
        if ($CReportedStartDate && $CReportedEndDate) {
            $query .= " AND TO_CHAR(V1.\"CRDATEUNIQUE\", 'YYYY-MM-DD') BETWEEN '$CReportedStartDate' AND '$CReportedEndDate'";
            $countquery .= " AND TO_CHAR(V1.\"CRDATEUNIQUE\", 'YYYY-MM-DD') BETWEEN '$CReportedStartDate' AND '$CReportedEndDate'";
        }else if ($CReportedStartDate && !$CReportedEndDate) {
            $query .= " AND TO_CHAR(V1.\"CRDATEUNIQUE\", 'YYYY-MM-DD') >= '$CReportedStartDate'";
            $countquery .= " AND TO_CHAR(V1.\"CRDATEUNIQUE\", 'YYYY-MM-DD') >= '$CReportedStartDate'";
        }else if (!$CReportedStartDate && $CReportedEndDate) {
            $query .= " AND TO_CHAR(V1.\"CRDATEUNIQUE\", 'YYYY-MM-DD') <= '$CReportedEndDate'";
            $countquery .= " AND TO_CHAR(V1.\"CRDATEUNIQUE\", 'YYYY-MM-DD') <= '$CReportedEndDate'";
        }
        if(isset($queryParams['AgingDays']) && $nextprev==true){
            $AgingDays = $queryParams['AgingDays'];
        }else if($request->AgingDays){
            $AgingDays = $request->AgingDays;
        }else{
            $AgingDays = '';
        }
        if ($AgingDays) {
            $AgingDays = is_numeric($AgingDays) ? $AgingDays : -99;
            $query .= " AND DATEDIFF(DAY, V1.\"CRDATEUNIQUE\", GETDATE()) = '$AgingDays'";
            $countquery .= " AND DATEDIFF(DAY, V1.\"CRDATEUNIQUE\", GETDATE()) = '$AgingDays'";
        }
        /*
        DESC
        IF GET PREV RECORD THEN SET DESC TO ASC AND POINTER SET TO > 3
        IF GET NEXT RECORD THEN SET DESC TO DESC AND POINTER SET TO < 3

        ASC
        IF GET PREV RECORD THEN SET ASC TO DESC AND POINTER SET TO < 3
        IF GET NEXT RECORD THEN SET ASC TO ASC AND POINTER SET TO > 3
        */
        if($SortByField && $SortByAD){
            if($all=='Prev' && $SortByAD=='DESC'){
                $sortby = 'ASC';
            }else if($all=='Next' && $SortByAD=='DESC'){
                $sortby = 'DESC';
            }else if($all=='Prev' && $SortByAD=='ASC'){
                $sortby = 'DESC';
            }else if($all=='Next' && $SortByAD=='ASC'){
                $sortby = 'ASC';
            }else{                
                $sortby = $SortByAD;
            }
            if($SortByField=='ParentStatusFlag'){
                $query .= " ORDER BY CASE
                    WHEN V2.\"StatusFlag\" IN ('D', 'R') THEN 'R'
                    ELSE V2.\"StatusFlag\"
                END ".$sortby."";
            }else if($SortByField=='ScheduleHours'){
                $query .= " ORDER BY (TIMESTAMPDIFF(MINUTE, V1.\"SchStartTime\", V1.\"SchEndTime\") / 60) ".$sortby."";
            }else{
                $query .= " ORDER BY ".$SortByField." ".$sortby."";
            }
        }else{
            if($all=='Prev'){
                $sortby = 'ASC';
            }else if($all=='Next'){
                $sortby = 'DESC';
            }else{
                $sortby = 'DESC';
            }
            $query .= " ORDER BY V1.\"CONFLICTID\" ".$sortby."";
        }
        if(!in_array($all, ['-1', 'Next', 'Prev']))
        {
            $query .= " LIMIT $perPage OFFSET $offset";
        }
        if($request->debug){
            echo $query;
            die;
        }
        $statement = $this->conn->prepare($query);
        if($nextprev==true){
            if($request->debug1){
                echo $query;
                die;
            }
            $results = $statement->fetch(PDO::FETCH_ASSOC);
            return !empty($results) ? convertToSslUrl(route('conflict-detail', ['CONFLICTID' => $results['CONFLICTID']])) : '';
        }
        $results = $statement->fetchAll(PDO::FETCH_ASSOC);
        if($all == '-1')
        {
            return $results;
        }
        $statement_count = $this->conn->prepare($countquery);        
        $total_results = $statement_count->fetch(PDO::FETCH_ASSOC);
        $rowCount = $total_results['count'];        
        $paginatedResults = new \Illuminate\Pagination\LengthAwarePaginator(
            $results,
            $rowCount,
            $perPage,
            $currentPage,
            ['path' => request()->url(), 'query' => request()->query()]
        );
        return $paginatedResults;
    }

    public function getVisitsExport($queryParams, $ProviderID, $AppProviderID, $currentPage=1, $perPage=5000){
        // Calculate the offset
        $offset = ($currentPage - 1) * $perPage;
        $sortableLinks = [
            'conflictid' => 'V1."CONFLICTID"',
            'office' => 'V1."Office"',
            'contract' => 'V1."Contract"',
            'parentstatusflag' => 'ParentStatusFlag',
            'aidecode' => 'V1."AideCode"',
            'aidelname' => 'V1."AideLName"',
            'aidefname' => 'V1."AideFName"',
            'ssn' => 'COALESCE(V1."AideSSN", V1."SSN")',
            'padmissionid' => 'V1."P_PAdmissionID"',
            'plname' => 'V1."P_PLName"',
            'pfname' => 'V1."P_PFName"',
            'pmedicaidnumber' => 'V1."P_PMedicaidNumber"',
            'visitdate' => 'V1."VisitDate"',
            'billeddate' => 'V1."BilledDate"',
            'schedulehours' => 'ScheduleHours',
            'billedhours' => 'V1."BilledHours"'
        ];
        $sortableLinksAD = [
            'asc' => 'asc',
            'desc' => 'desc'
        ];
        if(isset($queryParams['sort']) && isset($sortableLinks[strtolower($queryParams['sort'])])){
            $SortByField = $sortableLinks[strtolower($queryParams['sort'])];
        }else{
            $SortByField = 'V1."CONFLICTID"';
        }
        if(isset($queryParams['direction']) && isset($sortableLinks[strtolower($queryParams['direction'])])){
            $SortByAD = $sortableLinksAD[strtolower($queryParams['direction'])];
        }else{
            $SortByAD = 'DESC';
        }
        $query = "SELECT DISTINCT V1.\"CONFLICTID\", V1.\"SSN\", V1.\"ProviderID\", V1.\"AppProviderID\", V1.\"ProviderName\", V1.\"VisitID\", V1.\"AppVisitID\", V1.\"VisitDate\", V1.\"SchStartTime\", V1.\"SchEndTime\", V1.\"VisitStartTime\", V1.\"VisitEndTime\", V1.\"EVVStartTime\", V1.\"EVVEndTime\", V1.\"CaregiverID\", V1.\"AppCaregiverID\", V1.\"AideCode\", V1.\"AideName\", V1.\"AideFName\", V1.\"AideLName\", COALESCE(V1.\"AideSSN\", V1.\"SSN\") AS \"AideSSN\", V1.\"OfficeID\", V1.\"AppOfficeID\", V1.\"Office\",

        V1.\"P_PatientID\", V1.\"P_AppPatientID\", V1.\"P_PAdmissionID\", V1.\"P_PName\", V1.\"P_PFName\", V1.\"P_PLName\", V1.\"P_PMedicaidNumber\", V1.\"P_PAddressID\", V1.\"P_PAppAddressID\", V1.\"P_PAddressL1\", V1.\"P_PAddressL2\", V1.\"P_PCity\", V1.\"P_PAddressState\", V1.\"P_PZipCode\", V1.\"P_PCounty\",
        
        V1.\"PLongitude\", V1.\"PLatitude\", V1.\"PayerID\", V1.\"AppPayerID\", V1.\"Contract\", V1.\"BilledDate\", V1.\"BilledHours\", V1.\"Billed\", V1.\"ServiceCodeID\", V1.\"AppServiceCodeID\", V1.\"RateType\", V1.\"ServiceCode\", TIMESTAMPDIFF(MINUTE, V1.\"SchStartTime\", V1.\"SchEndTime\") / 60 AS \"sch_hours\",
        V1.\"RateType\",      
        CASE
           WHEN V2.\"StatusFlag\" IN ('D', 'R') THEN 'R'
           ELSE V2.\"StatusFlag\"
        END AS \"ParentStatusFlag\",
        V2.\"StatusFlag\" AS \"OrgParentStatusFlag\",
        V2.\"NoResponseFlag\",
        CONCAT(V1.\"VisitID\", '~',V1.\"AppVisitID\") as \"VAPPID\",        
        CONCAT(V1.\"VisitID\", '~',V1.\"AppVisitID\") as \"APatientAPPID\",
        DATEDIFF(DAY, V1.\"CRDATEUNIQUE\", GETDATE()) AS \"AgeInDays\",
        CASE 
            WHEN DATEDIFF(DAY, V1.\"CRDATEUNIQUE\", GETDATE()) > SETT.NORESPONSELIMITTIME THEN TRUE
            ELSE FALSE
        END AS ALLOWDELETE
         FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS V1 INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTS AS V2 ON V2.\"CONFLICTID\"=V1.\"CONFLICTID\" CROSS JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.\"SETTINGS\" AS SETT";
        $countquery = "SELECT COUNT(DISTINCT V1.\"CONFLICTID\") AS \"count\" FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS V1 INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTS AS V2 ON V2.\"CONFLICTID\"=V1.\"CONFLICTID\"";
        $query .= " WHERE V1.\"ProviderID\" = '".$ProviderID."'";
        $countquery .= " WHERE V1.\"ProviderID\" = '".$ProviderID."'";
        
        if(isset($queryParams['guid']) && !empty($queryParams['guid'])){
            if($ofcquery = ofcquery($queryParams['guid'])){
                $query .= " AND V1.\"OfficeID\" IN (".$ofcquery.")";
                $countquery .= " AND V1.\"OfficeID\" IN (".$ofcquery.")";
            }else{                
                $query .= " AND V1.\"OfficeID\" IN ('-999')";
                $countquery .= " AND V1.\"OfficeID\" IN ('-999')";
            }
        }else{
            $query .= " AND V1.\"OfficeID\" IN ('-999')";
            $countquery .= " AND V1.\"OfficeID\" IN ('-999')";
        }
        // $query .= " AND CONCAT(\"P_PatientID\", '~', \"P_AppPatientID\") != '00000000-0000-0000-0000-db4ceb2647f0~0'";
        // $countquery .= " AND CONCAT(\"P_PatientID\", '~', \"P_AppPatientID\") != '00000000-0000-0000-0000-db4ceb2647f0~0'";
        if(isset($queryParams['PAdmissionID'])){
            $PAdmissionID = $queryParams['PAdmissionID'];
        }else{
            $PAdmissionID = '';
        }
        if ($PAdmissionID) {
            $query .= " AND V1.\"P_PAdmissionID\" ILIKE '%$PAdmissionID%'";
            $countquery .= " AND V1.\"P_PAdmissionID\" ILIKE '%$PAdmissionID%'";
        }
        if(isset($queryParams['MedicaidID'])){
            $MedicaidID = $queryParams['MedicaidID'];
        }else{
            $MedicaidID = '';
        }
        if ($MedicaidID) {
            $query .= " AND V1.\"P_PMedicaidNumber\" ILIKE '%$MedicaidID%'";
            $countquery .= " AND V1.\"P_PMedicaidNumber\" ILIKE '%$MedicaidID%'";
        }
        if(isset($queryParams['PLName'])){
            $PLName = $queryParams['PLName'];
        }else{
            $PLName = '';
        }
        if ($PLName) {
            $query .= " AND V1.\"P_PLName\" ILIKE '%$PLName%'";
            $countquery .= " AND V1.\"P_PLName\" ILIKE '%$PLName%'";
        }
        if(isset($queryParams['PFName'])){
            $PFName = $queryParams['PFName'];
        }else{
            $PFName = '';
        }
        if ($PFName) {
            $query .= " AND V1.\"P_PFName\" ILIKE '%$PFName%'";
            $countquery .= " AND V1.\"P_PFName\" ILIKE '%$PFName%'";
        }
        if(isset($queryParams['AideCode'])){
            $AideCode = $queryParams['AideCode'];
        }else{
            $AideCode = '';
        }
        if ($AideCode) {
            $query .= " AND V1.\"AideCode\" ILIKE '%$AideCode%'";
            $countquery .= " AND V1.\"AideCode\" ILIKE '%$AideCode%'";
        }
        if(isset($queryParams['AideLName'])){
            $AideLName = $queryParams['AideLName'];
        }else{
            $AideLName = '';
        }
        if ($AideLName) {
            $query .= " AND V1.\"AideLName\" ILIKE '%$AideLName%'";
            $countquery .= " AND V1.\"AideLName\" ILIKE '%$AideLName%'";
        }
        if(isset($queryParams['AideFName'])){
            $AideFName = $queryParams['AideFName'];
        }else{
            $AideFName = '';
        }
        if ($AideFName) {
            $query .= " AND V1.\"AideFName\" ILIKE '%$AideFName%'";
            $countquery .= " AND V1.\"AideFName\" ILIKE '%$AideFName%'";
        }
        $statusFlags = [];
        if (!empty($queryParams['status_flags']) && is_array($queryParams['status_flags'])) {
            $statusFlags = $queryParams['status_flags'];
        } elseif (!empty($queryParams['ConflictStatusFlag'])) {
            $statusFlags = [$queryParams['ConflictStatusFlag']];
        }
        
        if (!empty($statusFlags)) {
            if (in_array('R', $statusFlags)) {
                $statusFlags[] = 'D';
                $statusFlags = array_unique($statusFlags);
            }
            $statusFlagsList = "'" . implode("','", array_map('addslashes', $statusFlags)) . "'";
            $query .= " AND V2.\"StatusFlag\" IN ({$statusFlagsList})";
            $countquery .= " AND V2.\"StatusFlag\" IN ({$statusFlagsList})";
        }
        if(isset($queryParams['NoResponse'])){
            $NoResponse = $queryParams['NoResponse'];
        }else{
            $NoResponse = '';
        }
        if ($NoResponse) {
            if($NoResponse=='Yes'){
                $query .= " AND V2.\"NoResponseFlag\" = '$NoResponse'";
                $countquery .= " AND V2.\"NoResponseFlag\" = '$NoResponse'";
            }else{
                $query .= " AND V2.\"NoResponseFlag\" IS NULL";
                $countquery .= " AND V2.\"NoResponseFlag\" IS NULL";
            }
        }
        if(isset($queryParams['FlagForReview'])){
            $FlagForReview = $queryParams['FlagForReview'];
        }else{
            $FlagForReview = '';
        }
        if ($FlagForReview) {
            if($FlagForReview=='Yes'){
                $query .= " AND V2.\"FlagForReview\" = '$FlagForReview'";
                $countquery .= " AND V2.\"FlagForReview\" = '$FlagForReview'";
            }else{
                $query .= " AND V2.\"FlagForReview\" IS NULL";
                $countquery .= " AND V2.\"FlagForReview\" IS NULL";
            }
        }
        if(isset($queryParams['CONFLICTID'])){
            $CONFLICTID = $queryParams['CONFLICTID'];
        }else{
            $CONFLICTID = '';
        }
        if ($CONFLICTID) {
            $query .= " AND V2.\"CONFLICTID\" = '$CONFLICTID'";
            $countquery .= " AND V2.\"CONFLICTID\" = '$CONFLICTID'";
        }
        if(isset($queryParams['OverlapTimeMin'])){
            $OverlapTimeMin = $queryParams['OverlapTimeMin'];
        }else{
            $OverlapTimeMin = '';
        }
        if(isset($queryParams['OverlapTimeMax'])){
            $OverlapTimeMax = $queryParams['OverlapTimeMax'];
        }else{
            $OverlapTimeMax = '';
        }
        if ($OverlapTimeMin && $OverlapTimeMax) {
            $queryC = " AND (CASE
                WHEN V1.\"CShVTSTTime\" >= V1.\"ShVTSTTime\" AND V1.\"CShVTSTTime\" <= V1.\"ShVTENTime\" AND V1.\"CShVTENTime\" > V1.\"ShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"CShVTSTTime\", V1.\"ShVTENTime\")
                WHEN V1.\"ShVTSTTime\" >= V1.\"CShVTSTTime\" AND V1.\"ShVTSTTime\" <= V1.\"CShVTENTime\" AND V1.\"ShVTENTime\" > V1.\"CShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"ShVTSTTime\", V1.\"CShVTENTime\")
                WHEN V1.\"CShVTSTTime\" >= V1.\"ShVTSTTime\" AND V1.\"CShVTENTime\" <= V1.\"ShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"CShVTSTTime\", V1.\"CShVTENTime\")
                WHEN V1.\"ShVTSTTime\" >= V1.\"CShVTSTTime\" AND V1.\"ShVTENTime\" <= V1.\"CShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"ShVTSTTime\", V1.\"ShVTENTime\")
                WHEN V1.\"CShVTSTTime\" < V1.\"ShVTSTTime\" AND V1.\"CShVTENTime\" > V1.\"ShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"ShVTSTTime\", V1.\"ShVTENTime\")
                WHEN V1.\"ShVTSTTime\" < V1.\"CShVTSTTime\" AND V1.\"ShVTENTime\" > V1.\"CShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"CShVTSTTime\", V1.\"CShVTENTime\")
                ELSE 0
            END) BETWEEN '$OverlapTimeMin' AND '$OverlapTimeMax'";
            $query .= $queryC;
            $countquery .= $queryC;
        }else if (!$OverlapTimeMin && $OverlapTimeMax) {
            $OverlapTimeMin = 1;
            $queryC = " AND (CASE
                WHEN V1.\"CShVTSTTime\" >= V1.\"ShVTSTTime\" AND V1.\"CShVTSTTime\" <= V1.\"ShVTENTime\" AND V1.\"CShVTENTime\" > V1.\"ShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"CShVTSTTime\", V1.\"ShVTENTime\")
                WHEN V1.\"ShVTSTTime\" >= V1.\"CShVTSTTime\" AND V1.\"ShVTSTTime\" <= V1.\"CShVTENTime\" AND V1.\"ShVTENTime\" > V1.\"CShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"ShVTSTTime\", V1.\"CShVTENTime\")
                WHEN V1.\"CShVTSTTime\" >= V1.\"ShVTSTTime\" AND V1.\"CShVTENTime\" <= V1.\"ShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"CShVTSTTime\", V1.\"CShVTENTime\")
                WHEN V1.\"ShVTSTTime\" >= V1.\"CShVTSTTime\" AND V1.\"ShVTENTime\" <= V1.\"CShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"ShVTSTTime\", V1.\"ShVTENTime\")
                WHEN V1.\"CShVTSTTime\" < V1.\"ShVTSTTime\" AND V1.\"CShVTENTime\" > V1.\"ShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"ShVTSTTime\", V1.\"ShVTENTime\")
                WHEN V1.\"ShVTSTTime\" < V1.\"CShVTSTTime\" AND V1.\"ShVTENTime\" > V1.\"CShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"CShVTSTTime\", V1.\"CShVTENTime\")
                ELSE 0
            END) BETWEEN '$OverlapTimeMin' AND '$OverlapTimeMax'";
            $query .= $queryC;
            $countquery .= $queryC;
        }else if ($OverlapTimeMin && !$OverlapTimeMax) {
            $queryC = " AND (CASE
                WHEN V1.\"CShVTSTTime\" >= V1.\"ShVTSTTime\" AND V1.\"CShVTSTTime\" <= V1.\"ShVTENTime\" AND V1.\"CShVTENTime\" > V1.\"ShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"CShVTSTTime\", V1.\"ShVTENTime\")
                WHEN V1.\"ShVTSTTime\" >= V1.\"CShVTSTTime\" AND V1.\"ShVTSTTime\" <= V1.\"CShVTENTime\" AND V1.\"ShVTENTime\" > V1.\"CShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"ShVTSTTime\", V1.\"CShVTENTime\")
                WHEN V1.\"CShVTSTTime\" >= V1.\"ShVTSTTime\" AND V1.\"CShVTENTime\" <= V1.\"ShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"CShVTSTTime\", V1.\"CShVTENTime\")
                WHEN V1.\"ShVTSTTime\" >= V1.\"CShVTSTTime\" AND V1.\"ShVTENTime\" <= V1.\"CShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"ShVTSTTime\", V1.\"ShVTENTime\")
                WHEN V1.\"CShVTSTTime\" < V1.\"ShVTSTTime\" AND V1.\"CShVTENTime\" > V1.\"ShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"ShVTSTTime\", V1.\"ShVTENTime\")
                WHEN V1.\"ShVTSTTime\" < V1.\"CShVTSTTime\" AND V1.\"ShVTENTime\" > V1.\"CShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"CShVTSTTime\", V1.\"CShVTENTime\")
                ELSE 0
            END) >= '$OverlapTimeMin'";
            $query .= $queryC;
            $countquery .= $queryC;
        }
        if(isset($queryParams['PayerID'])){
            $PayerIDAppID = $queryParams['PayerID'];
        }else{
            $PayerIDAppID = '';
        }
        if ($PayerIDAppID) {
            $PayerID = '-999';
            $ApplicationPayerId = '-999';
            if(!empty($PayerIDAppID)){
                $PayerIDAppIDArr = explode('~', $PayerIDAppID);
                if(!empty($PayerIDAppIDArr) && sizeof($PayerIDAppIDArr)==2){
                    $PayerID = $PayerIDAppIDArr[0];
                    $ApplicationPayerId = $PayerIDAppIDArr[1];
                }else if(!empty($PayerIDAppIDArr) && sizeof($PayerIDAppIDArr)==1){
                    $PayerID = $PayerIDAppIDArr[0];
                }
            }
            $query .= " AND V1.\"PayerID\" = '$PayerID'";
            $countquery .= " AND V1.\"PayerID\" = '$PayerID'";
        }
        if(isset($queryParams['ConPayerID'])){
            $ConPayerIDAppID = $queryParams['ConPayerID'];
        }else{
            $ConPayerIDAppID = '';
        }
        if ($ConPayerIDAppID) {
            $ConPayerID = '-999';
            $ApplicationConPayerId = '-999';
            if(!empty($ConPayerIDAppID)){
                $ConPayerIDAppIDArr = explode('~', $ConPayerIDAppID);
                if(!empty($ConPayerIDAppIDArr) && sizeof($ConPayerIDAppIDArr)==2){
                    $ConPayerID = $ConPayerIDAppIDArr[0];
                    $ApplicationConPayerId = $ConPayerIDAppIDArr[1];
                }else if(!empty($ConPayerIDAppIDArr) && sizeof($ConPayerIDAppIDArr)==1){
                    $ConPayerID = $ConPayerIDAppIDArr[0];
                }
            }
            $query .= " AND V1.\"ConPayerID\" = '$ConPayerID'";
            $countquery .= " AND V1.\"ConPayerID\" = '$ConPayerID'";
        }
        if(isset($queryParams['OfficeID'])){
            $OfficeIDAppID = $queryParams['OfficeID'];
        }else{
            $OfficeIDAppID = '';
        }
        if ($OfficeIDAppID) {
            $OfficeID = '-999';
            $ApplicationOfficeID = '-999';
            if(!empty($OfficeIDAppID)){
                $OfficeIDAppIDArr = explode('~', $OfficeIDAppID);
                if(!empty($OfficeIDAppIDArr) && sizeof($OfficeIDAppIDArr)==2){
                    $OfficeID = $OfficeIDAppIDArr[0];
                    $ApplicationOfficeID = $OfficeIDAppIDArr[1];
                }else if(!empty($OfficeIDAppIDArr) && sizeof($OfficeIDAppIDArr)==1){
                    $OfficeID = $OfficeIDAppIDArr[0];
                }
            }
            $query .= " AND V1.\"OfficeID\" = '$OfficeID'";
            $countquery .= " AND V1.\"OfficeID\" = '$OfficeID'";
        }
        if(isset($queryParams['ConProviderID'])){
            $ConProviderIDAppID = $queryParams['ConProviderID'];
        }else{
            $ConProviderIDAppID = '';
        }
        if ($ConProviderIDAppID) {
            $ConProviderID = '-999';
            $ApplicationConProviderID = '-999';
            if(!empty($ConProviderIDAppID)){
                $ConProviderIDAppIDArr = explode('~', $ConProviderIDAppID);
                if(!empty($ConProviderIDAppIDArr) && sizeof($ConProviderIDAppIDArr)==2){
                    $ConProviderID = $ConProviderIDAppIDArr[0];
                    $ApplicationConProviderID = $ConProviderIDAppIDArr[1];
                }else if(!empty($ConProviderIDAppIDArr) && sizeof($ConProviderIDAppIDArr)==1){
                    $ConProviderID = $ConProviderIDAppIDArr[0];
                }
            }
            $query .= " AND V1.\"ConProviderID\" = '$ConProviderID'";
            $countquery .= " AND V1.\"ConProviderID\" = '$ConProviderID'";
        }
        if(isset($queryParams['ProviderID'])){
            $ProviderIDAppID = $queryParams['ProviderID'];
        }else{
            $ProviderIDAppID = '';
        }
        if ($ProviderIDAppID) {
            $PProviderID = '-999';
            $PApplicationProviderID = '-999';
            if(!empty($ProviderIDAppID)){
                $ProviderIDAppIDArr = explode('~', $ProviderIDAppID);
                if(!empty($ProviderIDAppIDArr) && sizeof($ProviderIDAppIDArr)==2){
                    $PProviderID = $ProviderIDAppIDArr[0];
                    $PApplicationProviderID = $ProviderIDAppIDArr[1];
                }else if(!empty($ProviderIDAppIDArr) && sizeof($ProviderIDAppIDArr)==1){
                    $PProviderID = $ProviderIDAppIDArr[0];
                }
            }
            $query .= " AND V1.\"ProviderID\" = '$PProviderID'";
            $countquery .= " AND V1.\"ProviderID\" = '$PProviderID'";
        }
        if(isset($queryParams['ConflictType'])){
            $ConflictType = $queryParams['ConflictType'];
        }else{
            $ConflictType = '';
        }
        if ($ConflictType) {
            if($ConflictType==1){//Same Sch Time
                $query .= " AND V1.\"SameSchTimeFlag\" = 'Y'";
                $countquery .= " AND V1.\"SameSchTimeFlag\" = 'Y'";
            }else if($ConflictType==2){//Same Visit Time
                $query .= " AND V1.\"SameVisitTimeFlag\" = 'Y'";
                $countquery .= " AND V1.\"SameVisitTimeFlag\" = 'Y'";
            }else if($ConflictType==3){//Sch And Visit Time Same
                $query .= " AND V1.\"SchAndVisitTimeSameFlag\" = 'Y'";
                $countquery .= " AND V1.\"SchAndVisitTimeSameFlag\" = 'Y'";
            }else if($ConflictType==4){//Sch Over Another Sch Time
                $query .= " AND V1.\"SchOverAnotherSchTimeFlag\" = 'Y'";
                $countquery .= " AND V1.\"SchOverAnotherSchTimeFlag\" = 'Y'";
            }else if($ConflictType==5){//Visit Time Over Another Visit Time Type
                $query .= " AND V1.\"VisitTimeOverAnotherVisitTimeFlag\" = 'Y'";
                $countquery .= " AND V1.\"VisitTimeOverAnotherVisitTimeFlag\" = 'Y'";
            }else if($ConflictType==6){//Sch Time Over Visit Time
                $query .= " AND V1.\"SchTimeOverVisitTimeFlag\" = 'Y'";
                $countquery .= " AND V1.\"SchTimeOverVisitTimeFlag\" = 'Y'";
            }else if($ConflictType==7){//Distance
                $query .= " AND V1.\"DistanceFlag\" = 'Y'";
                $countquery .= " AND V1.\"DistanceFlag\" = 'Y'";
            }else if($ConflictType==8){//In-Service
                $query .= " AND V1.\"InServiceFlag\" = 'Y'";
                $countquery .= " AND V1.\"InServiceFlag\" = 'Y'";
            }else if($ConflictType==9){//PTO
                $query .= " AND V1.\"PTOFlag\" = 'Y'";
                $countquery .= " AND V1.\"PTOFlag\" = 'Y'";
            }
            // else{
            //     $query .= " AND (V1.\"SchOverAnotherSchTimeFlag\" = 'Y' OR V1.\"VisitTimeOverAnotherVisitTimeFlag\" = 'Y')";
            //     $countquery .= " AND (V1.\"SchOverAnotherSchTimeFlag\" = 'Y' OR V1.\"VisitTimeOverAnotherVisitTimeFlag\" = 'Y')"; 
            // }
        }
        // else{
        //     $query .= " AND (V1.\"SchOverAnotherSchTimeFlag\" = 'Y' OR V1.\"VisitTimeOverAnotherVisitTimeFlag\" = 'Y')";
        //     $countquery .= " AND (V1.\"SchOverAnotherSchTimeFlag\" = 'Y' OR V1.\"VisitTimeOverAnotherVisitTimeFlag\" = 'Y')"; 
        // }
        if(isset($queryParams['VisitStartDate'])){
            $VisitStartDate = $queryParams['VisitStartDate'];
        }else{
            $VisitStartDate = '';
        }
        if(isset($queryParams['VisitEndDate'])){
            $VisitEndDate = $queryParams['VisitEndDate'];
        }else{
            $VisitEndDate = '';
        }
        if ($VisitStartDate && $VisitEndDate) {
            $query .= " AND V1.\"VisitDate\" BETWEEN '$VisitStartDate' AND '$VisitEndDate'";
            $countquery .= " AND V1.\"VisitDate\" BETWEEN '$VisitStartDate' AND '$VisitEndDate'";
        }else if ($VisitStartDate && !$VisitEndDate) {
            $query .= " AND V1.\"VisitDate\" >= '$VisitStartDate'";
            $countquery .= " AND V1.\"VisitDate\" >= '$VisitStartDate'";
        }else if (!$VisitStartDate && $VisitEndDate) {
            $query .= " AND V1.\"VisitDate\" <= '$VisitEndDate'";
            $countquery .= " AND V1.\"VisitDate\" <= '$VisitEndDate'";
        }
        if(isset($queryParams['BilledStartDate'])){
            $BilledStartDate = $queryParams['BilledStartDate'];
        }else{
            $BilledStartDate = '';
        }
        if(isset($queryParams['BilledEndDate'])){
            $BilledEndDate = $queryParams['BilledEndDate'];
        }else{
            $BilledEndDate = '';
        }
        if ($BilledStartDate && $BilledEndDate) {
            $query .= " AND TO_CHAR(V1.\"BilledDate\", 'YYYY-MM-DD') BETWEEN '$BilledStartDate' AND '$BilledEndDate'";
            $countquery .= " AND TO_CHAR(V1.\"BilledDate\", 'YYYY-MM-DD') BETWEEN '$BilledStartDate' AND '$BilledEndDate'";
        }else if ($BilledStartDate && !$BilledEndDate) {
            $query .= " AND TO_CHAR(V1.\"BilledDate\", 'YYYY-MM-DD') >= '$BilledStartDate'";
            $countquery .= " AND TO_CHAR(V1.\"BilledDate\", 'YYYY-MM-DD') >= '$BilledStartDate'";
        }else if (!$BilledStartDate && $BilledEndDate) {
            $query .= " AND TO_CHAR(V1.\"BilledDate\", 'YYYY-MM-DD') <= '$BilledEndDate'";
            $countquery .= " AND TO_CHAR(V1.\"BilledDate\", 'YYYY-MM-DD') <= '$BilledEndDate'";
        }
        //CReportedStartDate=2024-06-05&CReportedEndDate
        if(isset($queryParams['CReportedStartDate'])){
            $CReportedStartDate = $queryParams['CReportedStartDate'];
        }else{
            $CReportedStartDate = '';
        }
        if(isset($queryParams['CReportedEndDate'])){
            $CReportedEndDate = $queryParams['CReportedEndDate'];
        }else{
            $CReportedEndDate = '';
        }
        if ($CReportedStartDate && $CReportedEndDate) {
            $query .= " AND TO_CHAR(V1.\"CRDATEUNIQUE\", 'YYYY-MM-DD') BETWEEN '$CReportedStartDate' AND '$CReportedEndDate'";
            $countquery .= " AND TO_CHAR(V1.\"CRDATEUNIQUE\", 'YYYY-MM-DD') BETWEEN '$CReportedStartDate' AND '$CReportedEndDate'";
        }else if ($CReportedStartDate && !$CReportedEndDate) {
            $query .= " AND TO_CHAR(V1.\"CRDATEUNIQUE\", 'YYYY-MM-DD') >= '$CReportedStartDate'";
            $countquery .= " AND TO_CHAR(V1.\"CRDATEUNIQUE\", 'YYYY-MM-DD') >= '$CReportedStartDate'";
        }else if (!$CReportedStartDate && $CReportedEndDate) {
            $query .= " AND TO_CHAR(V1.\"CRDATEUNIQUE\", 'YYYY-MM-DD') <= '$CReportedEndDate'";
            $countquery .= " AND TO_CHAR(V1.\"CRDATEUNIQUE\", 'YYYY-MM-DD') <= '$CReportedEndDate'";
        }
        if(isset($queryParams['AgingDays'])){
            $AgingDays = $queryParams['AgingDays'];
        }else{
            $AgingDays = '';
        }
        if ($AgingDays) {
            $AgingDays = is_numeric($AgingDays) ? $AgingDays : -99;
            $query .= " AND DATEDIFF(DAY, V1.\"CRDATEUNIQUE\", GETDATE()) = '$AgingDays'";
            $countquery .= " AND DATEDIFF(DAY, V1.\"CRDATEUNIQUE\", GETDATE()) = '$AgingDays'";
        }
        if(isset($queryParams['countrecdf'])){
            $statement_count = $this->conn->prepare($countquery);        
            $total_results = $statement_count->fetch(PDO::FETCH_ASSOC);
            return $total_results['count'];
        }
        
        if($SortByField && $SortByAD){
            $sortby = $SortByAD;
            if($SortByField=='ParentStatusFlag'){
                $query .= " ORDER BY CASE
                    WHEN V2.\"StatusFlag\" IN ('D', 'R') THEN 'R'
                    ELSE V2.\"StatusFlag\"
                END ".$sortby."";
            }else if($SortByField=='ScheduleHours'){
                $query .= " ORDER BY (TIMESTAMPDIFF(MINUTE, V1.\"SchStartTime\", V1.\"SchEndTime\") / 60) ".$sortby."";
            }else{
                $query .= " ORDER BY ".$SortByField." ".$sortby."";
            }
        }else{
            $sortby = 'DESC';
            $query .= " ORDER BY V1.\"CONFLICTID\" ".$sortby."";
        }
        $query .= " LIMIT $perPage OFFSET $offset";
        $statement = $this->conn->prepare($query);
        $results = $statement->fetchAll(PDO::FETCH_ASSOC);
        return $results;
    }

    public function getchildvisits($ConflictIDs, $ProviderID, $AppProviderID, $UserID=''){
        $query_ch = "SELECT V1.\"ID\", V1.\"CONFLICTID\", V1.\"GroupID\", V1.\"SSN\", V1.\"ConProviderID\", V1.\"ConAppProviderID\", V1.\"ConProviderName\", V1.\"SameSchTimeFlag\", V1.\"SameVisitTimeFlag\", V1.\"SchAndVisitTimeSameFlag\", V1.\"SchOverAnotherSchTimeFlag\", V1.\"VisitTimeOverAnotherVisitTimeFlag\", V1.\"SchTimeOverVisitTimeFlag\", V1.\"DistanceFlag\", V1.\"InServiceFlag\", V1.\"PTOFlag\", V1.\"ConAgencyContact\", V1.\"ConAgencyPhone\", V1.\"ConLastUpdatedBy\", V1.\"ConLastUpdatedDate\", V1.\"ConContract\", (V1.\"ConBilledRateMinute\"*60) AS \"ConBilledRate\",
        CASE
           WHEN V1.\"StatusFlag\" IN ('D', 'R') THEN 'R'
           ELSE V1.\"StatusFlag\"
        END AS \"StatusFlag\",
        V1.\"StatusFlag\" AS \"OrgStatusFlag\", CONCAT(V1.\"VisitID\", '~',V1.\"AppVisitID\") as \"VAPPID\", CONCAT(V1.\"ConVisitID\", '~',V1.\"ConAppVisitID\") as \"ConVAPPID\",
        CASE
            WHEN V1.\"CShVTSTTime\" >= V1.\"ShVTSTTime\" AND V1.\"CShVTSTTime\" <= V1.\"ShVTENTime\" AND V1.\"CShVTENTime\" > V1.\"ShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"CShVTSTTime\", V1.\"ShVTENTime\")
            WHEN V1.\"ShVTSTTime\" >= V1.\"CShVTSTTime\" AND V1.\"ShVTSTTime\" <= V1.\"CShVTENTime\" AND V1.\"ShVTENTime\" > V1.\"CShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"ShVTSTTime\", V1.\"CShVTENTime\")
            WHEN V1.\"CShVTSTTime\" >= V1.\"ShVTSTTime\" AND V1.\"CShVTENTime\" <= V1.\"ShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"CShVTSTTime\", V1.\"CShVTENTime\")
            WHEN V1.\"ShVTSTTime\" >= V1.\"CShVTSTTime\" AND V1.\"ShVTENTime\" <= V1.\"CShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"ShVTSTTime\", V1.\"ShVTENTime\")
            WHEN V1.\"CShVTSTTime\" < V1.\"ShVTSTTime\" AND V1.\"CShVTENTime\" > V1.\"ShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"ShVTSTTime\", V1.\"ShVTENTime\")
            WHEN V1.\"ShVTSTTime\" < V1.\"CShVTSTTime\" AND V1.\"ShVTENTime\" > V1.\"CShVTENTime\" THEN TIMESTAMPDIFF(MINUTE, V1.\"CShVTSTTime\", V1.\"CShVTENTime\")
            ELSE 0
        END AS \"OverlapTime\",
        CONCAT(V1.\"VisitID\", '~', V1.\"AppVisitID\") as \"APatientAPPID\",
        CONCAT(V1.\"ConVisitID\", '~', V1.\"ConAppVisitID\") as \"ConAPatientAPPID\"      
         FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS V1";
         $query_ch .= " WHERE V1.\"ProviderID\" = '".$ProviderID."'";
        //  if($ofcquery = ofcquery()){
        //     $query_ch .= " AND V1.\"OfficeID\" IN (".$ofcquery.")";
        // }
        $query_ch .= " AND V1.\"CONFLICTID\" IN(".$ConflictIDs.")";
        $statement_ch = $this->conn->prepare($query_ch);            
        return $statement_ch->fetchAll(PDO::FETCH_ASSOC);
    }

    public function getVisitsPayer($request, $PayerID, $AppPayerID, $UserID, $all = '', $GroupIDCUR=''){
        $queryParams = [];
        $nextprev = false;
        if(($all=='Next' || $all=='Prev') && !empty($GroupIDCUR)){
            $queryParams = session('visit_query_params', []);
            $nextprev = true;
        }
        $currentPage = $request->input('page', 1);
        // Define the number of items per page
        // $perPage = 10;
        // Define allowed values for pagination
        $allowedPerPageOptions = [10, 50, 100, 200, 500];

        // Get the per_page value from the request or session
        $perPage = $request->per_page;

        // Check if per_page is a valid numeric value in the allowed options
        if (in_array($perPage, $allowedPerPageOptions)) {
            // Store the per_page value in the session if it's valid
            session(['per_page' => $perPage]);
        } else {
            // Retrieve the per_page from session or default to 10
            $perPage = session('per_page', 10);
        }
        // Calculate the offset
        $offset = ($currentPage - 1) * $perPage;
        $TOPL = '';
        if($all == '-1')
        {
            $TOPL = ' TOP 200';
        }else if(in_array($all, ['Next', 'Prev']))
        {
            $TOPL = ' TOP 1';
        }
        $sortableLinks = [
            'GroupID' => 'sq."GroupID"',
            'CRD' => 'sq."CRDATEUNIQUE"',
            'aidelname' => 'sq."AideLName"',
            'aidefname' => 'sq."AideFName"',
            'ssn' => 'sq."AideSSN"',
            'visitdate' => 'sq."VisitDate"'
        ];
        $sortableLinksAD = [
            'asc' => 'asc',
            'desc' => 'desc'
        ];
        if(isset($queryParams['sort']) && isset($sortableLinks[strtolower($queryParams['sort'])]) && $nextprev==true){
            $SortByField = $sortableLinks[strtolower($queryParams['sort'])];
        }else if($request->sort && isset($sortableLinks[strtolower($request->sort)])){
            $SortByField = $sortableLinks[strtolower($request->sort)];
        }else{
            $SortByField = 'sq."GroupID"';
        }
        if(isset($queryParams['direction']) && isset($sortableLinks[strtolower($queryParams['direction'])]) && $nextprev==true){
            $SortByAD = $sortableLinksAD[strtolower($queryParams['direction'])];
        }else if($request->direction && isset($sortableLinksAD[strtolower($request->direction)])){
            $SortByAD = strtoupper($sortableLinksAD[strtolower($request->direction)]);
        }else{
            $SortByAD = 'DESC';
        }
        $SelectQuery = "SELECT
            V1.\"ID\",
            V1.\"GroupID\",
            V1.\"SSN\",
            V1.\"CaregiverID\",
            V1.\"AppCaregiverID\",
            V1.\"VisitID\",
            V1.\"AppVisitID\",
            V1.\"VisitDate\",
            V1.\"CaregiverID\",
            V1.\"AppCaregiverID\",
            V1.\"AideCode\",
            V1.\"AideFName\",
            V1.\"AideLName\",
            COALESCE(V1.\"AideSSN\", V1.\"SSN\") AS \"AideSSN\",
            V1.\"G_CRDATEUNIQUE\" AS \"CRDATEUNIQUE\",
            V1.\"PayerID\",
            V1.\"Contract\",
            V1.\"FlagForReview\",
            ROW_NUMBER() OVER (PARTITION BY V1.\"GroupID\" ORDER BY V1.\"PayerID\" DESC) AS RN
        FROM
            CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS V1
        INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTS AS V2 ON V2.\"CONFLICTID\" = V1.\"CONFLICTID\"";

        $SelectQuerySub = "SELECT DISTINCT \"GroupID\" FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS V3 INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTS AS V4 ON
        V4.\"CONFLICTID\" = V3.\"CONFLICTID\"";
        $wherecon = false;
        if(isset($queryParams['PAdmissionID']) && $nextprev==true){
            $PAdmissionID = $queryParams['PAdmissionID'];
        }else if($request->PAdmissionID){
            $PAdmissionID = $request->PAdmissionID;
        }else{
            $PAdmissionID = '';
        }
        if ($PAdmissionID) {
            if($wherecon==false){
                $SelectQuerySub .= " WHERE ";
                $wherecon = true;
            }else{
                $SelectQuerySub .= " AND ";
            }
            $SelectQuerySub .= "V3.\"PA_PAdmissionID\" ILIKE '%$PAdmissionID%'";
        }
        if(isset($queryParams['MedicaidID']) && $nextprev==true){
            $MedicaidID = $queryParams['MedicaidID'];
        }else if($request->MedicaidID){
            $MedicaidID = $request->MedicaidID;
        }else{
            $MedicaidID = '';
        }
        if ($MedicaidID) {
            if($wherecon==false){
                $SelectQuerySub .= " WHERE ";
                $wherecon = true;
            }else{
                $SelectQuerySub .= " AND ";
            }
            $SelectQuerySub .= "V3.\"PA_PMedicaidNumber\" ILIKE '%$MedicaidID%'";
        }
        if(isset($queryParams['PLName']) && $nextprev==true){
            $PLName = $queryParams['PLName'];
        }else if($request->PLName){
            $PLName = $request->PLName;
        }else{
            $PLName = '';
        }
        if ($PLName) {
            if($wherecon==false){
                $SelectQuerySub .= " WHERE ";
                $wherecon = true;
            }else{
                $SelectQuerySub .= " AND ";
            }
            $SelectQuerySub .= "V3.\"PA_PLName\" ILIKE '%$PLName%'";
        }
        if(isset($queryParams['PFName']) && $nextprev==true){
            $PFName = $queryParams['PFName'];
        }else if($request->PFName){
            $PFName = $request->PFName;
        }else{
            $PFName = '';
        }
        if ($PFName) {
            if($wherecon==false){
                $SelectQuerySub .= " WHERE ";
                $wherecon = true;
            }else{
                $SelectQuerySub .= " AND ";
            }
            $SelectQuerySub .= "V3.\"PA_PFName\" ILIKE '%$PFName%'";
        }        
        $statusFlags = [];
        if(isset($queryParams['status_flags']) && is_array($queryParams['status_flags']) && $nextprev==true){
            $statusFlags = $queryParams['status_flags'];
        }else if($request->status_flags && is_array($request->status_flags)){
            $statusFlags = $request->status_flags;
        }else if(isset($queryParams['ConflictStatusFlag']) && $nextprev==true){
            $statusFlags = [$queryParams['ConflictStatusFlag']];
        }else if($request->ConflictStatusFlag){
            $statusFlags = [$request->ConflictStatusFlag];
        }
        
        if (!empty($statusFlags)) {
            if (in_array('R', $statusFlags)) {
                $statusFlags[] = 'D';
                $statusFlags = array_unique($statusFlags);
            }
            if($wherecon==false){
                $SelectQuerySub .= " WHERE ";
                $wherecon = true;
            }else{
                $SelectQuerySub .= " AND ";
            }
            $statusFlagsList = "'" . implode("','", array_map('addslashes', $statusFlags)) . "'";
            $SelectQuerySub .= "V4.\"StatusFlag\" IN ({$statusFlagsList})";
        }
        if(isset($queryParams['NoResponse']) && $nextprev==true){
            $NoResponse = $queryParams['NoResponse'];
        }else if($request->NoResponse){
            $NoResponse = $request->NoResponse;
        }else{
            $NoResponse = '';
        }
        if ($NoResponse) {
            if($NoResponse=='Yes'){
                if($wherecon==false){
                $SelectQuerySub .= " WHERE ";
                $wherecon = true;
            }else{
                $SelectQuerySub .= " AND ";
            }
            $SelectQuerySub .= "V4.\"NoResponseFlag\" = '$NoResponse'";
            }else{
                if($wherecon==false){
                    $SelectQuerySub .= " WHERE ";
                    $wherecon = true;
                }else{
                    $SelectQuerySub .= " AND ";
                }
                $SelectQuerySub .= "V4.\"NoResponseFlag\" IS NULL";
            }
        }
        if(request()->route()->getName() == 'flag-for-review'){
            $FlagForReview = 'Yes';
        }else if(isset($queryParams['FlagForReview']) && $nextprev==true){
            $FlagForReview = $queryParams['FlagForReview'];
        }else if($request->FlagForReview){
            $FlagForReview = $request->FlagForReview;
        }else{
            $FlagForReview = '';
        }
        if ($FlagForReview) {
            if($FlagForReview=='Yes'){
                if($wherecon==false){
                    $SelectQuerySub .= " WHERE ";
                    $wherecon = true;
                }else{
                    $SelectQuerySub .= " AND ";
                }
                $SelectQuerySub .= "V3.\"FlagForReview\" = '$FlagForReview'";
            }else{
                if($wherecon==false){
                    $SelectQuerySub .= " WHERE ";
                    $wherecon = true;
                }else{
                    $SelectQuerySub .= " AND ";
                }
                $SelectQuerySub .= "(V3.\"FlagForReview\" IS NULL OR V3.\"FlagForReview\" = 'No')";
            }
        }
        if(isset($queryParams['PayerID']) && $nextprev==true){
            $PayerIDAppID = $queryParams['PayerID'];
        }else if($request->PayerID){
            $PayerIDAppID = $request->PayerID;
        }else{
            $PayerIDAppID = '';
        }
        if ($PayerIDAppID) {
            $ConPayerID = '-999';
            $ConApplicationPayerId = '-999';
            if(!empty($PayerIDAppID)){
                $PayerIDAppIDArr = explode('~', $PayerIDAppID);
                if(!empty($PayerIDAppIDArr) && sizeof($PayerIDAppIDArr)==2){
                    $ConPayerID = $PayerIDAppIDArr[0];
                    $ConApplicationPayerId = $PayerIDAppIDArr[1];
                }else if(!empty($PayerIDAppIDArr) && sizeof($PayerIDAppIDArr)==1){
                    $ConPayerID = $PayerIDAppIDArr[0];
                }
            }
            if($wherecon==false){
                $SelectQuerySub .= " WHERE ";
                $wherecon = true;
            }else{
                $SelectQuerySub .= " AND ";
            }
            $SelectQuerySub .= "V3.\"PayerID\" = '$ConPayerID'";
        }
        if(isset($queryParams['OfficeID']) && $nextprev==true){
            $OfficeIDAppID = $queryParams['OfficeID'];
        }else if($request->OfficeID){
            $OfficeIDAppID = $request->OfficeID;
        }else{
            $OfficeIDAppID = '';
        }
        if ($OfficeIDAppID) {
            $OfficeID = '-999';
            $ApplicationOfficeID = '-999';
            if(!empty($OfficeIDAppID)){
                $OfficeIDAppIDArr = explode('~', $OfficeIDAppID);
                if(!empty($OfficeIDAppIDArr) && sizeof($OfficeIDAppIDArr)==2){
                    $OfficeID = $OfficeIDAppIDArr[0];
                    $ApplicationOfficeID = $OfficeIDAppIDArr[1];
                }else if(!empty($OfficeIDAppIDArr) && sizeof($OfficeIDAppIDArr)==1){
                    $OfficeID = $OfficeIDAppIDArr[0];
                }
            }
            if($wherecon==false){
                $SelectQuerySub .= " WHERE ";
                $wherecon = true;
            }else{
                $SelectQuerySub .= " AND ";
            }
            $SelectQuerySub .= "V3.\"OfficeID\" = '$OfficeID'";
        }
        if(isset($queryParams['ProviderID']) && $nextprev==true){
            $ProviderIDAppID = $queryParams['ProviderID'];
        }else if($request->ProviderID){
            $ProviderIDAppID = $request->ProviderID;
        }else{
            $ProviderIDAppID = '';
        }
        if ($ProviderIDAppID) {
            $PProviderID = '-999';
            $PApplicationProviderID = '-999';
            if(!empty($ProviderIDAppID)){
                $ProviderIDAppIDArr = explode('~', $ProviderIDAppID);
                if(!empty($ProviderIDAppIDArr) && sizeof($ProviderIDAppIDArr)==2){
                    $PProviderID = $ProviderIDAppIDArr[0];
                    $PApplicationProviderID = $ProviderIDAppIDArr[1];
                }else if(!empty($ProviderIDAppIDArr) && sizeof($ProviderIDAppIDArr)==1){
                    $PProviderID = $ProviderIDAppIDArr[0];
                }
            }
            if($wherecon==false){
                $SelectQuerySub .= " WHERE ";
                $wherecon = true;
            }else{
                $SelectQuerySub .= " AND ";
            }
            $SelectQuerySub .= "V3.\"ProviderID\" = '$PProviderID'";
        }
        if(isset($queryParams['ProviderTIN']) && $nextprev==true){
            $ProviderTINAppID = $queryParams['ProviderTIN'];
        }else if($request->ProviderTIN){
            $ProviderTINAppID = $request->ProviderTIN;
        }else{
            $ProviderTINAppID = '';
        }
        if ($ProviderTINAppID) {
            if($wherecon==false){
                $SelectQuerySub .= " WHERE ";
                $wherecon = true;
            }else{
                $SelectQuerySub .= " AND ";
            }
            $SelectQuerySub .= "V3.\"FederalTaxNumber\" = '$ProviderTINAppID'";
        }

        if(isset($queryParams['ConflictType']) && $nextprev==true){
            $ConflictType = $queryParams['ConflictType'];
        }else if($request->ConflictType){
            $ConflictType = $request->ConflictType;
        }else{
            $ConflictType = '';
        }
        
        // Add conflict type filtering using ConflictTypeHelper (like state portal)
        if ($ConflictType) {
            $conflictCondition = ConflictTypeHelper::buildConflictCondition((int)$ConflictType, 'V3');
            if (!empty($conflictCondition)) {
                if($wherecon==false){
                    $SelectQuerySub .= " WHERE ";
                    $wherecon = true;
                }else{
                    $SelectQuerySub .= " AND ";
                }
                $SelectQuerySub .= $conflictCondition;
            }
        }
        
        if(isset($queryParams['VisitStartDate']) && $nextprev==true){
            $VisitStartDate = $queryParams['VisitStartDate'];
        }else if($request->VisitStartDate){
            $VisitStartDate = $request->VisitStartDate;
        }else{
            $VisitStartDate = '';
        }
        if(isset($queryParams['VisitEndDate']) && $nextprev==true){
            $VisitEndDate = $queryParams['VisitEndDate'];
        }else if($request->VisitEndDate){
            $VisitEndDate = $request->VisitEndDate;
        }else{
            $VisitEndDate = '';
        }
        if ($VisitStartDate && $VisitEndDate) {
            if($wherecon==false){
                    $SelectQuerySub .= " WHERE ";
                    $wherecon = true;
                }else{
                    $SelectQuerySub .= " AND ";
                }
                $SelectQuerySub .= "V3.\"VisitDate\" BETWEEN '$VisitStartDate' AND '$VisitEndDate'";
        }else if ($VisitStartDate && !$VisitEndDate) {
            if($wherecon==false){
                    $SelectQuerySub .= " WHERE ";
                    $wherecon = true;
                }else{
                    $SelectQuerySub .= " AND ";
                }
                $SelectQuerySub .= "V3.\"VisitDate\" >= '$VisitStartDate'";
        }else if (!$VisitStartDate && $VisitEndDate) {
            if($wherecon==false){
                    $SelectQuerySub .= " WHERE ";
                    $wherecon = true;
                }else{
                    $SelectQuerySub .= " AND ";
                }
                $SelectQuerySub .= "V3.\"VisitDate\" <= '$VisitEndDate'";
        }
        if(isset($queryParams['BilledStartDate']) && $nextprev==true){
            $BilledStartDate = $queryParams['BilledStartDate'];
        }else if($request->BilledStartDate){
            $BilledStartDate = $request->BilledStartDate;
        }else{
            $BilledStartDate = '';
        }
        if(isset($queryParams['BilledEndDate']) && $nextprev==true){
            $BilledEndDate = $queryParams['BilledEndDate'];
        }else if($request->BilledEndDate){
            $BilledEndDate = $request->BilledEndDate;
        }else{
            $BilledEndDate = '';
        }
        if ($BilledStartDate && $BilledEndDate) {
            if($wherecon==false){
                $SelectQuerySub .= " WHERE ";
                $wherecon = true;
            }else{
                $SelectQuerySub .= " AND ";
            }
            $SelectQuerySub .= "TO_CHAR(V3.\"BilledDate\", 'YYYY-MM-DD') BETWEEN '$BilledStartDate' AND '$BilledEndDate'";
        }else if ($BilledStartDate && !$BilledEndDate) {
            if($wherecon==false){
                $SelectQuerySub .= " WHERE ";
                $wherecon = true;
            }else{
                $SelectQuerySub .= " AND ";
            }
            $SelectQuerySub .= "TO_CHAR(V3.\"BilledDate\", 'YYYY-MM-DD') >= '$BilledStartDate'";
        }else if (!$BilledStartDate && $BilledEndDate) {
            if($wherecon==false){
                $SelectQuerySub .= " WHERE ";
                $wherecon = true;
            }else{
                $SelectQuerySub .= " AND ";
            }
            $SelectQuerySub .= "TO_CHAR(V3.\"BilledDate\", 'YYYY-MM-DD') <= '$BilledEndDate'";
        }
        //CReportedStartDate=2024-06-05&CReportedEndDate
        if(isset($queryParams['CReportedStartDate']) && $nextprev==true){
            $CReportedStartDate = $queryParams['CReportedStartDate'];
        }else if($request->CReportedStartDate){
            $CReportedStartDate = $request->CReportedStartDate;
        }else{
            $CReportedStartDate = '';
        }
        if(isset($queryParams['CReportedEndDate']) && $nextprev==true){
            $CReportedEndDate = $queryParams['CReportedEndDate'];
        }else if($request->CReportedEndDate){
            $CReportedEndDate = $request->CReportedEndDate;
        }else{
            $CReportedEndDate = '';
        }
        if ($CReportedStartDate && $CReportedEndDate) {
            if($wherecon==false){
                $SelectQuerySub .= " WHERE ";
                $wherecon = true;
            }else{
                $SelectQuerySub .= " AND ";
            }
            $SelectQuerySub .= "TO_CHAR(V3.\"G_CRDATEUNIQUE\", 'YYYY-MM-DD') BETWEEN '$CReportedStartDate' AND '$CReportedEndDate'";
        }else if ($CReportedStartDate && !$CReportedEndDate) {
            if($wherecon==false){
                $SelectQuerySub .= " WHERE ";
                $wherecon = true;
            }else{
                $SelectQuerySub .= " AND ";
            }
            $SelectQuerySub .= "TO_CHAR(V3.\"G_CRDATEUNIQUE\", 'YYYY-MM-DD') >= '$CReportedStartDate'";
        }else if (!$CReportedStartDate && $CReportedEndDate) {
            if($wherecon==false){
                $SelectQuerySub .= " WHERE ";
                $wherecon = true;
            }else{
                $SelectQuerySub .= " AND ";
            }
            $SelectQuerySub .= "TO_CHAR(V3.\"G_CRDATEUNIQUE\", 'YYYY-MM-DD') <= '$CReportedEndDate'";
        }
        if(isset($queryParams['AgingDays']) && $nextprev==true){
            $AgingDays = $queryParams['AgingDays'];
        }else if($request->AgingDays){
            $AgingDays = $request->AgingDays;
        }else{
            $AgingDays = '';
        }
        if ($AgingDays) {
            $AgingDays = is_numeric($AgingDays) ? $AgingDays : -99;
            if($wherecon==false){
                $SelectQuerySub .= " WHERE ";
                $wherecon = true;
            }else{
                $SelectQuerySub .= " AND ";
            }
            $SelectQuerySub .= "DATEDIFF(DAY, V3.\"CRDATEUNIQUE\", GETDATE()) = '$AgingDays'";
        }

        if(isset($queryParams['AideCode']) && $nextprev==true){
            $AideCode = $queryParams['AideCode'];
        }else if($request->AideCode){
            $AideCode = $request->AideCode;
        }else{
            $AideCode = '';
        }
        if ($AideCode) {
            $SelectQuerySub .= " AND V3.\"AideCode\" ILIKE '%$AideCode%'";
        }
        if(isset($queryParams['AideLName']) && $nextprev==true){
            $AideLName = $queryParams['AideLName'];
        }else if($request->AideLName){
            $AideLName = $request->AideLName;
        }else{
            $AideLName = '';
        }
        if ($AideLName) {
            $SelectQuerySub .= " AND V3.\"AideLName\" ILIKE '%$AideLName%'";
        }
        if(isset($queryParams['AideFName']) && $nextprev==true){
            $AideFName = $queryParams['AideFName'];
        }else if($request->AideFName){
            $AideFName = $request->AideFName;
        }else{
            $AideFName = '';
        }
        if ($AideFName) {
            $SelectQuerySub .= " AND V3.\"AideFName\" ILIKE '%$AideFName%'";
        }
        $SelectQuery .= " INNER JOIN (".$SelectQuerySub.") AS V5 ON V5.\"GroupID\" = V1.\"GroupID\"";


        if(auth()->user()->hasRole('Payer') && $PayerID){
            $SelectQuery .= " WHERE V1.\"PayerID\" = '".$PayerID."'";
        }else{
            $SelectQuery .= " WHERE V1.\"PayerID\" = '-9999'";
        }    
        $SelectQuery .= " AND V1.\"GroupID\" IS NOT NULL";
        
        // Add ProviderID filter to main query
        if ($ProviderIDAppID && $PProviderID && $PProviderID != '-999') {
            $SelectQuery .= " AND V1.\"ProviderID\" = '$PProviderID'";
        }
        
        // Add VisitDate filter to main query
        if ($VisitStartDate && $VisitEndDate) {
            $SelectQuery .= " AND V1.\"VisitDate\" BETWEEN '$VisitStartDate' AND '$VisitEndDate'";
        } else if ($VisitStartDate && !$VisitEndDate) {
            $SelectQuery .= " AND V1.\"VisitDate\" >= '$VisitStartDate'";
        } else if (!$VisitStartDate && $VisitEndDate) {
            $SelectQuery .= " AND V1.\"VisitDate\" <= '$VisitEndDate'";
        }
        
        if(isset($queryParams['GroupID'])){
            $GroupID = $queryParams['GroupID'];
        }else if($request->GroupID){
            $GroupID = $request->GroupID;
        }else{
            $GroupID = '';
        }
        if ($GroupID) {
            $GroupID = (int)$GroupID;
            $SelectQuery .= " AND V1.\"GroupID\" = TRY_TO_NUMBER('$GroupID')";
        }
        if ($nextprev==true) {
            //$SelectQuery .= " AND V1.\"GroupID\" != '$GroupIDCUR'";

           /* DESC
            IF GET PREV RECORD THEN SET DESC TO ASC AND POINTER SET TO > 3
            IF GET NEXT RECORD THEN SET DESC TO DESC AND POINTER SET TO < 3

            ASC
            IF GET PREV RECORD THEN SET ASC TO DESC AND POINTER SET TO < 3
            IF GET NEXT RECORD THEN SET ASC TO ASC AND POINTER SET TO > 3
            */
            if($SortByField && $SortByAD){
                if($all=='Prev' && $SortByAD=='DESC'){
                    $SelectQuery .= " AND V1.\"GroupID\" > $GroupIDCUR";
                }else if($all=='Next' && $SortByAD=='DESC'){
                    $SelectQuery .= " AND V1.\"GroupID\" < $GroupIDCUR";
                }else if($all=='Prev' && $SortByAD=='ASC'){
                    $SelectQuery .= " AND V1.\"GroupID\" < $GroupIDCUR";
                }else if($all=='Next' && $SortByAD=='ASC'){
                    $SelectQuery .= " AND V1.\"GroupID\" > $GroupIDCUR";
                }
            }else{
                if($all=='Prev'){
                    $SelectQuery .= " AND V1.\"GroupID\" > $GroupIDCUR";
                }else if($all=='Next'){
                    $SelectQuery .= " AND V1.\"GroupID\" < $GroupIDCUR";
                }
            }
        }
        $NewQuery = 'WITH ';
        if(isset($ConPayerID) && $PayerID==$ConPayerID){
            $NewQuery .= "GroupConflicts AS (
                SELECT 
                    \"GroupID\",
                    COUNT(DISTINCT V1.\"CONFLICTID\") AS \"ConflictCount\"
                FROM CONFLICTREPORT_SANDBOX.PUBLIC.CONFLICTVISITMAPS V1
                INNER JOIN CONFLICTREPORT_SANDBOX.PUBLIC.CONFLICTS V2 
                    ON V2.\"CONFLICTID\" = V1.\"CONFLICTID\"
                WHERE 
                    V1.\"PayerID\" = '".$ConPayerID."'
                GROUP BY \"GroupID\"
                HAVING \"ConflictCount\" > 1
            ),";
        }
        $NewQuery .= "ConflictVisitMaps AS (".$SelectQuery.")";
        $query =  $NewQuery;
        $query .= "SELECT ".$TOPL." sq.* FROM ConflictVisitMaps sq";
        if(isset($ConPayerID) && $PayerID==$ConPayerID){
            $query .= " INNER JOIN GroupConflicts GC ON sq.\"GroupID\" = GC.\"GroupID\"";
        }
        $query .= " WHERE sq.RN = 1";
        // Use the new optimized count function instead of the old count query
        if(isset($queryParams['countrecdf'])){
            return $this->getPayerConflictsCountFromView($queryParams, $PayerID, $AppPayerID, $request);
        }
        /*
        DESC
        IF GET PREV RECORD THEN SET DESC TO ASC AND POINTER SET TO > 3
        IF GET NEXT RECORD THEN SET DESC TO DESC AND POINTER SET TO < 3

        ASC
        IF GET PREV RECORD THEN SET ASC TO DESC AND POINTER SET TO < 3
        IF GET NEXT RECORD THEN SET ASC TO ASC AND POINTER SET TO > 3
        */
        if($SortByField && $SortByAD){
            $sortbyf = $SortByField;
            if($all=='Prev' && $SortByAD=='DESC'){
                $sortby = 'ASC';
            }else if($all=='Next' && $SortByAD=='DESC'){
                $sortby = 'DESC';
            }else if($all=='Prev' && $SortByAD=='ASC'){
                $sortby = 'DESC';
            }else if($all=='Next' && $SortByAD=='ASC'){
                $sortby = 'ASC';
            }else{
                $sortby = $SortByAD;
            }
        }else{
            $sortbyf = 'GroupID';
            if($all=='Prev'){
                $sortby = 'ASC';
            }else if($all=='Next'){
                $sortby = 'DESC';
            }else{
                $sortby = 'DESC';
            }
        }
        $query .= " ORDER BY ".$sortbyf." ".$sortby."";
        if(!in_array($all, ['-1', 'Next', 'Prev']))
        {
            $query .= " LIMIT $perPage OFFSET $offset";
        }
        if($request->debug){
            echo $query;
            echo "<hr />";
            // The count function will handle its own debug output
            $this->getPayerConflictsCountFromView($queryParams, $PayerID, $AppPayerID, $request);
            die;
        }
        $statement = $this->conn->prepare($query);
        if($nextprev==true){
            if($request->debug1){
                echo $query;
                die;
            }
            $results = $statement->fetch(PDO::FETCH_ASSOC);
            return !empty($results) ? convertToSslUrl(route('conflict-detail', ['CONFLICTID' => $results['GroupID']])) : '';
        }
        $results = $statement->fetchAll(PDO::FETCH_ASSOC);
        if($all == '-1')
        {
            return $results;
        }
        // Use the new optimized count function instead of the old count query
        $rowCount = $this->getPayerConflictsCountFromView($queryParams, $PayerID, $AppPayerID, $request);        
        $paginatedResults = new \Illuminate\Pagination\LengthAwarePaginator(
            $results,
            $rowCount,
            $perPage,
            $currentPage,
            ['path' => request()->url(), 'query' => request()->query()]
        );
        return $paginatedResults;
    }

    public function getVisitsPayerExport($queryParams, $PayerID, $AppPayerID, $currentPage=1, $perPage=5000){
        $UserID = '';
        // Calculate the offset
        $offset = ($currentPage - 1) * $perPage;
        $sortableLinks = [
            'GroupID' => 'sq."GroupID"',
            'CRD' => 'sq."CRDATEUNIQUE"',
            'aidelname' => 'sq."AideLName"',
            'aidefname' => 'sq."AideFName"',
            'ssn' => 'sq."AideSSN"',
            'visitdate' => 'sq."VisitDate"'
        ];
        $sortableLinksAD = [
            'asc' => 'asc',
            'desc' => 'desc'
        ];
        if(isset($queryParams['sort']) && isset($sortableLinks[strtolower($queryParams['sort'])])){
            $SortByField = $sortableLinks[strtolower($queryParams['sort'])];
        }else{
            $SortByField = 'sq."GroupID"';
        }
        if(isset($queryParams['direction']) && isset($sortableLinks[strtolower($queryParams['direction'])])){
            $SortByAD = $sortableLinksAD[strtolower($queryParams['direction'])];
        }else{
            $SortByAD = 'DESC';
        }
        $SelectQuery = "SELECT
            V1.\"ID\",
            V1.\"GroupID\",
            V1.\"SSN\",
            V1.\"CaregiverID\",
            V1.\"AppCaregiverID\",
            V1.\"VisitID\",
            V1.\"AppVisitID\",
            V1.\"VisitDate\",
            V1.\"CaregiverID\",
            V1.\"AppCaregiverID\",
            V1.\"AideCode\",
            V1.\"AideFName\",
            V1.\"AideLName\",
            COALESCE(V1.\"AideSSN\", V1.\"SSN\") AS \"AideSSN\",
            V1.\"G_CRDATEUNIQUE\" AS \"CRDATEUNIQUE\",
            V1.\"PayerID\",
            V1.\"Contract\",
            ROW_NUMBER() OVER (PARTITION BY V1.\"GroupID\" ORDER BY V1.\"PayerID\" DESC) AS RN
        FROM
            CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS V1
        INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTS AS V2 ON V2.\"CONFLICTID\" = V1.\"CONFLICTID\"";

        $SelectQuerySub = "SELECT DISTINCT \"GroupID\" FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS V3 INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTS AS V4 ON
        V4.\"CONFLICTID\" = V3.\"CONFLICTID\"";
        $wherecon = false;
        if(isset($queryParams['PAdmissionID'])){
            $PAdmissionID = $queryParams['PAdmissionID'];
        }else{
            $PAdmissionID = '';
        }
        if ($PAdmissionID) {
            if($wherecon==false){
                $SelectQuerySub .= " WHERE ";
                $wherecon = true;
            }else{
                $SelectQuerySub .= " AND ";
            }
            $SelectQuerySub .= "V3.\"PA_PAdmissionID\" ILIKE '%$PAdmissionID%'";
        }
        if(isset($queryParams['MedicaidID'])){
            $MedicaidID = $queryParams['MedicaidID'];
        }else{
            $MedicaidID = '';
        }
        if ($MedicaidID) {
            if($wherecon==false){
                $SelectQuerySub .= " WHERE ";
                $wherecon = true;
            }else{
                $SelectQuerySub .= " AND ";
            }
            $SelectQuerySub .= "V3.\"PA_PMedicaidNumber\" ILIKE '%$MedicaidID%'";
        }
        if(isset($queryParams['PLName'])){
            $PLName = $queryParams['PLName'];
        }else{
            $PLName = '';
        }
        if ($PLName) {
            if($wherecon==false){
                $SelectQuerySub .= " WHERE ";
                $wherecon = true;
            }else{
                $SelectQuerySub .= " AND ";
            }
            $SelectQuerySub .= "V3.\"PA_PLName\" ILIKE '%$PLName%'";
        }
        if(isset($queryParams['PFName'])){
            $PFName = $queryParams['PFName'];
        }else{
            $PFName = '';
        }
        if ($PFName) {
            if($wherecon==false){
                $SelectQuerySub .= " WHERE ";
                $wherecon = true;
            }else{
                $SelectQuerySub .= " AND ";
            }
            $SelectQuerySub .= "V3.\"PA_PFName\" ILIKE '%$PFName%'";
        }        
        $statusFlags = [];
        if (!empty($queryParams['status_flags']) && is_array($queryParams['status_flags'])) {
            $statusFlags = $queryParams['status_flags'];
        } elseif (!empty($queryParams['ConflictStatusFlag'])) {
            $statusFlags = [$queryParams['ConflictStatusFlag']];
        }
        
        if (!empty($statusFlags)) {
            if (in_array('R', $statusFlags)) {
                $statusFlags[] = 'D';
                $statusFlags = array_unique($statusFlags);
            }
            if($wherecon==false){
                $SelectQuerySub .= " WHERE ";
                $wherecon = true;
            }else{
                $SelectQuerySub .= " AND ";
            }
            $statusFlagsList = "'" . implode("','", array_map('addslashes', $statusFlags)) . "'";
            $SelectQuerySub .= "V4.\"StatusFlag\" IN ({$statusFlagsList})";
        }
        if(isset($queryParams['NoResponse'])){
            $NoResponse = $queryParams['NoResponse'];
        }else{
            $NoResponse = '';
        }
        if ($NoResponse) {
            if($NoResponse=='Yes'){
                if($wherecon==false){
                $SelectQuerySub .= " WHERE ";
                $wherecon = true;
            }else{
                $SelectQuerySub .= " AND ";
            }
            $SelectQuerySub .= "V4.\"NoResponseFlag\" = '$NoResponse'";
            }else{
                if($wherecon==false){
                    $SelectQuerySub .= " WHERE ";
                    $wherecon = true;
                }else{
                    $SelectQuerySub .= " AND ";
                }
                $SelectQuerySub .= "V4.\"NoResponseFlag\" IS NULL";
            }
        }
        
        if(isset($queryParams['FlagForReview'])){
            $FlagForReview = $queryParams['FlagForReview'];
        }else{
            $FlagForReview = '';
        }
        if ($FlagForReview) {
            if($FlagForReview=='Yes'){
                if($wherecon==false){
                    $SelectQuerySub .= " WHERE ";
                    $wherecon = true;
                }else{
                    $SelectQuerySub .= " AND ";
                }
                $SelectQuerySub .= "V3.\"FlagForReview\" = '$FlagForReview'";
            }else{
                if($wherecon==false){
                    $SelectQuerySub .= " WHERE ";
                    $wherecon = true;
                }else{
                    $SelectQuerySub .= " AND ";
                }
                $SelectQuerySub .= "(V3.\"FlagForReview\" IS NULL OR V3.\"FlagForReview\" = 'No')";
            }
        }    
        if(isset($queryParams['PayerID'])){
            $PayerIDAppID = $queryParams['PayerID'];
        }else{
            $PayerIDAppID = '';
        }
        if ($PayerIDAppID) {
            $ConPayerID = '-999';
            $ConApplicationPayerId = '-999';
            if(!empty($PayerIDAppID)){
                $PayerIDAppIDArr = explode('~', $PayerIDAppID);
                if(!empty($PayerIDAppIDArr) && sizeof($PayerIDAppIDArr)==2){
                    $ConPayerID = $PayerIDAppIDArr[0];
                    $ConApplicationPayerId = $PayerIDAppIDArr[1];
                }else if(!empty($PayerIDAppIDArr) && sizeof($PayerIDAppIDArr)==1){
                    $ConPayerID = $PayerIDAppIDArr[0];
                }
            }
            if($wherecon==false){
                $SelectQuerySub .= " WHERE ";
                $wherecon = true;
            }else{
                $SelectQuerySub .= " AND ";
            }
            $SelectQuerySub .= "V3.\"PayerID\" = '$ConPayerID'";
        }
        if(isset($queryParams['OfficeID'])){
            $OfficeIDAppID = $queryParams['OfficeID'];
        }else{
            $OfficeIDAppID = '';
        }
        if ($OfficeIDAppID) {
            $OfficeID = '-999';
            $ApplicationOfficeID = '-999';
            if(!empty($OfficeIDAppID)){
                $OfficeIDAppIDArr = explode('~', $OfficeIDAppID);
                if(!empty($OfficeIDAppIDArr) && sizeof($OfficeIDAppIDArr)==2){
                    $OfficeID = $OfficeIDAppIDArr[0];
                    $ApplicationOfficeID = $OfficeIDAppIDArr[1];
                }else if(!empty($OfficeIDAppIDArr) && sizeof($OfficeIDAppIDArr)==1){
                    $OfficeID = $OfficeIDAppIDArr[0];
                }
            }
            if($wherecon==false){
                $SelectQuerySub .= " WHERE ";
                $wherecon = true;
            }else{
                $SelectQuerySub .= " AND ";
            }
            $SelectQuerySub .= "V3.\"OfficeID\" = '$OfficeID'";
        }
        if(isset($queryParams['ProviderID'])){
            $ProviderIDAppID = $queryParams['ProviderID'];
        }else{
            $ProviderIDAppID = '';
        }
        if ($ProviderIDAppID) {
            $PProviderID = '-999';
            $PApplicationProviderID = '-999';
            if(!empty($ProviderIDAppID)){
                $ProviderIDAppIDArr = explode('~', $ProviderIDAppID);
                if(!empty($ProviderIDAppIDArr) && sizeof($ProviderIDAppIDArr)==2){
                    $PProviderID = $ProviderIDAppIDArr[0];
                    $PApplicationProviderID = $ProviderIDAppIDArr[1];
                }else if(!empty($ProviderIDAppIDArr) && sizeof($ProviderIDAppIDArr)==1){
                    $PProviderID = $ProviderIDAppIDArr[0];
                }
            }
            if($wherecon==false){
                $SelectQuerySub .= " WHERE ";
                $wherecon = true;
            }else{
                $SelectQuerySub .= " AND ";
            }
            $SelectQuerySub .= "V3.\"ProviderID\" = '$PProviderID'";
        }
        if(isset($queryParams['ProviderTIN'])){
            $ProviderTINAppID = $queryParams['ProviderTIN'];
        }else{
            $ProviderTINAppID = '';
        }
        if ($ProviderTINAppID) {
            if($wherecon==false){
                $SelectQuerySub .= " WHERE ";
                $wherecon = true;
            }else{
                $SelectQuerySub .= " AND ";
            }
            $SelectQuerySub .= "V3.\"FederalTaxNumber\" = '$ProviderTINAppID'";
        }
        if(isset($queryParams['ConflictType'])){
            $ConflictType = $queryParams['ConflictType'];
        }else{
            $ConflictType = '';
        }
        if ($ConflictType) {
            if($ConflictType==1){//Same Sch Time
                if($wherecon==false){
                    $SelectQuerySub .= " WHERE ";
                    $wherecon = true;
                }else{
                    $SelectQuerySub .= " AND ";
                }
                $SelectQuerySub .= "V3.\"SameSchTimeFlag\" = 'Y'";
            }else if($ConflictType==2){//Same Visit Time
                if($wherecon==false){
                    $SelectQuerySub .= " WHERE ";
                    $wherecon = true;
                }else{
                    $SelectQuerySub .= " AND ";
                }
                $SelectQuerySub .= "V3.\"SameVisitTimeFlag\" = 'Y'";
            }else if($ConflictType==3){//Sch And Visit Time Same
                if($wherecon==false){
                    $SelectQuerySub .= " WHERE ";
                    $wherecon = true;
                }else{
                    $SelectQuerySub .= " AND ";
                }
                $SelectQuerySub .= "V3.\"SchAndVisitTimeSameFlag\" = 'Y'";
            }else if($ConflictType==4){//Sch Over Another Sch Time
                if($wherecon==false){
                    $SelectQuerySub .= " WHERE ";
                    $wherecon = true;
                }else{
                    $SelectQuerySub .= " AND ";
                }
                $SelectQuerySub .= "V3.\"SchOverAnotherSchTimeFlag\" = 'Y'";
            }else if($ConflictType==5){//Visit Time Over Another Visit Time Type
                if($wherecon==false){
                    $SelectQuerySub .= " WHERE ";
                    $wherecon = true;
                }else{
                    $SelectQuerySub .= " AND ";
                }
                $SelectQuerySub .= "V3.\"VisitTimeOverAnotherVisitTimeFlag\" = 'Y'";
            }else if($ConflictType==6){//Sch Time Over Visit Time
                if($wherecon==false){
                    $SelectQuerySub .= " WHERE ";
                    $wherecon = true;
                }else{
                    $SelectQuerySub .= " AND ";
                }
                $SelectQuerySub .= "V3.\"SchTimeOverVisitTimeFlag\" = 'Y'";
            }else if($ConflictType==7){//Distance
                if($wherecon==false){
                    $SelectQuerySub .= " WHERE ";
                    $wherecon = true;
                }else{
                    $SelectQuerySub .= " AND ";
                }
                $SelectQuerySub .= "V3.\"DistanceFlag\" = 'Y'";
            }else if($ConflictType==8){//In-Service
                if($wherecon==false){
                    $SelectQuerySub .= " WHERE ";
                    $wherecon = true;
                }else{
                    $SelectQuerySub .= " AND ";
                }
                $SelectQuerySub .= "V3.\"InServiceFlag\" = 'Y'";
            }else if($ConflictType==9){//PTO
                if($wherecon==false){
                    $SelectQuerySub .= " WHERE ";
                    $wherecon = true;
                }else{
                    $SelectQuerySub .= " AND ";
                }
                $SelectQuerySub .= "V3.\"PTOFlag\" = 'Y'";
            }
            // else{//ELSE
            //     if($wherecon==false){
            //         $SelectQuerySub .= " WHERE ";
            //         $wherecon = true;
            //     }else{
            //         $SelectQuerySub .= " AND ";
            //     }
            //     $SelectQuerySub .= "(V3.\"SchOverAnotherSchTimeFlag\" = 'Y' OR V3.\"VisitTimeOverAnotherVisitTimeFlag\" = 'Y')";
            // }
        }
        // else{//ELSE
        //     if($wherecon==false){
        //         $SelectQuerySub .= " WHERE ";
        //         $wherecon = true;
        //     }else{
        //         $SelectQuerySub .= " AND ";
        //     }
        //     $SelectQuerySub .= "(V3.\"SchOverAnotherSchTimeFlag\" = 'Y' OR V3.\"VisitTimeOverAnotherVisitTimeFlag\" = 'Y')";
        // }
        if(isset($queryParams['VisitStartDate'])){
            $VisitStartDate = $queryParams['VisitStartDate'];
        }else{
            $VisitStartDate = '';
        }
        if(isset($queryParams['VisitEndDate'])){
            $VisitEndDate = $queryParams['VisitEndDate'];
        }else{
            $VisitEndDate = '';
        }
        if ($VisitStartDate && $VisitEndDate) {
            if($wherecon==false){
                    $SelectQuerySub .= " WHERE ";
                    $wherecon = true;
                }else{
                    $SelectQuerySub .= " AND ";
                }
                $SelectQuerySub .= "V3.\"VisitDate\" BETWEEN '$VisitStartDate' AND '$VisitEndDate'";
        }else if ($VisitStartDate && !$VisitEndDate) {
            if($wherecon==false){
                    $SelectQuerySub .= " WHERE ";
                    $wherecon = true;
                }else{
                    $SelectQuerySub .= " AND ";
                }
                $SelectQuerySub .= "V3.\"VisitDate\" >= '$VisitStartDate'";
        }else if (!$VisitStartDate && $VisitEndDate) {
            if($wherecon==false){
                    $SelectQuerySub .= " WHERE ";
                    $wherecon = true;
                }else{
                    $SelectQuerySub .= " AND ";
                }
                $SelectQuerySub .= "V3.\"VisitDate\" <= '$VisitEndDate'";
        }
        if(isset($queryParams['BilledStartDate'])){
            $BilledStartDate = $queryParams['BilledStartDate'];
        }else{
            $BilledStartDate = '';
        }
        if(isset($queryParams['BilledEndDate'])){
            $BilledEndDate = $queryParams['BilledEndDate'];
        }else{
            $BilledEndDate = '';
        }
        if ($BilledStartDate && $BilledEndDate) {
            if($wherecon==false){
                $SelectQuerySub .= " WHERE ";
                $wherecon = true;
            }else{
                $SelectQuerySub .= " AND ";
            }
            $SelectQuerySub .= "TO_CHAR(V3.\"BilledDate\", 'YYYY-MM-DD') BETWEEN '$BilledStartDate' AND '$BilledEndDate'";
        }else if ($BilledStartDate && !$BilledEndDate) {
            if($wherecon==false){
                $SelectQuerySub .= " WHERE ";
                $wherecon = true;
            }else{
                $SelectQuerySub .= " AND ";
            }
            $SelectQuerySub .= "TO_CHAR(V3.\"BilledDate\", 'YYYY-MM-DD') >= '$BilledStartDate'";
        }else if (!$BilledStartDate && $BilledEndDate) {
            if($wherecon==false){
                $SelectQuerySub .= " WHERE ";
                $wherecon = true;
            }else{
                $SelectQuerySub .= " AND ";
            }
            $SelectQuerySub .= "TO_CHAR(V3.\"BilledDate\", 'YYYY-MM-DD') <= '$BilledEndDate'";
        }
        //CReportedStartDate=2024-06-05&CReportedEndDate
        if(isset($queryParams['CReportedStartDate'])){
            $CReportedStartDate = $queryParams['CReportedStartDate'];
        }else{
            $CReportedStartDate = '';
        }
        if(isset($queryParams['CReportedEndDate'])){
            $CReportedEndDate = $queryParams['CReportedEndDate'];
        }else{
            $CReportedEndDate = '';
        }
        if ($CReportedStartDate && $CReportedEndDate) {
            if($wherecon==false){
                $SelectQuerySub .= " WHERE ";
                $wherecon = true;
            }else{
                $SelectQuerySub .= " AND ";
            }
            $SelectQuerySub .= "TO_CHAR(V3.\"G_CRDATEUNIQUE\", 'YYYY-MM-DD') BETWEEN '$CReportedStartDate' AND '$CReportedEndDate'";
        }else if ($CReportedStartDate && !$CReportedEndDate) {
            if($wherecon==false){
                $SelectQuerySub .= " WHERE ";
                $wherecon = true;
            }else{
                $SelectQuerySub .= " AND ";
            }
            $SelectQuerySub .= "TO_CHAR(V3.\"G_CRDATEUNIQUE\", 'YYYY-MM-DD') >= '$CReportedStartDate'";
        }else if (!$CReportedStartDate && $CReportedEndDate) {
            if($wherecon==false){
                $SelectQuerySub .= " WHERE ";
                $wherecon = true;
            }else{
                $SelectQuerySub .= " AND ";
            }
            $SelectQuerySub .= "TO_CHAR(V3.\"G_CRDATEUNIQUE\", 'YYYY-MM-DD') <= '$CReportedEndDate'";
        }
        if(isset($queryParams['AgingDays'])){
            $AgingDays = $queryParams['AgingDays'];
        }else{
            $AgingDays = '';
        }
        if ($AgingDays) {
            $AgingDays = is_numeric($AgingDays) ? $AgingDays : -99;
            if($wherecon==false){
                $SelectQuerySub .= " WHERE ";
                $wherecon = true;
            }else{
                $SelectQuerySub .= " AND ";
            }
            $SelectQuerySub .= "DATEDIFF(DAY, V3.\"CRDATEUNIQUE\", GETDATE()) = '$AgingDays'";
        }

        if(isset($queryParams['AideCode'])){
            $AideCode = $queryParams['AideCode'];
        }else{
            $AideCode = '';
        }
        if ($AideCode) {
            $SelectQuerySub .= " AND V3.\"AideCode\" ILIKE '%$AideCode%'";
        }
        if(isset($queryParams['AideLName'])){
            $AideLName = $queryParams['AideLName'];
        }else{
            $AideLName = '';
        }
        if ($AideLName) {
            $SelectQuerySub .= " AND V3.\"AideLName\" ILIKE '%$AideLName%'";
        }
        if(isset($queryParams['AideFName'])){
            $AideFName = $queryParams['AideFName'];
        }else{
            $AideFName = '';
        }
        if ($AideFName) {
            $SelectQuerySub .= " AND V3.\"AideFName\" ILIKE '%$AideFName%'";
        }
        $SelectQuery .= " INNER JOIN (".$SelectQuerySub.") AS V5 ON V5.\"GroupID\" = V1.\"GroupID\"";


        if($PayerID && $AppPayerID){
            $SelectQuery .= " WHERE V1.\"PayerID\" = '".$PayerID."'";
        }else{
            $SelectQuery .= " WHERE V1.\"PayerID\" = '-9999'";
        }    
        $SelectQuery .= " AND V1.\"GroupID\" IS NOT NULL";
        
        if(isset($queryParams['GroupID'])){
            $GroupID = $queryParams['GroupID'];
        }else{
            $GroupID = '';
        }
        if ($GroupID) {
            $GroupID = (int)$GroupID;
            $SelectQuery .= " AND V1.\"GroupID\" = TRY_TO_NUMBER('$GroupID')";
        }

        $NewQuery = 'WITH ';
        if(isset($ConPayerID) && $PayerID==$ConPayerID){
            $NewQuery .= "GroupConflicts AS (
                SELECT 
                    \"GroupID\",
                    COUNT(DISTINCT V1.\"CONFLICTID\") AS \"ConflictCount\"
                FROM CONFLICTREPORT_SANDBOX.PUBLIC.CONFLICTVISITMAPS V1
                INNER JOIN CONFLICTREPORT_SANDBOX.PUBLIC.CONFLICTS V2 
                    ON V2.\"CONFLICTID\" = V1.\"CONFLICTID\"
                WHERE 
                    V1.\"PayerID\" = '".$ConPayerID."''
                GROUP BY \"GroupID\"
                HAVING \"ConflictCount\" > 1
            ),";
        }
        $NewQuery .= "ConflictVisitMaps AS (".$SelectQuery.")";
        $query =  $NewQuery;
        $query .= "SELECT sq.* FROM ConflictVisitMaps sq";
        if(isset($ConPayerID) && $PayerID==$ConPayerID){
            $query .= " INNER JOIN GroupConflicts GC ON sq.\"GroupID\" = GC.\"GroupID\"";
        }
        $query .= " WHERE sq.RN = 1";
        $countquery = $NewQuery;
        $countquery .= "SELECT COUNT(DISTINCT sq.\"ID\") AS \"count\" FROM ConflictVisitMaps sq";
        if(isset($ConPayerID) && $PayerID==$ConPayerID){
            $countquery .= " INNER JOIN GroupConflicts GC ON sq.\"GroupID\" = GC.\"GroupID\"";
        }
        $countquery .= " WHERE sq.RN = 1";
        if(isset($queryParams['countrecdf'])){
            $statement_count = $this->conn->prepare($countquery);        
            $total_results = $statement_count->fetch(PDO::FETCH_ASSOC);
            return $total_results['count'];
        }
        if($SortByField && $SortByAD){
            $sortbyf = $SortByField;
            $sortby = $SortByAD;
        }else{
            $sortbyf = 'GroupID';
            $sortby = 'DESC';
        }
        $query .= " ORDER BY ".$sortbyf." ".$sortby."";
        $query .= " LIMIT $perPage OFFSET $offset";
        $statement = $this->conn->prepare($query);
        $results = $statement->fetchAll(PDO::FETCH_ASSOC);
        return $results;
    }

    public function getchildvisitsPayer($GroupIDs, $PayerID, $AppPayerID, $UserID=''){
    $WhereInQuery = '-999';
    $AllPayerFlagQ = false;
    $WhereInQuery = $PayerID;
    $subquerin1 = '';
    $subquerin = '';
    $subquerin1 = "CASE 
        WHEN V1.\"PayerID\" = '".$WhereInQuery."' THEN (V1.\"BilledRateMinute\" * 60)
        ELSE 0
    END AS \"BilledRate\",";
    $subquerin = "a.APID = '" . $WhereInQuery . "' AND ";
    $SQLChild = "SELECT DISTINCT 
        V1.\"CONFLICTID\",
        V1.\"VisitID\" AS \"AVID\",
        V1.\"GroupID\",
        V1.\"ProviderName\",
        DO.\"Federal Tax Number\" AS \"AgencyTIN\",
        DO.\"NPI\" AS \"AgencyNPI\",
        V1.\"Contract\",
        V1.\"PayerID\",
        V1.\"AppPayerID\",
        V1.\"PayerID\" AS \"APID\",
        V1.\"VisitStartTime\",
        V1.\"VisitEndTime\",
        V1.\"Office\",
        DATEDIFF(day, V1.\"CRDATEUNIQUE\", CURRENT_DATE) AS \"AgingDays\",
        ".$subquerin1."
        V1.\"BilledDate\",
        V1.\"BilledHours\",
        CASE 
    WHEN V1.\"InServiceFlag\" = 'Y' AND (V1.\"SchStartTime\" IS NULL OR V1.\"SchEndTime\" IS NULL)
        THEN TIMESTAMPDIFF(MINUTE, V1.\"ShVTSTTime\", V1.\"ShVTENTime\") / 60
    ELSE TIMESTAMPDIFF(MINUTE, V1.\"SchStartTime\", V1.\"SchEndTime\") / 60
END AS \"sch_hours\",

        V1.\"LastUpdatedBy\",
        V1.\"LastUpdatedDate\",
        V2.\"StatusFlag\",
        V2.\"StatusFlag\" AS \"OrgParentStatusFlag\",
        CVMCH.\"ShiftPrice\",
        CVMCH.\"OverlapPrice\",
        CVMCH.\"OverlapTime\",
        CASE 
            WHEN V2.\"StatusFlag\" IN ('R', 'D') THEN CVMCH.\"OverlapPrice\"
            ELSE 0
        END AS \"FinalPrice\",
        V1.\"PA_PAdmissionID\",
        V1.\"PA_PFName\",
        V1.\"PA_PLName\",
        V1.\"PA_PMedicaidNumber\",
        V1.\"AgencyContact\",
        V1.\"AgencyPhone\",
        V1.\"ShVTSTTime\",
        V1.\"ShVTENTime\",
        V1.\"SameSchTimeFlag\", V1.\"SameVisitTimeFlag\", V1.\"SchAndVisitTimeSameFlag\", 
        V1.\"SchOverAnotherSchTimeFlag\", V1.\"VisitTimeOverAnotherVisitTimeFlag\", 
        V1.\"SchTimeOverVisitTimeFlag\", V1.\"DistanceFlag\", V1.\"InServiceFlag\", V1.\"PTOFlag\",
        TIMESTAMPDIFF(MINUTE, V1.\"ShVTSTTime\", V1.\"ShVTENTime\") AS \"TotalMinutes\"   
        FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS V1
        INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTS AS V2 
            ON V2.\"CONFLICTID\" = V1.\"CONFLICTID\"
        LEFT JOIN ANALYTICS".$this->dbsuffix.".BI.DIMOFFICE AS DO 
        ON DO.\"Office Id\" = V1.\"OfficeID\"
        INNER JOIN (
            SELECT 
                a.\"GroupID\",
                a.\"CONFLICTID\",
                a.\"BilledRateMinute\",
                grp.\"GroupSize\",
                CASE 
                    WHEN a.\"BilledRateMinute\" = 0 OR a.\"APID\" <> '".$WhereInQuery."' THEN 0
                    ELSE CONFLICTREPORT".$this->dbsuffix.".PUBLIC.GETFULLSHIFTTIME(
                        a.\"BILLABLEMINUTESFULLSHIFT\", 
                        a.\"ShVTSTTime\", 
                        a.\"ShVTENTime\"
                    ) * a.\"BilledRateMinute\"
                END AS \"ShiftPrice\",
                SUM(
                    COALESCE(
                        CONFLICTREPORT".$this->dbsuffix.".PUBLIC.GETOVERLAPTIME(
                            NULL,
                            a.\"ShVTSTTime\",
                            a.\"ShVTENTime\",
                            b.\"ShVTSTTime\",
                            b.\"ShVTENTime\"
                        ), 0
                    )
                ) AS \"OverlapTime\",
                CASE 
                    WHEN a.\"BilledRateMinute\" = 0 OR a.\"APID\" <> '".$WhereInQuery."' THEN 0
                    WHEN grp.\"GroupSize\" = 2 AND a.\"BILLABLEMINUTESOVERLAP\" IS NOT NULL
                    THEN a.\"BILLABLEMINUTESOVERLAP\" * a.\"BilledRateMinute\"
                    ELSE SUM(
                        COALESCE(
                            CONFLICTREPORT".$this->dbsuffix.".PUBLIC.GETOVERLAPTIME(
                                NULL,
                                a.\"ShVTSTTime\",
                                a.\"ShVTENTime\",
                                b.\"ShVTSTTime\",
                                b.\"ShVTENTime\"
                            ), 0
                        )
                    ) * a.\"BilledRateMinute\"
                END AS \"OverlapPrice\"
            FROM (
                SELECT DISTINCT 
                    \"GroupID\", \"CONFLICTID\", \"ShVTSTTime\", \"ShVTENTime\",
                    \"BilledRateMinute\", \"BILLABLEMINUTESOVERLAP\", \"BILLABLEMINUTESFULLSHIFT\",
                    \"PayerID\" AS \"APID\"
                FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS 
                WHERE \"GroupID\" IN (".$GroupIDs.")
            ) a
            LEFT JOIN (
                SELECT DISTINCT 
                    \"GroupID\", \"CONFLICTID\", \"ShVTSTTime\", \"ShVTENTime\"
                FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS 
                WHERE \"GroupID\" IN (".$GroupIDs.")
            ) b
                ON a.\"GroupID\" = b.\"GroupID\" AND a.\"CONFLICTID\" <> b.\"CONFLICTID\"
            INNER JOIN (
                SELECT \"GroupID\", COUNT(DISTINCT \"CONFLICTID\") AS \"GroupSize\"
                FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS
                WHERE \"GroupID\" IN (".$GroupIDs.")
                GROUP BY \"GroupID\"
            ) grp 
                ON grp.\"GroupID\" = a.\"GroupID\"
            GROUP BY 
                a.\"GroupID\", a.\"CONFLICTID\", a.\"BilledRateMinute\", 
                a.\"BILLABLEMINUTESOVERLAP\", a.\"BILLABLEMINUTESFULLSHIFT\", 
                a.\"ShVTSTTime\", a.\"ShVTENTime\", grp.\"GroupSize\",a.\"APID\"
        ) AS CVMCH 
        ON CVMCH.\"GroupID\" = V1.\"GroupID\" AND CVMCH.\"CONFLICTID\" = V1.\"CONFLICTID\"
        ORDER BY V1.\"CONFLICTID\" ASC";
        $statement_ch = $this->conn->prepare($SQLChild);            
        $results = $statement_ch->fetchAll(PDO::FETCH_ASSOC);
        foreach ($results as &$row) {
            if (
                (empty($row['VisitStartTime']) || empty($row['VisitEndTime'])) &&
                isset($row['InServiceFlag']) && $row['InServiceFlag'] === 'Y'
            ) {
                $row['VisitStartTime'] = $row['ShVTSTTime'];
                $row['VisitEndTime'] = $row['ShVTENTime'];
            }
        }
        unset($row);
        
        // Map conflict types to user-friendly names for each result (like state portal)
        foreach ($results as &$result) {
            $result['ConTypes'] = $this->mapConflictTypes($result);
        }
        unset($result);
        
        return $results;
    }

    /**
     * Map conflict type flags to user-friendly names (like state portal)
     * 
     * @param array $result The result array with conflict type flags
     * @return string Comma-separated conflict type descriptions
     */
    private function mapConflictTypes($result)
    {
        $conTypes = [];
        
        // Check each conflict type flag and add appropriate description
        if (isset($result['SameSchTimeFlag']) && $result['SameSchTimeFlag'] == 'Y') {
            $conTypes[] = 'Exact Schedule Time Match';
        }
        if (isset($result['SameVisitTimeFlag']) && $result['SameVisitTimeFlag'] == 'Y') {
            $conTypes[] = 'Exact Visit Time Match';
        }
        if (isset($result['SchAndVisitTimeSameFlag']) && $result['SchAndVisitTimeSameFlag'] == 'Y') {
            $conTypes[] = 'Exact Schedule and Visit Time Match';
        }
        if (isset($result['SchOverAnotherSchTimeFlag']) && $result['SchOverAnotherSchTimeFlag'] == 'Y') {
            $conTypes[] = 'Schedule time overlap';
        }
        if (isset($result['VisitTimeOverAnotherVisitTimeFlag']) && $result['VisitTimeOverAnotherVisitTimeFlag'] == 'Y') {
            $conTypes[] = 'Visit Time Overlap';
        }
        if (isset($result['SchTimeOverVisitTimeFlag']) && $result['SchTimeOverVisitTimeFlag'] == 'Y') {
            $conTypes[] = 'Schedule and Visit time overlap';
        }
        if (isset($result['DistanceFlag']) && $result['DistanceFlag'] == 'Y') {
            $conTypes[] = 'Time-Distance';
        }
        if (isset($result['InServiceFlag']) && $result['InServiceFlag'] == 'Y') {
            $conTypes[] = 'In-Service';
        }
        if (isset($result['PTOFlag']) && $result['PTOFlag'] == 'Y') {
            $conTypes[] = 'PTO';
        }
        
        return implode(', ', $conTypes);
    }

    public function getOverLapTimePrice($ConflictIDs){
        $query_agency = "SELECT
        CVM.\"CONFLICTID\",
        SUM(CASE
            WHEN CVM.\"BilledRateMinute\" > 0 AND CVM.\"ConVisitStartTime\" IS NOT NULL AND CVM.\"VisitStartTime\" IS NOT NULL AND CVM.\"ConVisitEndTime\" IS NOT NULL AND CVM.\"VisitEndTime\" IS NOT NULL AND CVM.\"ConVisitStartTime\" >= CVM.\"VisitStartTime\" AND CVM.\"ConVisitStartTime\" <= CVM.\"VisitEndTime\" AND CVM.\"ConVisitEndTime\" > CVM.\"VisitEndTime\" THEN 1
            WHEN CVM.\"BilledRateMinute\" > 0 AND CVM.\"ConVisitStartTime\" IS NOT NULL AND CVM.\"VisitStartTime\" IS NOT NULL AND CVM.\"ConVisitEndTime\" IS NOT NULL AND CVM.\"VisitEndTime\" IS NOT NULL AND	CVM.\"ConVisitStartTime\" >= CVM.\"VisitStartTime\" AND CVM.\"ConVisitEndTime\" <= CVM.\"VisitEndTime\"	THEN 1
            WHEN CVM.\"BilledRateMinute\" > 0 AND CVM.\"ConVisitStartTime\" IS NOT NULL AND CVM.\"VisitStartTime\" IS NOT NULL AND CVM.\"ConVisitEndTime\" IS NOT NULL AND CVM.\"VisitEndTime\" IS NOT NULL AND	CVM.\"ConVisitStartTime\" < CVM.\"VisitStartTime\" AND CVM.\"ConVisitEndTime\" > CVM.\"VisitEndTime\" THEN 1
            ELSE 0
        END) AS \"ChildCount\",
        SUM(CASE
            WHEN CVM.\"BilledRateMinute\" > 0 AND CVM.\"ConVisitStartTime\" IS NOT NULL AND CVM.\"VisitStartTime\" IS NOT NULL AND CVM.\"ConVisitEndTime\" IS NOT NULL AND CVM.\"VisitEndTime\" IS NOT NULL AND CVM.\"ConVisitStartTime\" >= CVM.\"VisitStartTime\" AND CVM.\"ConVisitStartTime\" <= CVM.\"VisitEndTime\" AND CVM.\"ConVisitEndTime\" > CVM.\"VisitEndTime\" THEN TIMESTAMPDIFF(MINUTE, CVM.\"ConVisitStartTime\", CVM.\"VisitEndTime\") * CVM.\"BilledRateMinute\"
            WHEN CVM.\"BilledRateMinute\" > 0 AND CVM.\"ConVisitStartTime\" IS NOT NULL AND CVM.\"VisitStartTime\" IS NOT NULL AND CVM.\"ConVisitEndTime\" IS NOT NULL AND CVM.\"VisitEndTime\" IS NOT NULL AND	CVM.\"ConVisitStartTime\" >= CVM.\"VisitStartTime\" AND CVM.\"ConVisitEndTime\" <= CVM.\"VisitEndTime\"	THEN TIMESTAMPDIFF(MINUTE, CVM.\"ConVisitStartTime\", CVM.\"ConVisitEndTime\") * CVM.\"BilledRateMinute\"
            WHEN CVM.\"BilledRateMinute\" > 0 AND CVM.\"ConVisitStartTime\" IS NOT NULL AND CVM.\"VisitStartTime\" IS NOT NULL AND CVM.\"ConVisitEndTime\" IS NOT NULL AND CVM.\"VisitEndTime\" IS NOT NULL AND	CVM.\"ConVisitStartTime\" < CVM.\"VisitStartTime\" AND CVM.\"ConVisitEndTime\" > CVM.\"VisitEndTime\" THEN TIMESTAMPDIFF(MINUTE, CVM.\"VisitStartTime\", CVM.\"VisitEndTime\") * CVM.\"BilledRateMinute\"
            ELSE 0
        END) AS \"OverLapTotalPrice\",
        SUM(CASE
            WHEN CVM.\"BilledRateMinute\" > 0 AND CVM.\"ConVisitStartTime\" IS NOT NULL AND CVM.\"VisitStartTime\" IS NOT NULL AND CVM.\"ConVisitEndTime\" IS NOT NULL AND CVM.\"VisitEndTime\" IS NOT NULL AND CVM.\"ConVisitStartTime\" >= CVM.\"VisitStartTime\" AND CVM.\"ConVisitStartTime\" <= CVM.\"VisitEndTime\" AND CVM.\"ConVisitEndTime\" > CVM.\"VisitEndTime\" THEN TIMESTAMPDIFF(MINUTE, CVM.\"ConVisitStartTime\", CVM.\"VisitEndTime\")
            WHEN CVM.\"BilledRateMinute\" > 0 AND CVM.\"ConVisitStartTime\" IS NOT NULL AND CVM.\"VisitStartTime\" IS NOT NULL AND CVM.\"ConVisitEndTime\" IS NOT NULL AND CVM.\"VisitEndTime\" IS NOT NULL AND	CVM.\"ConVisitStartTime\" >= CVM.\"VisitStartTime\" AND CVM.\"ConVisitEndTime\" <= CVM.\"VisitEndTime\"	THEN TIMESTAMPDIFF(MINUTE, CVM.\"ConVisitStartTime\", CVM.\"ConVisitEndTime\")
            WHEN CVM.\"BilledRateMinute\" > 0 AND CVM.\"ConVisitStartTime\" IS NOT NULL AND CVM.\"VisitStartTime\" IS NOT NULL AND CVM.\"ConVisitEndTime\" IS NOT NULL AND CVM.\"VisitEndTime\" IS NOT NULL AND	CVM.\"ConVisitStartTime\" < CVM.\"VisitStartTime\" AND CVM.\"ConVisitEndTime\" > CVM.\"VisitEndTime\" THEN TIMESTAMPDIFF(MINUTE, CVM.\"VisitStartTime\", CVM.\"VisitEndTime\")
            ELSE 0
        END) AS \"OverLapTotalTime\"
        FROM
        CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS CVM
        WHERE
        CVM.\"CONFLICTID\" IN(".$ConflictIDs.")
        GROUP BY
        CVM.\"CONFLICTID\"";
        $statement = $this->conn->prepare($query_agency);
        $resultarr = $statement->fetchAll(PDO::FETCH_ASSOC);
        $DataArrayA = [];
        if(!empty($resultarr)){
            foreach($resultarr as $rowds){
                $DataArrayRetu = [];
                if(!empty($rowds['ChildCount']) && !empty($rowds['OverLapTotalPrice'])){
                    $DataArrayRetu['OverLapPrice'] = ($rowds['OverLapTotalPrice']/$rowds['ChildCount']);
                }else{
                    $DataArrayRetu['OverLapPrice'] = 0;
                }
                if(!empty($rowds['ChildCount']) && !empty($rowds['OverLapTotalPrice'])){
                    $DataArrayRetu['OverLapTime'] = ($rowds['OverLapTotalPrice']/$rowds['ChildCount']);
                }else{
                    $DataArrayRetu['OverLapTime'] = 0;
                }
                $DataArrayA[$rowds['CONFLICTID']] = $DataArrayRetu;
            }
        }
        return $DataArrayA;
    }

    public function get_fieldname_ajax($request, $LoggedInUserID = '')
    {
        $search = $request->q ?? '';
        $page = $request->page ?? 1;
        $TYPEID = $request->TYPEID=='C' ? 'C' : 'P';
        $pageSize = 50; // Number of results per page
        $offset = ($page - 1) * $pageSize;
        $query = "SELECT ID AS \"id\", \"FieldDisplayValue\" AS \"text\" FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.LOG_FIELDS WHERE \"FieldFor\" = '".$TYPEID."' AND \"NotShowInDropDown\" IS NULL";
        $query_count = "SELECT COUNT(\"ID\") AS \"count\" FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.LOG_FIELDS WHERE \"FieldFor\" = '".$TYPEID."' AND \"NotShowInDropDown\" IS NULL AND \"HideColumnFlag\" IS NULL";
        if(!empty($search)){
            $query .= " AND \"FieldDisplayValue\" ILIKE '%$search%'";
            $query_count .= " AND \"FieldDisplayValue\" ILIKE '%$search%'";
        }
        if($LoggedInUserID=='P'){
            $query .= " AND \"HideForProviderFlag\" IS NULL";
            $query_count .= " AND \"HideForProviderFlag\" IS NULL";
        }else if($LoggedInUserID=='PA'){
            $query .= " AND \"HideHidePayerFlag\" IS NULL";
            $query_count .= " AND \"HideHidePayerFlag\" IS NULL";
        }
        $query .= " ORDER BY \"FieldDisplayValue\" ASC";
        $query .= ' LIMIT '.$pageSize.' OFFSET '.$offset;
        $statement = $this->conn->prepare($query);
        $statement_count = $this->conn->prepare($query_count);

        $total_results = $statement_count->fetch(PDO::FETCH_ASSOC);
        $rowCount = $total_results['count'];
        $more = ($offset + $pageSize) < $rowCount;
        $results = $statement->fetchAll(PDO::FETCH_ASSOC);
        return response()->json(['items' => $results, 'more' => $more]);
    }

    public function get_payers_ajax($request, $PayerID = '', $AppPayerID = '', $ProviderID = '', $AppProviderID = '')
    {
        $search = $request->q ?? '';
        $showonlyconflict = $request->showonlyconflict ? true : false;
        $page = $request->page ?? 1;
        $pageSize = 50; // Number of results per page
        $offset = ($page - 1) * $pageSize;
        $query = "SELECT DISTINCT CONCAT(D.\"Payer Id\", '~', D.\"Application Payer Id\") AS \"id\", CONCAT(D.\"Payer Name\", ' (', D.\"Application Payer Id\", ')') AS \"text\" FROM ANALYTICS".$this->dbsuffix.".BI.DIMPAYER AS D";
        if($showonlyconflict && !empty($ProviderID) && !empty($AppProviderID)){
            $query .= " INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS CVM ON CVM.\"PayerID\" = D.\"Payer Id\" AND CVM.\"PayerID\" IS NOT NULL AND CVM.\"AppPayerID\" != '0' AND CVM.\"ProviderID\" = '".$ProviderID."'";            
            if($ofcquery = ofcquery()){
                $query .= " AND CVM.\"OfficeID\" IN (".$ofcquery.")";
            }
        }else if($showonlyconflict && !empty($PayerID) && !empty($AppPayerID)){
            $query .= " INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS CVM ON CVM.\"PayerID\" = D.\"Payer Id\" AND CVM.\"PayerID\" IS NOT NULL AND CVM.\"AppPayerID\" != '0' AND CVM.\"GroupID\" IN(SELECT DISTINCT V3.\"GroupID\" FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS V3 WHERE V3.\"PayerID\" = '".$PayerID."')";
        }
        $query .= " WHERE D.\"Is Active\" = TRUE AND D.\"Is Demo\" = FALSE";
        $query_count = "SELECT COUNT(DISTINCT D.\"Payer Id\") AS \"count\" FROM ANALYTICS".$this->dbsuffix.".BI.DIMPAYER AS D";
        if($showonlyconflict && !empty($ProviderID) && !empty($AppProviderID)){
            $query_count .= " INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS CVM ON CVM.\"PayerID\" = D.\"Payer Id\" AND CVM.\"PayerID\" IS NOT NULL AND CVM.\"AppPayerID\" != '0' AND CVM.\"ProviderID\" = '".$ProviderID."'";
            if($ofcquery = ofcquery()){
                $query_count .= " AND CVM.\"OfficeID\" IN (".$ofcquery.")";
            }
        }else if($showonlyconflict && !empty($PayerID) && !empty($AppPayerID)){
            $query_count .= " INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS CVM ON CVM.\"PayerID\" = D.\"Payer Id\" AND CVM.\"PayerID\" IS NOT NULL AND CVM.\"AppPayerID\" != '0' AND CVM.\"GroupID\" IN(SELECT DISTINCT V3.\"GroupID\" FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS V3 WHERE V3.\"PayerID\" = '".$PayerID."')";
        }
        $query_count .= " WHERE D.\"Is Active\" = TRUE AND D.\"Is Demo\" = FALSE";
        if(!empty($search)){
            $query .= " AND D.\"Payer Name\" ILIKE '%$search%'";
            $query_count .= " AND D.\"Payer Name\" ILIKE '%$search%'";
        }
        $query .= " ORDER BY CONCAT(D.\"Payer Name\", ' (', D.\"Application Payer Id\", ')') ASC";
        $query .= ' LIMIT '.$pageSize.' OFFSET '.$offset;
        $statement = $this->conn->prepare($query);
        $statement_count = $this->conn->prepare($query_count);

        $total_results = $statement_count->fetch(PDO::FETCH_ASSOC);
        $rowCount = $total_results['count'];
        $more = ($offset + $pageSize) < $rowCount;
        $results = $statement->fetchAll(PDO::FETCH_ASSOC);
        return response()->json(['items' => $results, 'more' => $more]);
    }

    public function get_payers_ajax_selected($PayerIDAppID)
    {
        //auth()->user()->hasRole('Payer') && 
        $PayerID = '-999';
        $ApplicationPayerId = '-999';
        if(!empty($PayerIDAppID)){
            if(!is_array($PayerIDAppID)){
            $PayerIDAppIDArr = explode('~', $PayerIDAppID);
            }else{
                $PayerIDAppIDArr = $PayerIDAppID;
            }
            if(!empty($PayerIDAppIDArr) && sizeof($PayerIDAppIDArr)==2){
                $PayerID = $PayerIDAppIDArr[0];
                $ApplicationPayerId = $PayerIDAppIDArr[1];
            }else if(!empty($PayerIDAppIDArr) && sizeof($PayerIDAppIDArr)==1){
                $PayerID = $PayerIDAppIDArr[0];
            }
        }
        $query = "SELECT CONCAT(\"Payer Id\", '~', \"Application Payer Id\") AS \"id\", CONCAT(\"Payer Name\", ' (', \"Application Payer Id\", ')') AS \"text\" FROM ANALYTICS".$this->dbsuffix.".BI.DIMPAYER WHERE \"Is Active\" = TRUE AND \"Is Demo\" = FALSE";
        $query .= " AND \"Payer Id\" = '$PayerID'";
        $query .= " ORDER BY CONCAT(\"Payer Name\", ' (', \"Application Payer Id\", ')') ASC";
        $statement = $this->conn->prepare($query);
        $results = $statement->fetchAll(PDO::FETCH_ASSOC);
        return $results;
    }

    public function get_offices_ajax($request)
    {
        $Provider_Id = Auth::user()->Provider_Id;
        $Application_Provider_Id = Auth::user()->Application_Provider_Id;
        $search = $request->q ?? '';
        $page = $request->page ?? 1;
        $pageSize = 50; // Number of results per page
        $offset = ($page - 1) * $pageSize;
        $query = "SELECT CONCAT(\"Office Id\", '~', \"Application Office Id\") AS \"id\", CONCAT(\"Office Name\", ' (', \"Application Office Id\", ')') AS \"text\" FROM ANALYTICS".$this->dbsuffix.".BI.DIMOFFICE WHERE \"Is Active\" = TRUE";

        if(auth()->user()->hasRole('Provider')){
            $query .= " AND \"Provider Id\" = '$Provider_Id'";
        }
        if($ofcquery = ofcquery()){
            $query .= " AND \"Office Id\" IN (".$ofcquery.")";
        }
        $query_count = "SELECT COUNT(\"Office Id\") AS \"count\" FROM ANALYTICS".$this->dbsuffix.".BI.DIMOFFICE WHERE \"Is Active\" = TRUE";
        if(auth()->user()->hasRole('Provider')){
            $query_count .= " AND \"Provider Id\" = '$Provider_Id'";
        }
        if($ofcquery = ofcquery()){
            $query_count .= " AND \"Office Id\" IN (".$ofcquery.")";
        }
        if(!empty($search)){
            $query .= " AND \"Office Name\" ILIKE '%$search%'";
            $query_count .= " AND \"Office Name\" ILIKE '%$search%'";
        }
        $query .= " ORDER BY CONCAT(\"Office Name\", ' (', \"Application Office Id\", ')') ASC";
        $query .= ' LIMIT '.$pageSize.' OFFSET '.$offset;
        $statement = $this->conn->prepare($query);
        $statement_count = $this->conn->prepare($query_count);

        $total_results = $statement_count->fetch(PDO::FETCH_ASSOC);
        $rowCount = $total_results['count'];
        $more = ($offset + $pageSize) < $rowCount;
        $results = $statement->fetchAll(PDO::FETCH_ASSOC);
        return response()->json(['items' => $results, 'more' => $more]);
    }

    public function get_offices_ajax_selected($OfficeIDAppID)
    {
        $Provider_Id = Auth::user()->Provider_Id;
        $Application_Provider_Id = Auth::user()->Application_Provider_Id;
        $OfficeID = '-999';
        $ApplicationOfficeID = '-999';
        if(!empty($OfficeIDAppID)){
            $OfficeIDAppIDArr = explode('~', $OfficeIDAppID);
            if(!empty($OfficeIDAppIDArr) && sizeof($OfficeIDAppIDArr)==2){
                $OfficeID = $OfficeIDAppIDArr[0];
                $ApplicationOfficeID = $OfficeIDAppIDArr[1];
            }else if(!empty($OfficeIDAppIDArr) && sizeof($OfficeIDAppIDArr)==1){
                $OfficeID = $OfficeIDAppIDArr[0];
            }
        }
        $query = "SELECT CONCAT(\"Office Id\", '~', \"Application Office Id\") AS \"id\", CONCAT(\"Office Name\", ' (', \"Application Office Id\", ')') AS \"text\" FROM ANALYTICS".$this->dbsuffix.".BI.DIMOFFICE WHERE \"Is Active\" = TRUE";
        if(auth()->user()->hasRole('Provider')){
            $query .= " AND \"Provider Id\" = '$Provider_Id'";
        }        
        if($ofcquery = ofcquery()){
            $query .= " AND \"Office Id\" IN (".$ofcquery.")";
        }
        $query .= " AND \"Office Id\" = '$OfficeID'";
        $query .= " ORDER BY CONCAT(\"Office Name\", ' (', \"Application Office Id\", ')') ASC";
        $statement = $this->conn->prepare($query);
        $results = $statement->fetchAll(PDO::FETCH_ASSOC);
        return $results;
    }

    public function get_conagency_ajax($request, $PayerID = '', $AppPayerID = '', $ProviderID = '', $AppProviderID = '')
    {
        if(!empty($PayerID) && !empty($AppPayerID)){
            $search = $request->q ?? '';
            $showonlyconflict = $request->showonlyconflict ? true : false;
            $page = $request->page ?? 1;
            $pageSize = 50; // Number of results per page
            $offset = ($page - 1) * $pageSize;
            $query = "SELECT DISTINCT CONCAT(D.\"Provider Id\", '~', D.\"Application Provider Id\") AS \"id\", CONCAT(D.\"Provider Name\", ' (', D.\"Application Provider Id\", ')') AS \"text\" FROM ANALYTICS".$this->dbsuffix.".BI.DIMPAYERPROVIDER AS C INNER JOIN ANALYTICS".$this->dbsuffix.".BI.DIMPROVIDER AS D ON D.\"Provider Id\" = C.\"Provider Id\"";
            if($showonlyconflict && !empty($PayerID) && !empty($AppPayerID)){
                $query .= " INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS CVM ON CVM.\"ProviderID\" = D.\"Provider Id\" AND CVM.\"ProviderID\" IS NOT NULL AND CVM.\"PayerID\" = '".$PayerID."'";
            }
            $query .= " WHERE C.\"Payer Id\" = '".$PayerID."' AND D.\"Is Active\" = TRUE AND D.\"Is Demo\" = FALSE";
            $query_count = "SELECT COUNT(DISTINCT D.\"Provider Id\") AS \"count\" FROM ANALYTICS".$this->dbsuffix.".BI.DIMPAYERPROVIDER AS C INNER JOIN ANALYTICS".$this->dbsuffix.".BI.DIMPROVIDER AS D ON D.\"Provider Id\" = C.\"Provider Id\"";
            if($showonlyconflict && !empty($PayerID) && !empty($AppPayerID)){
                $query_count .= " INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS CVM ON CVM.\"ProviderID\" = D.\"Provider Id\" AND CVM.\"ProviderID\" IS NOT NULL AND CVM.\"PayerID\" = '".$PayerID."'";
            }
            $query_count .= " WHERE C.\"Payer Id\" = '".$PayerID."' AND D.\"Is Active\" = TRUE AND D.\"Is Demo\" = FALSE";
            if(!empty($search)){
                $query .= " AND D.\"Provider Name\" ILIKE '%$search%'";
                $query_count .= " AND D.\"Provider Name\" ILIKE '%$search%'";
            }
            //$query .= " ORDER BY D.\"Provider Name\" ASC";
            $query .= " ORDER BY CONCAT(D.\"Provider Name\", ' (', D.\"Application Provider Id\", ')') ASC";

            // $query = "SELECT CONCAT(\"Provider Id\", '~', \"Application Provider Id\") AS \"id\", CONCAT(\"Provider Name\", ' (', \"Application Provider Id\", ')') AS \"text\" FROM ANALYTICS".$this->dbsuffix.".BI.DIMPROVIDER WHERE \"Is Active\" = TRUE AND \"Is Demo\" = FALSE";
            // $query_count = "SELECT COUNT(\"Provider Id\") AS \"count\" FROM ANALYTICS".$this->dbsuffix.".BI.DIMPROVIDER WHERE \"Is Active\" = TRUE AND \"Is Demo\" = FALSE";
            // if(!empty($search)){
            //     $query .= " AND \"Provider Name\" ILIKE '%$search%'";
            //     $query_count .= " AND \"Provider Name\" ILIKE '%$search%'";
            // }
            // $query .= " ORDER BY CONCAT(\"Provider Name\", ' (', \"Application Provider Id\", ')') ASC";
            $query .= ' LIMIT '.$pageSize.' OFFSET '.$offset;
            $statement = $this->conn->prepare($query);
            $statement_count = $this->conn->prepare($query_count);

            $total_results = $statement_count->fetch(PDO::FETCH_ASSOC);
            $rowCount = $total_results['count'];
            $more = ($offset + $pageSize) < $rowCount;
            $results = $statement->fetchAll(PDO::FETCH_ASSOC);
            return response()->json(['items' => $results, 'more' => $more]);
        }else{
            $search = $request->q ?? '';
            $showonlyconflict = $request->showonlyconflict ? true : false;
            $page = $request->page ?? 1;
            $pageSize = 50; // Number of results per page
            $offset = ($page - 1) * $pageSize;
            $query = "SELECT DISTINCT CONCAT(D.\"Provider Id\", '~', D.\"Application Provider Id\") AS \"id\", CONCAT(D.\"Provider Name\", ' (', D.\"Application Provider Id\", ')') AS \"text\" FROM ANALYTICS".$this->dbsuffix.".BI.DIMPROVIDER AS D";
            if($showonlyconflict && !empty($ProviderID) && !empty($AppProviderID)){
                $query .= " INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS CVM ON CVM.\"ConProviderID\" = D.\"Provider Id\" AND CVM.\"ConProviderID\" IS NOT NULL AND CVM.\"ProviderID\" = '".$ProviderID."'";
                if($ofcquery = ofcquery()){
                    $query .= " AND CVM.\"OfficeID\" IN (".$ofcquery.")";
                }
            }
            $query .= " WHERE D.\"Is Active\" = TRUE AND D.\"Is Demo\" = FALSE";
            $query_count = "SELECT COUNT(DISTINCT D.\"Provider Id\") AS \"count\" FROM ANALYTICS".$this->dbsuffix.".BI.DIMPROVIDER AS D";
            if($showonlyconflict && !empty($ProviderID) && !empty($AppProviderID)){
                $query_count .= " INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS C ON C.\"ConProviderID\" = D.\"Provider Id\" AND C.\"ConProviderID\" IS NOT NULL AND C.\"ProviderID\" = '".$ProviderID."'";
                if($ofcquery = ofcquery()){
                    $query_count .= " AND C.\"OfficeID\" IN (".$ofcquery.")";
                }
            }
            $query_count .= " WHERE D.\"Is Active\" = TRUE AND D.\"Is Demo\" = FALSE";
            if(!empty($search)){
                $query .= " AND D.\"Provider Name\" ILIKE '%$search%'";
                $query_count .= " AND D.\"Provider Name\" ILIKE '%$search%'";
            }
            $query .= " ORDER BY CONCAT(D.\"Provider Name\", ' (', D.\"Application Provider Id\", ')') ASC";
            $query .= ' LIMIT '.$pageSize.' OFFSET '.$offset;
            $statement = $this->conn->prepare($query);
            $statement_count = $this->conn->prepare($query_count);

            $total_results = $statement_count->fetch(PDO::FETCH_ASSOC);
            $rowCount = $total_results['count'];
            $more = ($offset + $pageSize) < $rowCount;
            $results = $statement->fetchAll(PDO::FETCH_ASSOC);
            return response()->json(['items' => $results, 'more' => $more]);
        }
    }

    public function get_conagency_ajax_selected($ProviderIDAppID)
    {
        $ProviderID = '-999';
        $ApplicationProviderId = '-999';
        if(!empty($ProviderIDAppID)){
            if(!is_array($ProviderIDAppID)){
                $ProviderIDAppIDArr = explode('~', $ProviderIDAppID);
            }else{
                $ProviderIDAppIDArr = $ProviderIDAppID;
            }
            if(!empty($ProviderIDAppIDArr) && sizeof($ProviderIDAppIDArr)==2){
                $ProviderID = $ProviderIDAppIDArr[0];
                $ApplicationProviderId = $ProviderIDAppIDArr[1];
            }else if(!empty($ProviderIDAppIDArr) && sizeof($ProviderIDAppIDArr)==1){
                $ProviderID = $ProviderIDAppIDArr[0];
            }
        }
        $query = "SELECT CONCAT(\"Provider Id\", '~', \"Application Provider Id\") AS \"id\", CONCAT(\"Provider Name\", ' (', \"Application Provider Id\", ')') AS \"text\" FROM ANALYTICS".$this->dbsuffix.".BI.DIMPROVIDER WHERE \"Is Active\" = TRUE AND \"Is Demo\" = FALSE";
        $query .= " AND \"Provider Id\" = '$ProviderID'";
        $query .= " ORDER BY CONCAT(\"Provider Name\", ' (', \"Application Provider Id\", ')') ASC";
        $statement = $this->conn->prepare($query);
        $results = $statement->fetchAll(PDO::FETCH_ASSOC);
        return $results;
    }

    public function get_conagencytin_ajax($request, $PayerID = '', $AppPayerID = '', $ProviderID = '', $AppProviderID = '')
    {
        if(!empty($PayerID)){
            $search = $request->q ?? '';
            $page = $request->page ?? 1;
            $showonlyconflict = $request->showonlyconflict ? true : false;
            $pageSize = 50; // Number of results per page
            $offset = ($page - 1) * $pageSize;
            $query = "SELECT DISTINCT D.\"Federal Tax Number\" AS \"id\", D.\"Federal Tax Number\" AS \"text\" FROM ANALYTICS".$this->dbsuffix.".BI.DIMPAYERPROVIDER AS C INNER JOIN ANALYTICS".$this->dbsuffix.".BI.DIMPROVIDER AS D ON D.\"Provider Id\" = C.\"Provider Id\"";
            if($showonlyconflict && !empty($PayerID)){
                $query .= " INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS CVM ON CVM.\"ProviderID\" = D.\"Provider Id\" AND CVM.\"ProviderID\" IS NOT NULL AND CVM.\"PayerID\" = '".$PayerID."'";
            }
            $query .= " WHERE C.\"Payer Id\" = '".$PayerID."' AND D.\"Is Active\" = TRUE AND D.\"Is Demo\" = FALSE";
            $query_count = "SELECT COUNT(DISTINCT D.\"Federal Tax Number\") AS \"count\" FROM ANALYTICS".$this->dbsuffix.".BI.DIMPAYERPROVIDER AS C INNER JOIN ANALYTICS".$this->dbsuffix.".BI.DIMPROVIDER AS D ON D.\"Provider Id\" = C.\"Provider Id\"";
            if($showonlyconflict && !empty($PayerID)){
                $query_count .= " INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS CVM ON CVM.\"ProviderID\" = D.\"Provider Id\" AND CVM.\"ProviderID\" IS NOT NULL AND CVM.\"PayerID\" = '".$PayerID."'";
            }
            $query_count .= " WHERE C.\"Payer Id\" = '".$PayerID."' AND D.\"Is Active\" = TRUE AND D.\"Is Demo\" = FALSE";
            if(!empty($search)){
                $query .= " AND D.\"Federal Tax Number\" ILIKE '%$search%'";
                $query_count .= " AND D.\"Federal Tax Number\" ILIKE '%$search%'";
            }
            $query .= " ORDER BY D.\"Federal Tax Number\" ASC";
            $query .= ' LIMIT '.$pageSize.' OFFSET '.$offset;
            $statement = $this->conn->prepare($query);
            $statement_count = $this->conn->prepare($query_count);

            $total_results = $statement_count->fetch(PDO::FETCH_ASSOC);
            $rowCount = $total_results['count'];
            $more = ($offset + $pageSize) < $rowCount;
            $results = $statement->fetchAll(PDO::FETCH_ASSOC);
            return response()->json(['items' => $results, 'more' => $more]);
        }else{
            $search = $request->q ?? '';
            $page = $request->page ?? 1;
            $showonlyconflict = $request->showonlyconflict ? true : false;
            $pageSize = 50; // Number of results per page
            $offset = ($page - 1) * $pageSize;
            $query = "SELECT DISTINCT D.\"Federal Tax Number\" AS \"id\", D.\"Federal Tax Number\" AS \"text\" FROM ANALYTICS".$this->dbsuffix.".BI.DIMPROVIDER AS D";
            if($showonlyconflict && !empty($ProviderID)){
                $query .= " INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS CVM ON CVM.\"ConProviderID\" = D.\"Provider Id\" AND CVM.\"ConProviderID\" IS NOT NULL AND CVM.\"ProviderID\" = '".$ProviderID."'";                
                if($ofcquery = ofcquery()){
                    $query .= " AND CVM.\"OfficeID\" IN (".$ofcquery.")";
                }
            }
            $query .= " WHERE D.\"Is Active\" = TRUE AND D.\"Is Demo\" = FALSE";
            $query_count = "SELECT COUNT(DISTINCT D.\"Provider Id\") AS \"count\" FROM ANALYTICS".$this->dbsuffix.".BI.DIMPROVIDER AS D";
            if($showonlyconflict && !empty($ProviderID)){
                $query_count .= " INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS CVM ON CVM.\"ConProviderID\" = D.\"Provider Id\" AND CVM.\"ConProviderID\" IS NOT NULL AND CVM.\"ProviderID\" = '".$ProviderID."'";                
                if($ofcquery = ofcquery()){
                    $query_count .= " AND CVM.\"OfficeID\" IN (".$ofcquery.")";
                }
            }
            $query_count .= " WHERE \"Is Active\" = TRUE AND \"Is Demo\" = FALSE";
            if(!empty($search)){
                $query .= " AND D.\"Federal Tax Number\" ILIKE '%$search%'";
                $query_count .= " AND D.\"Federal Tax Number\" ILIKE '%$search%'";
            }
            $query .= " ORDER BY D.\"Federal Tax Number\" ASC";
            $query .= ' LIMIT '.$pageSize.' OFFSET '.$offset;
            $statement = $this->conn->prepare($query);
            $statement_count = $this->conn->prepare($query_count);

            $total_results = $statement_count->fetch(PDO::FETCH_ASSOC);
            $rowCount = $total_results['count'];
            $more = ($offset + $pageSize) < $rowCount;
            $results = $statement->fetchAll(PDO::FETCH_ASSOC);
            return response()->json(['items' => $results, 'more' => $more]);
        }
    }

    public function get_conagencytin_ajax_selected($FederalTaxNumber)
    {
        $FederalTaxNumberS = '-999';
        if(!empty($FederalTaxNumber)){
            $FederalTaxNumberS = $FederalTaxNumber;
        }
        $query = "SELECT \"Federal Tax Number\" AS \"id\", \"Federal Tax Number\" AS \"text\" FROM ANALYTICS".$this->dbsuffix.".BI.DIMPROVIDER WHERE \"Is Active\" = TRUE AND \"Is Demo\" = FALSE";
        $query .= " AND \"Federal Tax Number\" = '$FederalTaxNumberS'";
        $query .= " ORDER BY \"Federal Tax Number\" ASC";
        $statement = $this->conn->prepare($query);
        $results = $statement->fetchAll(PDO::FETCH_ASSOC);
        return $results;
    }

    public function get_noresponsereason()
    {
        $query = "SELECT \"ID\" AS \"id\", \"Title\" AS \"text\" FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.NORESPONSEREASONS";
        $query .= " ORDER BY \"Title\" ASC";
        $statement = $this->conn->prepare($query);
        $results = $statement->fetchAll(PDO::FETCH_ASSOC);
        return $results;
    }

    public function visitdetail($CONFLICTID, $ProviderID, $AppProviderID, $UserID){
        $query = "SELECT TOP 1 V1.\"ID\", V1.\"CONFLICTID\", V1.\"SSN\", V1.\"ProviderID\", V1.\"AppProviderID\", V1.\"ProviderName\", V1.\"VisitID\", V1.\"AppVisitID\", V1.\"VisitDate\", V1.\"SchStartTime\", V1.\"SchEndTime\", V1.\"VisitStartTime\", V1.\"VisitEndTime\", V1.\"EVVStartTime\", V1.\"EVVEndTime\", V1.\"CaregiverID\", V1.\"AppCaregiverID\", V1.\"AideCode\", V1.\"AideName\", V1.\"AideFName\", V1.\"AideLName\", COALESCE(V1.\"AideSSN\", V1.\"SSN\") AS \"AideSSN\", V1.\"OfficeID\", V1.\"AppOfficeID\", V1.\"Office\", V1.\"P_PatientID\", V1.\"P_AppPatientID\", 

        V1.\"P_PAdmissionID\", V1.\"P_PName\", V1.\"P_PFName\", V1.\"P_PLName\", V1.\"P_PMedicaidNumber\", V1.\"P_PAddressID\", V1.\"P_PAppAddressID\", V1.\"P_PAddressL1\", V1.\"P_PAddressL2\", V1.\"P_PCity\", V1.\"P_PAddressState\", V1.\"P_PZipCode\", V1.\"P_PCounty\", V1.\"P_PStatus\",
        
        V1.\"PLongitude\", V1.\"PLatitude\", V1.\"PayerID\", V1.\"AppPayerID\", V1.\"Contract\", V1.\"BilledDate\", V1.\"BilledHours\", V1.\"Billed\", V1.\"ServiceCodeID\", V1.\"AppServiceCodeID\", V1.\"RateType\", V1.\"ServiceCode\", 
        TIMESTAMPDIFF(MINUTE, V1.\"SchStartTime\", V1.\"SchEndTime\") / 60 AS \"sch_hours\",
         (V1.\"BilledRateMinute\"*60) AS \"BilledRate\", V1.\"TotalBilledAmount\", V1.\"IsMissed\", V1.\"MissedVisitReason\", V1.\"EVVType\", V1.\"InServiceFlag\", V1.\"PTOFlag\", V1.\"PStatus\", V1.\"AideStatus\",
        CASE
           WHEN V2.\"StatusFlag\" IN ('D', 'R') THEN 'R'
           ELSE V2.\"StatusFlag\"
        END AS \"ParentStatusFlag\",
        V2.\"StatusFlag\" AS \"OrgParentStatusFlag\", V2.\"NoResponseFlag\", V2.\"NoResponseTitle\", V2.\"NoResponseNotes\", CONCAT(V1.\"VisitID\", '~',V1.\"AppVisitID\") as \"VAPPID\",
        CONCAT(V1.\"VisitID\", '~', V1.\"AppVisitID\") as \"APatientAPPID\",
        V1.\"AgencyPhone\",
        V1.\"AgencyContact\",
        V1.\"BillRateBoth\",
        DATEDIFF(DAY, V1.\"CRDATEUNIQUE\", GETDATE()) AS \"AgeInDays\",
        V1.\"LastUpdatedBy\", V1.\"LastUpdatedDate\",
        CASE 
            WHEN 
            V1.\"BilledRateMinute\" > 0 THEN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.GETFULLSHIFTTIME(V1.\"BILLABLEMINUTESFULLSHIFT\", V1.\"ShVTSTTime\", V1.\"ShVTENTime\") * V1.\"BilledRateMinute\"
            ELSE 0
        END AS \"ShiftPrice\",
        V1.\"ReverseUUID\",
        CASE 
            WHEN DATEDIFF(DAY, V1.\"CRDATEUNIQUE\", GETDATE()) > SETT.NORESPONSELIMITTIME THEN TRUE
            ELSE FALSE
        END AS ALLOWDELETE, V2.\"FlagForReview\"
         FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS V1 INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTS AS V2 ON V2.\"CONFLICTID\"=V1.\"CONFLICTID\" CROSS JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.\"SETTINGS\" AS SETT";
        $query .= " WHERE V1.\"ProviderID\" = '".$ProviderID."'";
        if($ofcquery = ofcquery()){
            $query .= " AND V1.\"OfficeID\" IN (".$ofcquery.")";
        }
        $query .= " AND V1.\"CONFLICTID\" = '$CONFLICTID'";
        $statement = $this->conn->prepare($query);
        return $statement->fetch(PDO::FETCH_ASSOC);
    }

    public function VisitDetailConflictData($CONFLICTID, $ProviderID, $AppProviderID, $UserID){
        $query_ch = "SELECT V1.\"ID\", V1.\"CONFLICTID\", V1.\"GroupID\", V3.\"CONFLICTID\" AS \"RefCONFLICTID\", V1.\"SSN\", V1.\"ConProviderID\", V1.\"ConAppProviderID\", V1.\"ConProviderName\", V1.\"ConSchStartTime\", V1.\"ConSchEndTime\", V1.\"ConVisitStartTime\", V1.\"ConVisitEndTime\", V1.\"ConEVVStartTime\", V1.\"ConEVVEndTime\", V1.\"SameSchTimeFlag\", V1.\"SameVisitTimeFlag\", V1.\"SchAndVisitTimeSameFlag\", V1.\"SchOverAnotherSchTimeFlag\", V1.\"VisitTimeOverAnotherVisitTimeFlag\", V1.\"SchTimeOverVisitTimeFlag\", V1.\"DistanceFlag\", V1.\"InServiceFlag\", V1.\"PTOFlag\", V1.\"ConAgencyContact\", V1.\"ConAgencyPhone\", V1.\"ConLastUpdatedBy\", V1.\"ConLastUpdatedDate\",
         CASE 
            WHEN V1.\"InServiceFlag\" = 'Y' AND (V1.\"ConSchStartTime\" IS NULL OR V1.\"ConSchEndTime\" IS NULL)
                THEN TIMESTAMPDIFF(MINUTE, V1.\"ShVTSTTime\", V1.\"ShVTENTime\") / 60
            ELSE TIMESTAMPDIFF(MINUTE, V1.\"ConSchStartTime\", V1.\"ConSchEndTime\") / 60
        END AS \"sch_hours\",
          V1.\"DistanceMilesFromLatLng\", V1.\"ETATravleMinutes\", V1.\"ConServiceCode\", (V1.\"ConBilledRateMinute\"*60) AS \"ConBilledRate\", V1.\"ConTotalBilledAmount\", V1.\"ConContract\", V1.\"ConIsMissed\", V1.\"ConMissedVisitReason\", V1.\"ConEVVType\",
        CASE
           WHEN V1.\"StatusFlag\" IN ('D', 'R') THEN 'R'
           ELSE V1.\"StatusFlag\"
        END AS \"StatusFlag\",
        V1.\"StatusFlag\" AS \"OrgStatusFlag\", V1.\"ConPLongitude\", V1.\"ConPLatitude\", 
        
        V1.\"ConPAddressL1\", V1.\"ConPAddressL2\", V1.\"ConPCity\", V1.\"ConPAddressState\", V1.\"ConPZipCode\", V1.\"ConPCounty\",

        V1.\"ConP_PAdmissionID\", V1.\"ConP_PName\", V1.\"ConP_PFName\", V1.\"ConP_PLName\", V1.\"ConP_PMedicaidNumber\", V1.\"ConP_PAddressID\", V1.\"ConP_PAppAddressID\", V1.\"ConP_PAddressL1\", V1.\"ConP_PAddressL2\", V1.\"ConP_PCity\", V1.\"ConP_PAddressState\", V1.\"ConP_PZipCode\", V1.\"ConP_PCounty\", V1.\"ConP_PStatus\",
        
        V1.\"ConVisitID\", V1.\"ConAppVisitID\", CONCAT(V1.\"VisitID\", '~',V1.\"AppVisitID\") as \"VAPPID\", CONCAT(V1.\"ConVisitID\", '~',V1.\"ConAppVisitID\") as \"ConVAPPID\",        
        CONCAT(V1.\"VisitID\", '~', V1.\"AppVisitID\") as \"APatientAPPID\",
        CONCAT(V1.\"ConVisitID\", '~', V1.\"ConAppVisitID\") as \"ConAPatientAPPID\",
        DATEDIFF(DAY, V1.\"CRDATEUNIQUE\", GETDATE()) AS \"AgeInDays\",
        V1.\"ResolveDate\",
        CASE 
            WHEN  V1.\"BilledRateMinute\" > 0 
            THEN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.GETOVERLAPTIME(
                V1.\"BILLABLEMINUTESOVERLAP\", 
                V1.\"ShVTSTTime\", 
                V1.\"ShVTENTime\", 
                V1.\"CShVTSTTime\", 
                V1.\"CShVTENTime\"
            ) * V1.\"BilledRateMinute\"
            ELSE 0
        END AS \"OverLapPrice\",
        CONFLICTREPORT".$this->dbsuffix.".PUBLIC.GETOVERLAPTIME(
                V1.\"BILLABLEMINUTESOVERLAP\", 
                V1.\"ShVTSTTime\", 
                V1.\"ShVTENTime\", 
                V1.\"CShVTSTTime\", 
                V1.\"CShVTENTime\"
            )
         AS \"OverLapTime\",
        CASE 
            WHEN 
            V1.\"BilledRateMinute\" > 0 THEN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.GETFULLSHIFTTIME(V1.\"BILLABLEMINUTESFULLSHIFT\", V1.\"ShVTSTTime\", V1.\"ShVTENTime\") * V1.\"BilledRateMinute\"
            ELSE 0
        END AS \"ShiftPrice\",
         CASE
           WHEN V1.\"BilledRateMinute\" > 0 AND V2.\"StatusFlag\" IN ('R', 'D') THEN
                CONFLICTREPORT".$this->dbsuffix.".PUBLIC.GETOVERLAPTIME(
                    V1.\"BILLABLEMINUTESOVERLAP\",
                    V1.\"ShVTSTTime\",
                    V1.\"ShVTENTime\",
                    V1.\"CShVTSTTime\",
                   V1.\"CShVTENTime\"
                ) * V1.\"BilledRateMinute\"
             ELSE 0
         END AS \"FinalPrice\",
        V1.\"ConNoResponseFlag\",
        V1.\"ConBilled\",
        V1.\"ReverseUUID\"
        FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS V1 
        INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTS AS V2 ON V2.\"CONFLICTID\"=V1.\"CONFLICTID\"
        LEFT JOIN CONFLICTREPORT_SANDBOX.PUBLIC.CONFLICTVISITMAPS AS V3 ON CONCAT(V3.\"ProviderID\", '~', V3.\"AppProviderID\")=CONCAT(V1.\"ConProviderID\", '~', V1.\"ConAppProviderID\")
        AND CONCAT(V3.\"ConProviderID\", '~', V3.\"ConAppProviderID\")=CONCAT(V1.\"ProviderID\", '~', V1.\"AppProviderID\") AND V3.\"GroupID\"=V1.\"GroupID\"";
        $query_ch .= " WHERE V1.\"ProviderID\" = '".$ProviderID."'";
        if($ofcquery = ofcquery()){
            $query_ch .= " AND V1.\"OfficeID\" IN (".$ofcquery.")";
        }
        $query_ch .= " AND V1.\"CONFLICTID\" = '".$CONFLICTID."'";
        $statement_ch = $this->conn->prepare($query_ch);
        
        return $statement_ch->fetchAll(PDO::FETCH_ASSOC);
    }

    public function PTOByCID($CONFLICTID, $ProviderID, $AppProviderID, $UserID){
        $query_PTO = "SELECT DISTINCT V1.\"PTOFlag\" FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS V1";
        $query_PTO .= " WHERE V1.\"ProviderID\" = '".$ProviderID."'";
        $query_PTO .= " AND V1.\"CONFLICTID\" = '$CONFLICTID' AND \"PTOFlag\"='Y'";
        $statement_PTO = $this->conn->prepare($query_PTO);
        return $statement_PTO->fetch(PDO::FETCH_ASSOC);
    }

    public function InServiceByCID($CONFLICTID, $ProviderID, $AppProviderID, $UserID){
        $query_INS = "SELECT DISTINCT V1.\"InServiceFlag\" FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS V1";
        $query_INS .= " WHERE V1.\"ProviderID\" = '".$ProviderID."'";
        $query_INS .= " AND V1.\"CONFLICTID\" = '$CONFLICTID' AND \"InServiceFlag\"='Y'";
        $statement_INS = $this->conn->prepare($query_INS);
        return $statement_INS->fetch(PDO::FETCH_ASSOC);
    }

    public function visitdetailPayer($GroupID, $PayerID, $AppPayerID, $UserID){

        $GroupID = (int)$GroupID;

        $query = "SELECT TOP 1 V1.\"GroupID\", V1.\"SSN\", V1.\"CaregiverID\", V1.\"AppCaregiverID\", \"VisitID\", V1.\"AppVisitID\", V1.\"VisitDate\", V1.\"CaregiverID\", V1.\"AppCaregiverID\", V1.\"AideCode\", V1.\"AideFName\", V1.\"AideLName\",COALESCE(V1.\"AideSSN\", V1.\"SSN\") AS \"AideSSN\", V1.\"CRDATEUNIQUE\", V1.\"InServiceFlag\", V1.\"PTOFlag\", V1.\"FlagForReview\", V1.\"PayerID\" AS APID FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS V1 INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTS AS V2 ON V2.\"CONFLICTID\"=V1.\"CONFLICTID\"";
        // $countquery = "SELECT COUNT(DISTINCT V1.\"GroupID\") AS \"count\" FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS V1 INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTS AS V2 ON V2.\"CONFLICTID\"=V1.\"CONFLICTID\"";
        
        if(auth()->user()->hasRole('Payer') && $PayerID){
            $query .= " WHERE V1.\"PayerID\" = '".$PayerID."'";
            // $countquery .= " WHERE V1.\"PayerID\" = '".$PayerID."'";
        }else{
            $query .= " WHERE V1.\"PayerID\" = '-9999'";
            // $countquery .= " WHERE V1.\"PayerID\" = '-9999'";
        }
        $query .= " AND V1.\"GroupID\" IS NOT NULL";
        $query .= " AND V1.\"GroupID\" = $GroupID";
        $statement = $this->conn->prepare($query);
        return $statement->fetch(PDO::FETCH_ASSOC);
    }

public function VisitDetailConflictDataPayer($GroupID, $PayerID, $AppPayerID, $UserID){
    $WhereInQuery = '-999';
    if(auth()->user()->hasRole('Payer')){
        $WhereInQuery = $PayerID;
    }
    $subquerin1 = "CASE 
        WHEN V1.\"PayerID\" = '".$WhereInQuery."' THEN (V1.\"BilledRateMinute\"*60)
        ELSE 0
    END AS \"BilledRate\",
    CASE 
        WHEN V1.\"PayerID\" = '".$WhereInQuery."' THEN V1.\"BilledHours\"
        ELSE 0
    END AS \"BilledHours\",
    CASE 
        WHEN V1.\"PayerID\" = '".$WhereInQuery."' THEN V1.\"TotalBilledAmount\"
        ELSE 0
    END AS \"TotalBilledAmount\",";
    $subquerin = "a.APID = '" . $WhereInQuery . "' AND ";
    $SQLChild = "SELECT DISTINCT V1.\"CONFLICTID\",
        V1.\"VisitID\" AS \"AVID\",
        V1.\"GroupID\",
        V1.\"ProviderName\",
        V1.\"ProviderID\",
        V1.\"AppProviderID\",
        V1.\"PayerID\",
        V1.\"AppPayerID\",
        V1.\"PayerID\" AS APID,
        V1.\"Contract\",
        V1.\"VisitDate\",
        V1.\"SchStartTime\",
        V1.\"SchEndTime\",
        V1.\"VisitStartTime\",
        V1.\"VisitEndTime\",
        V1.\"EVVStartTime\",
        V1.\"EVVEndTime\",
        V1.\"Office\",
        DATEDIFF(day, V1.\"CRDATEUNIQUE\", CURRENT_DATE) AS \"AgingDays\",
        V1.\"BilledDate\",
        ".$subquerin1."
        CASE 
            WHEN V1.\"InServiceFlag\" = 'Y' AND (V1.\"SchStartTime\" IS NULL OR V1.\"SchEndTime\" IS NULL)
                THEN TIMESTAMPDIFF(MINUTE, V1.\"ShVTSTTime\", V1.\"ShVTENTime\") / 60
            ELSE TIMESTAMPDIFF(MINUTE, V1.\"SchStartTime\", V1.\"SchEndTime\") / 60
        END AS \"sch_hours\",
        V1.\"LastUpdatedBy\",
        V1.\"LastUpdatedDate\",
        V2.\"StatusFlag\",
        V2.\"StatusFlag\" AS \"OrgParentStatusFlag\",
        CVMCH.\"ShiftPrice\",
        CVMCH.\"OverlapPrice\",
        CVMCH.\"OverlapTime\",
        CVMCH.\"GroupSize\",
        CVMCH.\"BILLABLEMINUTESOVERLAP\",
        CASE 
            WHEN V2.\"StatusFlag\" IN ('R', 'D') THEN CVMCH.\"OverlapPrice\"
            ELSE 0
        END AS \"FinalPrice\",
        V1.\"PA_PAdmissionID\",
        V1.\"PA_PFName\",
        V1.\"PA_PLName\",
        V1.\"PA_PMedicaidNumber\",
        V1.\"PA_PAddressL1\",
        V1.\"PA_PAddressL2\",
        V1.\"PA_PCity\",
        V1.\"PA_PAddressState\",
        V1.\"PA_PZipCode\",
        V1.\"PA_PCounty\",
        V1.\"AgencyContact\",
        V1.\"AgencyPhone\",
        V1.\"SameSchTimeFlag\",
        V1.\"SameVisitTimeFlag\",
        V1.\"SchAndVisitTimeSameFlag\",
        V1.\"SchOverAnotherSchTimeFlag\",
        V1.\"VisitTimeOverAnotherVisitTimeFlag\",
        V1.\"SchTimeOverVisitTimeFlag\",
        V1.\"DistanceFlag\",
        V1.\"InServiceFlag\",
        V1.\"PTOFlag\",
        V1.\"PA_PStatus\",
        V1.\"IsMissed\", V1.\"MissedVisitReason\",
        V2.\"NoResponseFlag\",
        V2.\"NoResponseTitle\",
        V2.\"NoResponseNotes\",
        V1.\"EVVType\",
        V1.\"DistanceMilesFromLatLng\",
        V1.\"ServiceCode\",
        V2.\"ResolveDate\",
        V2.\"ResolvedBy\",
        V1.\"ContractType\",
        V1.\"PLongitude\",
        V1.\"PLatitude\",
        V1.\"ShVTSTTime\",
        V1.\"ShVTENTime\",
        TIMESTAMPDIFF(MINUTE, V1.\"ShVTSTTime\", V1.\"ShVTENTime\") AS \"TotalMinutes\",
        V1.\"BilledRateMinute\"
    FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS V1
    INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTS AS V2 
        ON V2.\"CONFLICTID\" = V1.\"CONFLICTID\"
    INNER JOIN (
        SELECT 
            a.\"GroupID\",
            a.\"CONFLICTID\",
            a.\"BilledRateMinute\",
            a.\"BILLABLEMINUTESOVERLAP\",
            grp.\"GroupSize\" AS \"GroupSize\",
            CASE 
                WHEN a.\"BilledRateMinute\" = 0 OR a.\"APID\" <> '".$WhereInQuery."' THEN 0
                ELSE CONFLICTREPORT".$this->dbsuffix.".PUBLIC.GETFULLSHIFTTIME(
                    a.\"BILLABLEMINUTESFULLSHIFT\", 
                    a.\"ShVTSTTime\", 
                    a.\"ShVTENTime\"
                ) * a.\"BilledRateMinute\"
            END AS \"ShiftPrice\",
            SUM(
                COALESCE(
                    CONFLICTREPORT".$this->dbsuffix.".PUBLIC.GETOVERLAPTIME(
                        NULL,
                        a.\"ShVTSTTime\",
                        a.\"ShVTENTime\",
                        b.\"ShVTSTTime\",
                        b.\"ShVTENTime\"
                    ), 0
                )
            ) AS \"OverlapTime\",
            CASE 
                WHEN a.\"BilledRateMinute\" = 0 OR a.\"APID\" <> '".$WhereInQuery."' THEN 0
                WHEN grp.\"GroupSize\" = 2 AND a.\"BILLABLEMINUTESOVERLAP\" IS NOT NULL
                    THEN a.\"BILLABLEMINUTESOVERLAP\" * a.\"BilledRateMinute\"
                ELSE SUM(
                    COALESCE(
                        CONFLICTREPORT".$this->dbsuffix.".PUBLIC.GETOVERLAPTIME(
                            NULL,
                            a.\"ShVTSTTime\",
                            a.\"ShVTENTime\",
                            b.\"ShVTSTTime\",
                            b.\"ShVTENTime\"
                        ), 0
                    ) * a.\"BilledRateMinute\"
                )
            END AS \"OverlapPrice\"
        FROM (
            SELECT DISTINCT 
                \"GroupID\", \"CONFLICTID\", \"ShVTSTTime\", \"ShVTENTime\",
                \"BilledRateMinute\", \"BILLABLEMINUTESOVERLAP\", \"BILLABLEMINUTESFULLSHIFT\",
                \"PayerID\" AS \"APID\"
            FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS 
            WHERE \"GroupID\" IN (".$GroupID.")
        ) a
        LEFT JOIN (
            SELECT DISTINCT 
                \"GroupID\", \"CONFLICTID\", \"ShVTSTTime\", \"ShVTENTime\"
            FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS 
            WHERE \"GroupID\" IN (".$GroupID.")
        ) b
            ON a.\"GroupID\" = b.\"GroupID\" AND a.\"CONFLICTID\" <> b.\"CONFLICTID\"
        INNER JOIN (
            SELECT \"GroupID\", COUNT(DISTINCT \"CONFLICTID\") AS \"GroupSize\"
            FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS
            WHERE \"GroupID\" IN (".$GroupID.")
            GROUP BY \"GroupID\"
        ) grp 
            ON grp.\"GroupID\" = a.\"GroupID\"
        GROUP BY 
            a.\"GroupID\", a.\"CONFLICTID\", a.\"BilledRateMinute\", 
            a.\"BILLABLEMINUTESOVERLAP\", a.\"BILLABLEMINUTESFULLSHIFT\", 
            a.\"ShVTSTTime\", a.\"ShVTENTime\", grp.\"GroupSize\", a.\"APID\"
    ) AS CVMCH 
    ON CVMCH.\"GroupID\" = V1.\"GroupID\" AND CVMCH.\"CONFLICTID\" = V1.\"CONFLICTID\"
    ORDER BY V1.\"CONFLICTID\" ASC";

    $statement_ch = $this->conn->prepare($SQLChild);            
    return $statement_ch->fetchAll(PDO::FETCH_ASSOC);
}


    public function PTOByCIDPayer($CONFLICTID, $PayerID, $AppPayerID, $UserID){
        $query_PTO = "SELECT DISTINCT V1.\"PTOFlag\" FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS V1";
        if(auth()->user()->hasRole('Payer') && $PayerID && $AppPayerID){
            $query_PTO .= " WHERE V1.\"PayerID\" = '".$PayerID."'";
        }else if(auth()->user()->hasRole('Governing Bodies') && $UserID){
            $AllPayerFlag = Auth::user()->AllPayerFlag;
            $payer_state = Auth::user()->payer_state;
            if($AllPayerFlag==1){
                $WhereInQuery = "SELECT DISTINCT \"Payer Id\" FROM ANALYTICS".$this->dbsuffix.".BI.DIMPAYER WHERE \"Is Active\" = TRUE AND \"Is Demo\" = FALSE AND LOWER(\"Payer State\") = LOWER('".$payer_state."')";
            }else{
                $WhereInQuery = "SELECT DISTINCT \"PayerID\" FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.GOVBODIESPAYERS WHERE \"UserID\" = $UserID";
            }
            $query_PTO .= " WHERE V1.\"PayerID\" IN($WhereInQuery)";
        }else{
            $query_PTO .= " WHERE V1.\"PayerID\" = '-9999'";
        }
        $query_PTO .= " AND V1.\"CONFLICTID\" = '$CONFLICTID' AND \"PTOFlag\"='Y'";
        $statement_PTO = $this->conn->prepare($query_PTO);
        return $statement_PTO->fetch(PDO::FETCH_ASSOC);
    }

    public function InServiceByCIDPayer($CONFLICTID, $PayerID, $AppPayerID, $UserID){
        $query_INS = "SELECT DISTINCT V1.\"InServiceFlag\" FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS V1";
        if(auth()->user()->hasRole('Payer') && $PayerID && $AppPayerID){
            $query_INS .= " WHERE V1.\"PayerID\" = '".$PayerID."'";
        }else if(auth()->user()->hasRole('Governing Bodies') && $UserID){
            $AllPayerFlag = Auth::user()->AllPayerFlag;
            $payer_state = Auth::user()->payer_state;
            if($AllPayerFlag==1){
                $WhereInQuery = "SELECT DISTINCT \"Payer Id\" FROM ANALYTICS".$this->dbsuffix.".BI.DIMPAYER WHERE \"Is Active\" = TRUE AND \"Is Demo\" = FALSE AND LOWER(\"Payer State\") = LOWER('".$payer_state."')";
            }else{
                $WhereInQuery = "SELECT DISTINCT \"PayerID\" FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.GOVBODIESPAYERS WHERE \"UserID\" = $UserID";
            }
            $query_INS .= " WHERE V1.\"PayerID\" IN($WhereInQuery)";
        }else{
            $query_INS .= " WHERE V1.\"PayerID\" = '-9999'";
        }
        $query_INS .= " AND V1.\"CONFLICTID\" = '$CONFLICTID' AND \"InServiceFlag\"='Y'";
        $statement_INS = $this->conn->prepare($query_INS);
        return $statement_INS->fetch(PDO::FETCH_ASSOC);
    }

    public function ConflictCommu_Inter($CONFLICTID, $TYPE){
        $sql = "SELECT \"ID\", \"CONFLICTID\", \"DESCRIPTION\", \"CREATEDBY\", \"CommentType\", \"RECORDEDDATETIME\", \"ATTACHMENTURL\"
                FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICT_COMMU_INTER
                WHERE \"CONFLICTID\" = '$CONFLICTID' AND \"CommentType\" = '$TYPE'";
        if($TYPE == 2)
        {
            $uid = Auth::user()->id;
            $sql .= "AND \"CREATEDBY\" = '$uid'";
        }        
        $statement = $this->conn->prepare($sql);
        return $statement->fetchAll();
    }

    public function executeinsertupdate($sql){
        $statement_INS = $this->conn->prepare($sql);
        $statement_INS->fetch();
        return true;
    }   

    public function executeselect($sql){
        $statement = $this->conn->prepare($sql);
        return $statement->fetch(PDO::FETCH_ASSOC);
    }

    public function noresponsereasonbyid($reason){
        $queryR = "SELECT \"Title\" AS \"text\" FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.NORESPONSEREASONS WHERE \"ID\" = '".$reason."'";
        $statementR = $this->conn->prepare($queryR);
        return $statementR->fetch(PDO::FETCH_ASSOC);
    }

    public function getloglist($request, $CONFLICTID, $CONID, $TYPEID, $LoggedInUserID = ''){
        // $perPage = 20;
        // Define allowed values for pagination
        $allowedPerPageOptions = [10, 50, 100, 200, 500];

        // Get the per_page value from the request or session
        $perPage = $request->per_page;

        // Check if per_page is a valid numeric value in the allowed options
        if (in_array($perPage, $allowedPerPageOptions)) {
            // Store the per_page value in the session if it's valid
            session(['per_page' => $perPage]);
        } else {
            // Retrieve the per_page from session or default to 10
            $perPage = session('per_page', 10);
        }
        $currentPage = $request->input('page', 1);
        $offset = ($currentPage - 1) * $perPage;
        // Your query
        $query = "SELECT LHV.*, LH.\"CreatedDateTime\", CV.CONFLICTID, LH.\"LogTypeFlag\" AS \"LogTypeFlag\", LF.\"RestrictedFlag\", CONCAT(CV.\"PayerID\", '~', CV.\"AppPayerID\") AS \"Payer_PAPID\", CONCAT(CV.\"ProviderID\", '~', CV.\"AppProviderID\") AS \"Payer_PRAPID\", CONCAT(CV.\"ProviderID\", '~', CV.\"AppProviderID\") AS \"Provider_PAPID\", CONCAT(CV.\"ConProviderID\", '~', CV.\"ConAppProviderID\") AS \"Provider_PRAPID\"
                  FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.LOG_HISTORY_VALUES AS LHV
                  INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.LOG_HISTORY AS LH ON LH.ID=LHV.LHID
                  INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.LOG_FIELDS AS LF ON LF.ID=LHV.\"LogID\" AND LF.\"FieldFor\" = '".$TYPEID."'
                  INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS CV ON CV.ID=LH.CONID AND CV.CONFLICTID = '".$CONFLICTID."'";
            $query .= " WHERE LF.\"HideColumnFlag\" IS NULL";
        if($CONID){
            $query .= " AND LH.CONID='".$CONID."'";
        }
        if($LoggedInUserID && $LoggedInUserID=='P'){
            $query .= " AND LF.\"HideForProviderFlag\" IS NULL";
        }
        if($LoggedInUserID && $LoggedInUserID=='PA'){
            $query .= " AND LF.\"HideHidePayerFlag\" IS NULL";
        }
        if($FieldName = $request->FieldName){
            $query .= " AND";
            $query .= " LHV.\"LogID\"='".$FieldName."'";
        }
        $query .= " ORDER BY LH.\"CreatedDateTime\" DESC, LHV.ID DESC";
        $query .= " LIMIT $perPage OFFSET $offset";
        $statement = $this->conn->prepare($query);
        
        $results = $statement->fetchAll(PDO::FETCH_ASSOC);

        $countquery = "SELECT COUNT(DISTINCT LHV.\"ID\") AS \"count\" 
                  FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.LOG_HISTORY_VALUES AS LHV 
                  INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.LOG_HISTORY AS LH ON LH.ID=LHV.LHID 
                  INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.LOG_FIELDS AS LF ON LF.ID=LHV.\"LogID\" AND LF.\"FieldFor\" = '".$TYPEID."'
                  INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS CV ON CV.ID=LH.CONID AND CV.CONFLICTID = '".$CONFLICTID."'";
        $countquery .= " WHERE LF.\"HideColumnFlag\" IS NULL";
        if($CONID){
            $countquery .= " AND LH.CONID='".$CONID."'";
        }
        if($FieldName = $request->FieldName){
            $countquery .= " AND";
            $countquery .= " LHV.\"LogID\"='".$FieldName."'";
        }
        $statement_count = $this->conn->prepare($countquery);        
        $total_results = $statement_count->fetch(PDO::FETCH_ASSOC);
        $rowCount = $total_results['count'];        
        $paginatedResults = new \Illuminate\Pagination\LengthAwarePaginator(
            $results,
            $rowCount,
            $perPage,
            $currentPage,
            ['path' => request()->url(), 'query' => request()->query()]
        );
        return $paginatedResults;
    }

    public function getlogVisitlist($request, $CONFLICTID, $PATIENTID, $PTY){
        // $perPage = 10;
        // Define allowed values for pagination
        $allowedPerPageOptions = [10, 50, 100, 200, 500];

        // Get the per_page value from the request or session
        $perPage = $request->per_page;

        // Check if per_page is a valid numeric value in the allowed options
        if (in_array($perPage, $allowedPerPageOptions)) {
            // Store the per_page value in the session if it's valid
            session(['per_page' => $perPage]);
        } else {
            // Retrieve the per_page from session or default to 10
            $perPage = session('per_page', 10);
        }
        $currentPage = $request->input('page', 1);
        $offset = ($currentPage - 1) * $perPage;
        // Your query
        $query = "SELECT VHL.ID, VHL.\"VisitID\", VHL.\"AppVisitID\", VHL.\"ProviderID\", VHL.\"AppProviderID\", VHL.\"ProviderName\", VHL.\"VisitDate\", VHL.\"SchStartTime\", VHL.\"SchEndTime\", VHL.\"VisitStartTime\", VHL.\"VisitEndTime\", VHL.\"EVVStartTime\", VHL.\"EVVEndTime\", VHL.\"CaregiverID\", VHL.\"AppCaregiverID\", VHL.\"AideCode\", VHL.\"AideName\", VHL.\"AideFName\", VHL.\"AideLName\", VHL.\"AideSSN\", VHL.\"AideStatus\", VHL.\"OfficeID\", VHL.\"AppOfficeID\", VHL.\"Office\", VHL.\"PatientID\", VHL.\"AppPatientID\", VHL.\"PAdmissionID\", VHL.\"PName\", VHL.\"PFName\", VHL.\"PLName\", VHL.\"PMedicaidNumber\", VHL.\"PStatus\", VHL.\"PAddressID\", VHL.\"PAppAddressID\", VHL.\"PAddressL1\", VHL.\"PAddressL2\", VHL.\"PCity\", VHL.\"PAddressState\", VHL.\"PZipCode\", VHL.\"PCounty\", VHL.\"P_PatientID\", VHL.\"P_AppPatientID\", VHL.\"P_PAdmissionID\", VHL.\"P_PName\", VHL.\"P_PFName\", VHL.\"P_PLName\", VHL.\"P_PMedicaidNumber\", VHL.\"P_PStatus\", VHL.\"P_PAddressID\", VHL.\"P_PAppAddressID\", VHL.\"P_PAddressL1\", VHL.\"P_PAddressL2\", VHL.\"P_PCity\", VHL.\"P_PAddressState\", VHL.\"P_PZipCode\", VHL.\"P_PCounty\", VHL.\"PA_PatientID\", VHL.\"PA_AppPatientID\", VHL.\"PA_PAdmissionID\", VHL.\"PA_PName\", VHL.\"PA_PFName\", VHL.\"PA_PLName\", VHL.\"PA_PMedicaidNumber\", VHL.\"PA_PStatus\", VHL.\"PA_PAddressID\", VHL.\"PA_PAppAddressID\", VHL.\"PA_PAddressL1\", VHL.\"PA_PAddressL2\", VHL.\"PA_PCity\", VHL.\"PA_PAddressState\", VHL.\"PA_PZipCode\", VHL.\"PA_PCounty\", VHL.\"PLongitude\", VHL.\"PLatitude\", VHL.\"PayerID\", VHL.\"AppPayerID\", VHL.\"Contract\", VHL.\"PayerState\", VHL.\"BilledDate\", VHL.\"BilledHours\", VHL.\"Billed\", VHL.\"BilledRate\", VHL.\"TotalBilledAmount\", VHL.\"ServiceCodeID\", VHL.\"AppServiceCodeID\", VHL.\"RateType\", VHL.\"ServiceCode\", VHL.\"LastUpdatedBy\", VHL.\"LastUpdatedDate\", VHL.\"IsMissed\", VHL.\"MissedVisitReason\", VHL.\"EVVType\", VHL.\"AgencyPhone\", VHL.\"CreatedDate\" FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.VISITHISTORY_LOG AS VHL";
        $PATIENTIDArr = explode('~', $PATIENTID);
        if(!empty($PATIENTIDArr) && sizeof($PATIENTIDArr)==2){
            $query .= " WHERE VHL.\"VisitID\"='".$PATIENTIDArr[0]."'";
        }else if(!empty($PATIENTIDArr) && sizeof($PATIENTIDArr)==1){
            $query .= " WHERE VHL.\"VisitID\"='".$PATIENTIDArr[0]."'";
        }else{
            $query .= " WHERE VHL.\"VisitID\"='-9999'";
        }
        $query .= " ORDER BY VHL.ID DESC";
        $query .= " LIMIT $perPage OFFSET $offset";
        $statement = $this->conn->prepare($query);
        
        $results = $statement->fetchAll(PDO::FETCH_ASSOC);

        $countquery = "SELECT COUNT(DISTINCT VHL.\"ID\") AS \"count\" FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.VISITHISTORY_LOG AS VHL";
        $PATIENTIDArr = explode('~', $PATIENTID);
        if(!empty($PATIENTIDArr) && sizeof($PATIENTIDArr)==2){
            $countquery .= " WHERE VHL.\"VisitID\"='".$PATIENTIDArr[0]."'";
        }else if(!empty($PATIENTIDArr) && sizeof($PATIENTIDArr)==1){
            $countquery .= " WHERE VHL.\"VisitID\"='".$PATIENTIDArr[0]."'";
        }else{
            $countquery .= " WHERE VHL.\"VisitID\"='-9999'";
        }
        $statement_count = $this->conn->prepare($countquery);
        $total_results = $statement_count->fetch(PDO::FETCH_ASSOC);
        $rowCount = $total_results['count'];        
        $paginatedResults = new \Illuminate\Pagination\LengthAwarePaginator(
            $results,
            $rowCount,
            $perPage,
            $currentPage,
            ['path' => request()->url(), 'query' => request()->query()]
        );
        return $paginatedResults;
    }

    public function GetProvidersID(){
        $Provider_Id = Auth::user()->Provider_Id;
        $Application_Provider_Id = Auth::user()->Application_Provider_Id;
        $queryFields = "SELECT CONCAT(PID, '~', APPLICATIONPID) AS PAPPID, CONTACT_NAME, PHONE FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONTACT_MAINTENANCE";
        if(auth()->user()->hasRole('Provider') && $Provider_Id && $Application_Provider_Id){
            $queryFields .= " WHERE \"ProviderID\" = '".$Provider_Id."'";
        }else if(auth()->user()->hasRole('Payer') || auth()->user()->hasRole('Governing Bodies')){
            $queryFields .= " WHERE \"ProviderID\" = PID";
        }else{
            $queryFields .= " WHERE \"ProviderID\" = '-9999'";
        }
        $statementQuery = $this->conn->prepare($queryFields);
        $fetchAll = $statementQuery->fetchAll(PDO::FETCH_ASSOC);
        $fetchAllArray = [];
        if(!empty($fetchAll)){
            foreach($fetchAll as $rowKS){
                $PAPPID = $rowKS['PAPPID'];
                unset($rowKS['PAPPID']);
                $fetchAllArray[$PAPPID] = $rowKS;
            }
        }
        return $fetchAllArray;
    }

    public function getlogfiedls(){
        $queryFields = "SELECT ID, \"FieldDisplayValue\", \"FieldType\" FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.LOG_FIELDS";
        $statementQuery = $this->conn->prepare($queryFields);
        return $statementQuery->fetchAll(PDO::FETCH_ASSOC);
    }

    public function getConflicatCommect($ProviderID, $AppProviderID, $VisitID, $AppVisitID)
    {
        $query = "SELECT DISTINCT CONCAT(\"VisitID\", '~', \"AppVisitID\") AS VAPPID 
                FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS 
                WHERE 
                (
                    (\"ProviderID\" = '{$ProviderID}')
                    OR
                    (\"ConProviderID\" = '{$ProviderID}')
                )
                AND
                (
                    (\"VisitID\" = '{$VisitID}')
                    OR
                    (\"ConVisitID\" = '{$VisitID}')
                )";

        $sqldata = [];
        $statementQuery = $this->conn->prepare($query);
        while ($row = $statementQuery->fetch(PDO::FETCH_ASSOC)) {
            $sqldata[] = $row['VAPPID'];
        }

        return $sqldata;
    } 

    public function getProviders($request, $PayerID, $AppPayerID, $UserID, $all = ''){
        $AllPayerFlagQ = false;
        $WhereInQuery = $PayerID;
        $currentPage = $request->input('page', 1);
        $allowedPerPageOptions = [10, 50, 100, 200, 500];
        $perPage = $request->per_page;
        if (in_array($perPage, $allowedPerPageOptions)) {
            session(['per_page' => $perPage]);
        } else {
            $perPage = session('per_page', 10);
        }
        $offset = ($currentPage - 1) * $perPage;
        $TOPL = '';
        if($all == '-1')
        {
            $TOPL = ' TOP 2000';
        }
        // Build all filters using reusable method
        $whereCondition = $this->buildSummaryTableFilterCondition($request, 'providers');

        // Main query using proper CTE pattern for COUNT and IMPACT table joins
        // Following DatabaseConstants.php pattern with CTE and LEFT JOIN
        $query = "WITH ProviderCounts AS (
            SELECT 
                PROVIDERID,
                PROVIDER_NAME,
                TIN,
                STATUSFLAG,
                COUNT(DISTINCT VISIT_KEY) AS TotalAll
            FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.PAYER_CONFLICT_SUMMARY_COUNT
            WHERE PAYERID = '" . $WhereInQuery . "'" . 
            $whereCondition . "
            GROUP BY PROVIDERID, PROVIDER_NAME, TIN, STATUSFLAG
        ), 
        ProviderImpacts AS (
            SELECT 
                PROVIDERID,
                PROVIDER_NAME,
                TIN,
                STATUSFLAG,
                SUM(CON_SP) AS CON_SP,
                SUM(CON_OP) AS CON_OP,
                SUM(CON_FP) AS CON_FP
            FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.PAYER_CONFLICT_SUMMARY_IMPACT
            WHERE PAYERID = '" . $WhereInQuery . "'" . 
            $whereCondition . "
            GROUP BY PROVIDERID, PROVIDER_NAME, TIN, STATUSFLAG
        )
        SELECT$TOPL
        pc.PROVIDERID AS \"APRID\",
        pc.PROVIDER_NAME AS \"ProviderName\",
        pc.TIN AS \"FederalTaxNumber\",
        pc.STATUSFLAG AS \"StatusFlag\",
        pc.TotalAll AS \"TotalAll\",
        COALESCE(pi.CON_SP, 0) AS \"ShiftPriceAll\",
        COALESCE(pi.CON_OP, 0) AS \"OverlapPriceAll\",
        COALESCE(pi.CON_FP, 0) AS \"FinalPriceAll\"
        FROM ProviderCounts pc
        LEFT JOIN ProviderImpacts pi ON 
            pc.PROVIDERID = pi.PROVIDERID AND 
            pc.PROVIDER_NAME = pi.PROVIDER_NAME AND 
            pc.TIN = pi.TIN AND 
            pc.STATUSFLAG = pi.STATUSFLAG";
        $querycount = $query;

        $sortableLinks = [
            'pr' => 'pc.PROVIDER_NAME',
            'tn' => 'pc.TIN',
            'st' => 'pc.STATUSFLAG',
            'co' => '"TotalAll"',
            'rr' => '"FinalPriceAll"',
            'es' => '"ShiftPriceAll"',
            'ov' => '"OverlapPriceAll"'
        ];
        $sortableLinksAD = [
            'asc' => 'asc',
            'desc' => 'desc'
        ];
        $SortByField = '';
        if($request->sort && isset($sortableLinks[strtolower($request->sort)])){
            $SortByField = $sortableLinks[strtolower($request->sort)];
        }
        $SortByAD = '';
        if($request->direction && isset($sortableLinksAD[strtolower($request->direction)])){
            $SortByAD = strtoupper($sortableLinksAD[strtolower($request->direction)]);
        }

        if($SortByField && $SortByAD){
            $sortby = $SortByAD;
            $query .= " ORDER BY ".$SortByField." ".$sortby."";
        }else{
            $query .= " ORDER BY \"TotalAll\" DESC, pc.PROVIDER_NAME ASC, pc.STATUSFLAG ASC";
        }            
        if($TOPL==''){
            $query .= " LIMIT $perPage OFFSET $offset";
        }
        if($request->debug){
            echo $query;
            die;
        }
        $statement = $this->conn->prepare($query);
        $results = $statement->fetchAll(PDO::FETCH_ASSOC);     
        if($TOPL=='-1'){
            return $results;
        }      
        $countQueryN = "
            SELECT COUNT(*) AS \"count\"
            FROM (
                $querycount
            ) AS subquery";
        $statement_count = $this->conn->prepare($countQueryN);        
        $total_results = $statement_count->fetch(PDO::FETCH_ASSOC);
        $rowCount = $total_results['count'];
        $paginatedResults = new \Illuminate\Pagination\LengthAwarePaginator(
            $results,
            $rowCount,
            $perPage,
            $currentPage,
            ['path' => request()->url(), 'query' => request()->query()]
        );
        return $paginatedResults;
    }

    public function getProvidersAll($request, $PayerID, $AppPayerID, $UserID, $all = ''){
        $WhereInQuery = $PayerID;

        // Build all filters using reusable method
        $whereCondition = $this->buildSummaryTableFilterCondition($request, 'providers');

        // Main query using COUNT table for total count
        $query = "SELECT COUNT(DISTINCT VISIT_KEY) AS \"TotalAll\"
        FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.PAYER_CONFLICT_SUMMARY_COUNT
        WHERE PAYERID = '" . $WhereInQuery . "'" . 
        $whereCondition;
        $statement = $this->conn->prepare($query);
        $results = $statement->fetch(PDO::FETCH_ASSOC);
        return $results;
    }

    public function getCaregivers($request, $PayerID, $AppPayerID, $UserID, $all = ''){
        $AllPayerFlagQ = false;
        $WhereInQuery = $PayerID;
        $currentPage = $request->input('page', 1);
        $allowedPerPageOptions = [10, 50, 100, 200, 500];
        $perPage = $request->per_page;
        if (in_array($perPage, $allowedPerPageOptions)) {
            session(['per_page' => $perPage]);
        } else {
            $perPage = session('per_page', 10);
        }
        $offset = ($currentPage - 1) * $perPage;
        $TOPL = '';
        if($all == '-1')
        {
            $TOPL = ' TOP 5000';
        }
        // Build all filters using reusable method
        $whereCondition = $this->buildSummaryTableFilterCondition($request, 'caregivers');

        // Main query using proper CTE pattern for COUNT and IMPACT table joins
        // Following DatabaseConstants.php pattern with CTE and LEFT JOIN
        $query = "WITH CaregiverCounts AS (
            SELECT 
                PROVIDERID,
                PROVIDER_NAME,
                TIN,
                STATUSFLAG,
                CAREGIVER_NAME,
                COUNT(DISTINCT VISIT_KEY) AS TotalAll
            FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.PAYER_CONFLICT_SUMMARY_COUNT
            WHERE PAYERID = '" . $WhereInQuery . "'" . 
            $whereCondition . "
            GROUP BY PROVIDERID, PROVIDER_NAME, TIN, STATUSFLAG, CAREGIVER_NAME
        ), 
        CaregiverImpacts AS (
            SELECT 
                PROVIDERID,
                PROVIDER_NAME,
                TIN,
                STATUSFLAG,
                CAREGIVER_NAME,
                SUM(CON_SP) AS CON_SP,
                SUM(CON_OP) AS CON_OP,
                SUM(CON_FP) AS CON_FP
            FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.PAYER_CONFLICT_SUMMARY_IMPACT
            WHERE PAYERID = '" . $WhereInQuery . "'" . 
            $whereCondition . "
            GROUP BY PROVIDERID, PROVIDER_NAME, TIN, STATUSFLAG, CAREGIVER_NAME
        )
        SELECT$TOPL
        cc.PROVIDERID AS \"APRID\",
        cc.PROVIDER_NAME AS \"ProviderName\",
        cc.TIN AS \"FederalTaxNumber\",
        cc.STATUSFLAG AS \"StatusFlag\",
        cc.CAREGIVER_NAME AS \"Name\",
        cc.CAREGIVER_NAME AS \"AideName\",
        TRIM(SPLIT_PART(cc.CAREGIVER_NAME, ' ', 1)) AS \"AideFName\",
        CASE 
            WHEN POSITION(' ' IN cc.CAREGIVER_NAME) > 0 
            THEN TRIM(SUBSTRING(cc.CAREGIVER_NAME, POSITION(' ' IN cc.CAREGIVER_NAME) + 1))
            ELSE ''
        END AS \"AideLName\",
        cc.TotalAll AS \"TotalAll\",
        COALESCE(ci.CON_SP, 0) AS \"ShiftPriceAll\",
        COALESCE(ci.CON_OP, 0) AS \"OverlapPriceAll\",
        COALESCE(ci.CON_FP, 0) AS \"FinalPriceAll\"
        FROM CaregiverCounts cc
        LEFT JOIN CaregiverImpacts ci ON 
            cc.PROVIDERID = ci.PROVIDERID AND 
            cc.PROVIDER_NAME = ci.PROVIDER_NAME AND 
            cc.TIN = ci.TIN AND 
            cc.STATUSFLAG = ci.STATUSFLAG AND
            cc.CAREGIVER_NAME = ci.CAREGIVER_NAME";
        $querycount = $query;

        $sortableLinks = [
            'ca' => 'cc.CAREGIVER_NAME',
            'pr' => 'cc.PROVIDER_NAME',
            'tn' => 'cc.TIN',
            'st' => 'cc.STATUSFLAG',
            'co' => '"TotalAll"',
            'rr' => '"FinalPriceAll"',
            'es' => '"ShiftPriceAll"',
            'ov' => '"OverlapPriceAll"'
        ];
        $sortableLinksAD = [
            'asc' => 'asc',
            'desc' => 'desc'
        ];
        $SortByField = '';
        if($request->sort && isset($sortableLinks[strtolower($request->sort)])){
            $SortByField = $sortableLinks[strtolower($request->sort)];
        }
        $SortByAD = '';
        if($request->direction && isset($sortableLinksAD[strtolower($request->direction)])){
            $SortByAD = strtoupper($sortableLinksAD[strtolower($request->direction)]);
        }

        if($SortByField && $SortByAD){
            $sortby = $SortByAD;
            $query .= " ORDER BY ".$SortByField." ".$sortby."";
        }else{
            $query .= " ORDER BY \"TotalAll\" DESC, cc.CAREGIVER_NAME ASC, cc.PROVIDER_NAME ASC, cc.STATUSFLAG ASC";
        }            
        if($TOPL==''){
            $query .= " LIMIT $perPage OFFSET $offset";
        }
        if($request->debug){
            echo $query;
            die;
        }
        $statement = $this->conn->prepare($query);
        $results = $statement->fetchAll(PDO::FETCH_ASSOC);     
        if($TOPL=='-1'){
            return $results;
        }      
        $countQueryN = "
            SELECT COUNT(*) AS \"count\"
            FROM (
                $querycount
            ) AS subquery";
        $statement_count = $this->conn->prepare($countQueryN);        
        $total_results = $statement_count->fetch(PDO::FETCH_ASSOC);
        $rowCount = $total_results['count'];
        $paginatedResults = new \Illuminate\Pagination\LengthAwarePaginator(
            $results,
            $rowCount,
            $perPage,
            $currentPage,
            ['path' => request()->url(), 'query' => request()->query()]
        );
        return $paginatedResults;
    }

    public function getCaregiversAll($request, $PayerID, $AppPayerID, $UserID, $all = ''){
        $WhereInQuery = $PayerID;

        // Build all filters using reusable method
        $whereCondition = $this->buildSummaryTableFilterCondition($request, 'caregivers');

        // Main query using COUNT table for total count
        $query = "SELECT COUNT(DISTINCT VISIT_KEY) AS \"TotalAll\"
        FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.PAYER_CONFLICT_SUMMARY_COUNT
        WHERE PAYERID = '" . $WhereInQuery . "'" . 
        $whereCondition;
        $statement = $this->conn->prepare($query);
        $results = $statement->fetch(PDO::FETCH_ASSOC);
        return $results;
    }

    public function getPatients($request, $PayerID, $AppPayerID, $UserID, $all = ''){
        $AllPayerFlagQ = false;
        $WhereInQuery = $PayerID;
        $currentPage = $request->input('page', 1);
        $allowedPerPageOptions = [10, 50, 100, 200, 500];
        $perPage = $request->per_page;
        if (in_array($perPage, $allowedPerPageOptions)) {
            session(['per_page' => $perPage]);
        } else {
            $perPage = session('per_page', 10);
        }
        $offset = ($currentPage - 1) * $perPage;
        $TOPL = '';
        if($all == '-1')
        {
            $TOPL = ' TOP 2000';
        }
        // Build all filters using reusable method
        $whereCondition = $this->buildSummaryTableFilterCondition($request, 'patients');

        // Main query using proper CTE pattern for COUNT and IMPACT table joins
        // Following DatabaseConstants.php pattern with CTE and LEFT JOIN
        $query = "WITH PatientCounts AS (
            SELECT 
                PATIENT_FNAME,
                PATIENT_LNAME,
                ADMISSIONID,
                STATUSFLAG,
                COUNT(DISTINCT VISIT_KEY) AS TotalAll
            FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.PAYER_CONFLICT_SUMMARY_COUNT
            WHERE PAYERID = '" . $WhereInQuery . "'" . 
            $whereCondition . "
            GROUP BY PATIENT_FNAME, PATIENT_LNAME, ADMISSIONID, STATUSFLAG
        ), 
        PatientImpacts AS (
            SELECT 
                PATIENT_FNAME,
                PATIENT_LNAME,
                ADMISSIONID,
                STATUSFLAG,
                SUM(CON_SP) AS CON_SP,
                SUM(CON_OP) AS CON_OP,
                SUM(CON_FP) AS CON_FP
            FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.PAYER_CONFLICT_SUMMARY_IMPACT
            WHERE PAYERID = '" . $WhereInQuery . "'" . 
            $whereCondition . "
            GROUP BY PATIENT_FNAME, PATIENT_LNAME, ADMISSIONID, STATUSFLAG
        )
        SELECT$TOPL
        pc.PATIENT_FNAME AS \"PA_PFName\",
        pc.PATIENT_LNAME AS \"PA_PLName\",
        CONCAT(pc.PATIENT_FNAME, ' ', pc.PATIENT_LNAME) AS \"Name\",
        pc.ADMISSIONID AS \"AdmissionID\",
        pc.STATUSFLAG AS \"StatusFlag\",
        pc.TotalAll AS \"TotalAll\",
        COALESCE(pi.CON_SP, 0) AS \"ShiftPriceAll\",
        COALESCE(pi.CON_OP, 0) AS \"OverlapPriceAll\",
        COALESCE(pi.CON_FP, 0) AS \"FinalPriceAll\"
        FROM PatientCounts pc
        LEFT JOIN PatientImpacts pi ON 
            pc.PATIENT_FNAME = pi.PATIENT_FNAME AND 
            pc.PATIENT_LNAME = pi.PATIENT_LNAME AND 
            pc.ADMISSIONID = pi.ADMISSIONID AND 
            pc.STATUSFLAG = pi.STATUSFLAG";
        $querycount = $query;

        $sortableLinks = [
            'pa' => 'pc.PATIENT_FNAME',
            'pl' => 'pc.PATIENT_LNAME',
            'ai' => 'pc.ADMISSIONID',
            'st' => 'pc.STATUSFLAG',
            'co' => '"TotalAll"',
            'rr' => '"FinalPriceAll"',
            'es' => '"ShiftPriceAll"',
            'ov' => '"OverlapPriceAll"'
        ];
        $sortableLinksAD = [
            'asc' => 'asc',
            'desc' => 'desc'
        ];
        $SortByField = '';
        if($request->sort && isset($sortableLinks[strtolower($request->sort)])){
            $SortByField = $sortableLinks[strtolower($request->sort)];
        }
        $SortByAD = '';
        if($request->direction && isset($sortableLinksAD[strtolower($request->direction)])){
            $SortByAD = strtoupper($sortableLinksAD[strtolower($request->direction)]);
        }

        if($SortByField && $SortByAD){
            $sortby = $SortByAD;
            $query .= " ORDER BY ".$SortByField." ".$sortby."";
        }else{
            $query .= " ORDER BY \"TotalAll\" DESC, pc.PATIENT_FNAME ASC, pc.STATUSFLAG ASC";
        }            
        if($TOPL==''){
            $query .= " LIMIT $perPage OFFSET $offset";
        }
        if($request->debug){
            echo $query;
            die;
        }
        $statement = $this->conn->prepare($query);
        $results = $statement->fetchAll(PDO::FETCH_ASSOC);     
        if($TOPL=='-1'){
            return $results;
        }      
        $countQueryN = "
            SELECT COUNT(*) AS \"count\"
            FROM (
                $querycount
            ) AS subquery";
        $statement_count = $this->conn->prepare($countQueryN);        
        $total_results = $statement_count->fetch(PDO::FETCH_ASSOC);
        $rowCount = $total_results['count'];
        $paginatedResults = new \Illuminate\Pagination\LengthAwarePaginator(
            $results,
            $rowCount,
            $perPage,
            $currentPage,
            ['path' => request()->url(), 'query' => request()->query()]
        );
        return $paginatedResults;
    }

    public function getPatientsAll($request, $PayerID, $AppPayerID, $UserID, $all = ''){
        $WhereInQuery = $PayerID;

        // Build all filters using reusable method
        $whereCondition = $this->buildSummaryTableFilterCondition($request, 'patients');

        // Main query using COUNT table for total count
        $query = "SELECT COUNT(DISTINCT VISIT_KEY) AS \"TotalAll\"
        FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.PAYER_CONFLICT_SUMMARY_COUNT
        WHERE PAYERID = '" . $WhereInQuery . "'" . 
        $whereCondition;
        $statement = $this->conn->prepare($query);
        $results = $statement->fetch(PDO::FETCH_ASSOC);
        return $results;
    }

    public function getPayers($request, $PayerID, $AppPayerID, $UserID, $all = ''){
        $AllPayerFlagQ = false;
        $WhereInQuery = $PayerID;
        $currentPage = $request->input('page', 1);
        $allowedPerPageOptions = [10, 50, 100, 200, 500];
        $perPage = $request->per_page;
        if (in_array($perPage, $allowedPerPageOptions)) {
            session(['per_page' => $perPage]);
        } else {
            $perPage = session('per_page', 10);
        }
        $offset = ($currentPage - 1) * $perPage;
        $TOPL = '';
        if($all == '-1')
        {
            $TOPL = ' TOP 2000';
        }
        // Build all filters using reusable method
        $whereCondition = $this->buildSummaryTableFilterCondition($request, 'payers');

        // Main query using conflict summary tables with CONPAYERID
        // Following original function logic: GROUP BY StatusFlag, APID (CONPAYERID), Contract
        // Show conflicts between logged-in payer and other payers, maintaining original grouping
        $query = "WITH PayerCounts AS (
            SELECT 
                CONPAYERID,
                CONTRACT,
                STATUSFLAG,
                COUNT(DISTINCT VISIT_KEY) AS TotalAll
            FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.PAYER_CONFLICT_SUMMARY_COUNT
            WHERE PAYERID = '" . $WhereInQuery . "'" . 
            " AND CONPAYERID != '" . $WhereInQuery . "'" .
            " AND CONTRACT != 'Internal Contract'" .
            $whereCondition . "
            GROUP BY CONPAYERID, CONTRACT, STATUSFLAG
        ), 
        PayerImpacts AS (
            SELECT 
                CONPAYERID,
                CONTRACT,
                STATUSFLAG,
                SUM(CON_SP) AS CON_SP,
                SUM(CON_OP) AS CON_OP,
                SUM(CON_FP) AS CON_FP
            FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.PAYER_CONFLICT_SUMMARY_IMPACT
            WHERE PAYERID = '" . $WhereInQuery . "'" . 
            " AND CONPAYERID != '" . $WhereInQuery . "'" .
            " AND CONTRACT != 'Internal Contract'" .
            $whereCondition . "
            GROUP BY CONPAYERID, CONTRACT, STATUSFLAG
        )
        SELECT$TOPL
        pc.CONPAYERID AS \"APID\",
        pc.CONTRACT AS \"Name\",
        pc.STATUSFLAG AS \"StatusFlag\",
        pc.TotalAll AS \"TotalAll\",
        COALESCE(pi.CON_SP, 0) AS \"ShiftPriceAll\",
        COALESCE(pi.CON_OP, 0) AS \"OverlapPriceAll\",
        COALESCE(pi.CON_FP, 0) AS \"FinalPriceAll\"
        FROM PayerCounts pc
        LEFT JOIN PayerImpacts pi ON 
            pc.CONPAYERID = pi.CONPAYERID AND 
            pc.CONTRACT = pi.CONTRACT AND
            pc.STATUSFLAG = pi.STATUSFLAG";
        $querycount = $query;

        $sortableLinks = [
            'ct' => 'pc.CONTRACT',
            'st' => 'pc.STATUSFLAG',
            'co' => '"TotalAll"',
            'rr' => '"FinalPriceAll"',
            'es' => '"ShiftPriceAll"',
            'ov' => '"OverlapPriceAll"'
        ];
        $sortableLinksAD = [
            'asc' => 'asc',
            'desc' => 'desc'
        ];
        $SortByField = '';
        if($request->sort && isset($sortableLinks[strtolower($request->sort)])){
            $SortByField = $sortableLinks[strtolower($request->sort)];
        }
        $SortByAD = '';
        if($request->direction && isset($sortableLinksAD[strtolower($request->direction)])){
            $SortByAD = strtoupper($sortableLinksAD[strtolower($request->direction)]);
        }

        if($SortByField && $SortByAD){
            $sortby = $SortByAD;
            $query .= " ORDER BY ".$SortByField." ".$sortby."";
        }else{
            $query .= " ORDER BY \"TotalAll\" DESC, pc.CONTRACT ASC, pc.STATUSFLAG ASC";
        }            
        if($TOPL==''){
            $query .= " LIMIT $perPage OFFSET $offset";
        }
        if($request->debug){
            echo $query;
            die;
        }
        $statement = $this->conn->prepare($query);
        $results = $statement->fetchAll(PDO::FETCH_ASSOC);     
        if($TOPL=='-1'){
            return $results;
        }      
        $countQueryN = "
            SELECT COUNT(*) AS \"count\"
            FROM (
                $querycount
            ) AS subquery";
        $statement_count = $this->conn->prepare($countQueryN);        
        $total_results = $statement_count->fetch(PDO::FETCH_ASSOC);
        $rowCount = $total_results['count'];
        $paginatedResults = new \Illuminate\Pagination\LengthAwarePaginator(
            $results,
            $rowCount,
            $perPage,
            $currentPage,
            ['path' => request()->url(), 'query' => request()->query()]
        );
        return $paginatedResults;
    }

    public function getPayersAll($request, $PayerID, $AppPayerID, $UserID, $all = '') {
        $WhereInQuery = $PayerID;

        // Build all filters using reusable method
        $whereCondition = $this->buildSummaryTableFilterCondition($request, 'payers');

        // Main query using COUNT table for total count
        // Show conflicts between logged-in payer and other payers (following dashboard logic)
        // Exclude internal contracts
        $query = "SELECT COUNT(DISTINCT VISIT_KEY) AS \"TotalAll\"
        FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.PAYER_CONFLICT_SUMMARY_COUNT
        WHERE PAYERID = '" . $WhereInQuery . "'" . 
        " AND CONPAYERID != '" . $WhereInQuery . "'" .
        " AND CONTRACT != 'Internal Contract'" .
        $whereCondition;
        $statement = $this->conn->prepare($query);
        $results = $statement->fetch(PDO::FETCH_ASSOC);
        return $results;
    }
    
    public function GetConflictIDsBy($result){
        $SelectQuery = "SELECT DISTINCT CONFLICTID FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS WHERE \"VisitID\" = '".$result['VisitID']."' OR \"ConVisitID\" = '".$result['VisitID']."'";
        $statement_ChildRecords = $this->conn->prepare($SelectQuery);
        $ChildRecords = $statement_ChildRecords->fetchAll(PDO::FETCH_ASSOC);
        $GrAgencyArr = '';
        if (!empty($ChildRecords)) {
            foreach ($ChildRecords as $rowDSA) {
                if(!empty($GrAgencyArr)){
                    $GrAgencyArr .= ',';
                }
                $GrAgencyArr .= $rowDSA['CONFLICTID'];
            }
        }
        return $GrAgencyArr;
    }

    public function visitdetailByconflictid($CONFLICTID, $GroupID){
        $CONFLICTID = (int)$CONFLICTID;
        $GroupID = (int)$GroupID;
        $query = "SELECT V1.\"ID\" FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS V1 INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTS AS V2 ON V2.\"CONFLICTID\"=V1.\"CONFLICTID\"";
        $query .= " WHERE V1.\"CONFLICTID\" = '$CONFLICTID' AND V1.\"GroupID\" = '$GroupID'";
        $statement = $this->conn->prepare($query);
        return $statement->fetch(PDO::FETCH_ASSOC);
    }

    public function InNotificationCount($ProviderID, $AppProviderID, $UserID){
        $query_INS = "SELECT COUNT(V1.\"ID\") AS \"TotalCount\" FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.NOTIFICATIONS AS V1";
        $query_INS .= " INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS CVM ON CVM.CONFLICTID=V1.CONFLICTID";
        $query_INS .= " WHERE V1.\"ProviderID\" = '".$ProviderID."' AND V1.\"ReadUnreadFlag\" IS NULL";
        if($ofcquery = ofcquery()){
            $query_INS .= " AND CVM.\"OfficeID\" IN (".$ofcquery.")";
        }
        $query_INS .= " ORDER BY V1.ID DESC";
        $statement_INS = $this->conn->prepare($query_INS);
        $totalco = $statement_INS->fetch(PDO::FETCH_ASSOC);
        return ($totalco) ? $totalco['TotalCount'] : 0;
    }

    public function InNotificationLatest($ProviderID, $AppProviderID, $UserID, $Limit=10){
        $query_INS = "SELECT * FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.NOTIFICATIONS AS V1";
        $query_INS .= " INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS CVM ON CVM.CONFLICTID=V1.CONFLICTID";
        $query_INS .= " WHERE V1.\"ProviderID\" = '".$ProviderID."' AND V1.\"ReadUnreadFlag\" IS NULL";
        if($ofcquery = ofcquery()){
            $query_INS .= " AND CVM.\"OfficeID\" IN (".$ofcquery.")";
        }
        $query_INS .= " ORDER BY V1.ID DESC LIMIT ".$Limit;
        $statement_INS = $this->conn->prepare($query_INS);
        return $statement_INS->fetchAll(PDO::FETCH_ASSOC);
    }

    /*
        CASE 
        WHEN CCI.\"Attachmenturl\" IS NOT NULL AND CCI.\"Attachmenturl\" != '' THEN 
            SPLIT_PART(CCI.\"Attachmenturl\", 'https://conflict-document.s3.amazonaws.com/', -1)
        ELSE 
            NULL 
        END AS \"Attachmenturl\",
    */
    public function GetCommunicationProviderTab1($ReverseUUID, $payer_provider_type){
        $query_INS = "SELECT CCI.\"created_by\" as \"id\", CCI.\"created_by_name\" as \"crname\", CCI.\"Description\", CCI.\"created_at\", CCI.\"CommentType\", CASE WHEN CCI.\"Attachmenturl\" IS NOT NULL AND CCI.\"Attachmenturl\" != '' THEN SPLIT_PART(CCI.\"Attachmenturl\", '.', -1) ELSE NULL END AS \"FileExtension\", CCI.\"id\" AS CCIID, CCI.\"OriginalFileName\", CCI.\"FileSize\"
         FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICT_COMMU_INTERS AS CCI";
        $query_INS .= " WHERE CCI.\"CommentType\" = 1 AND CCI.\"communications_type\" = '".$payer_provider_type."' AND CCI.\"ReverseUUID\" = '".$ReverseUUID."'";
        $query_INS .= " ORDER BY CCI.\"created_at\" ASC";
        $statement_INS = $this->conn->prepare($query_INS);
        return $statement_INS->fetchAll(PDO::FETCH_ASSOC);
    }

    public function GetCommunicationProviderTab2($CONFLICTID){
        $query_INS = "SELECT CCI.\"created_by\" as \"id\", CCI.\"created_by_name\" as \"crname\", CCI.\"Description\", CCI.\"created_at\", CCI.\"CommentType\", CASE WHEN CCI.\"Attachmenturl\" IS NOT NULL AND CCI.\"Attachmenturl\" != '' THEN SPLIT_PART(CCI.\"Attachmenturl\", '.', -1) ELSE NULL END AS \"FileExtension\", CCI.\"id\" AS CCIID, CCI.\"OriginalFileName\", CCI.\"FileSize\"
        FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICT_COMMU_INTERS AS CCI";
        $query_INS .= " WHERE CCI.\"CommentType\" = 2 AND CCI.\"CONFLICTID\" = '".$CONFLICTID."'";
        $query_INS .= " ORDER BY CCI.\"created_at\" ASC";
        $statement_INS = $this->conn->prepare($query_INS);
        return $statement_INS->fetchAll(PDO::FETCH_ASSOC);
    }

    public function GetCommunicationProviderTab3($CONFLICTID){
        $query_INS = "SELECT * FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.NOTIFICATIONS AS CCI";
        $query_INS .= " WHERE CCI.\"CONFLICTID\" = '".$CONFLICTID."'";
        $query_INS .= " ORDER BY CCI.\"ID\" ASC";
        $statement_INS = $this->conn->prepare($query_INS);
        return $statement_INS->fetchAll(PDO::FETCH_ASSOC);
    }

    public function GetCommunicationPayerTab1($ReverseUUID){
        $query_INS = "SELECT CCI.\"created_by\" as \"id\", CCI.\"created_by_name\" as \"crname\", CCI.\"Description\", CCI.\"created_at\", CCI.\"CommentType\", CASE WHEN CCI.\"Attachmenturl\" IS NOT NULL AND CCI.\"Attachmenturl\" != '' THEN SPLIT_PART(CCI.\"Attachmenturl\", '.', -1) ELSE NULL END AS \"FileExtension\", CCI.\"id\" AS CCIID, CCI.\"OriginalFileName\", CCI.\"FileSize\"
        FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICT_COMMU_INTERS AS CCI";
        $query_INS .= " WHERE CCI.\"CommentType\" = 1 AND CCI.\"communications_type\" = 2 AND CCI.\"ReverseUUID\" = '".$ReverseUUID."'";
        $query_INS .= " ORDER BY CCI.\"created_at\" ASC";
        $statement_INS = $this->conn->prepare($query_INS);
        return $statement_INS->fetchAll(PDO::FETCH_ASSOC);
    }

    public function GetCommunicationPayerTab2($GroupID){
        $GroupID = (int)$GroupID;
        $query_INS = "SELECT CCI.\"created_by\" as \"id\", CCI.\"created_by_name\" as \"crname\", CCI.\"Description\", CCI.\"created_at\", CCI.\"CommentType\", CASE WHEN CCI.\"Attachmenturl\" IS NOT NULL AND CCI.\"Attachmenturl\" != '' THEN SPLIT_PART(CCI.\"Attachmenturl\", '.', -1) ELSE NULL END AS \"FileExtension\", CCI.\"id\" AS CCIID, CCI.\"OriginalFileName\", CCI.\"FileSize\"
        FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICT_COMMU_INTERS AS CCI";
        $query_INS .= " WHERE CCI.\"CommentType\" = 2 AND CCI.\"GroupID\" = '".$GroupID."'";
        $query_INS .= " ORDER BY CCI.\"created_at\" ASC";
        $statement_INS = $this->conn->prepare($query_INS);
        return $statement_INS->fetchAll(PDO::FETCH_ASSOC);
    }

    public function GetCommunicationByID($id){
        $query_INS = "SELECT CASE 
        WHEN CCI.\"Attachmenturl\" IS NOT NULL AND CCI.\"Attachmenturl\" != '' THEN 
            SPLIT_PART(CCI.\"Attachmenturl\", 'https://".env('AWS_BUCKET').".s3.amazonaws.com/', -1)
        ELSE 
            NULL 
        END AS \"Attachmenturl\", CCI.\"OriginalFileName\"
        FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICT_COMMU_INTERS AS CCI";
        $query_INS .= " WHERE CCI.\"id\" = '".$id."'";
        $statement_INS = $this->conn->prepare($query_INS);
        return $statement_INS->fetch(PDO::FETCH_ASSOC);
    }

    public function LastUpdateData(){
        $query_INS = "SELECT \"LastLoadDate\" FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.SETTINGS";
        $statement_INS = $this->conn->prepare($query_INS);
        $totalco = $statement_INS->fetch(PDO::FETCH_ASSOC);
        return (isset($totalco['LastLoadDate']) && !empty($totalco['LastLoadDate'])) ? date('m/d/Y h:i A', strtotime($totalco['LastLoadDate'])) : '';
    }

    public function LastUpdateDataArr(){
        $query_INS = "SELECT \"LastLoadDate\", \"InProgressFlag\" FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.SETTINGS";
        $statement_INS = $this->conn->prepare($query_INS);
        $totalco = $statement_INS->fetch(PDO::FETCH_ASSOC);
        if(!empty($totalco)){
            return ['LastLoadDate' => (isset($totalco['LastLoadDate']) && !empty($totalco['LastLoadDate'])) ? date('m/d/Y h:i A', strtotime($totalco['LastLoadDate'])) : '', 'InProgressFlag' => $totalco['InProgressFlag'] ? 'Refreshing conflicts... Please wait.' : ''];
        }else{
            return ['LastLoadDate' => '', 'InProgressFlag' => ''];
        }
        //return (isset($totalco['LastLoadDate']) && !empty($totalco['LastLoadDate'])) ? date('m/d/Y h:i A', strtotime($totalco['LastLoadDate'])) : '';
    }

    public function GetPayersByID($APIDArr, $UserID){
        $WhereInQueryM = "SELECT DISTINCT CONCAT(\"PayerID\", '~', \"AppPayerID\") AS PAID FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.GOVBODIESPAYERS WHERE \"UserID\" = $UserID";
        $statement_p = $this->conn->prepare($WhereInQueryM);
        $PayerIDsQuery = $statement_p->fetchAll(PDO::FETCH_ASSOC);
        if(!empty($PayerIDsQuery)){
            foreach($PayerIDsQuery as $rowPP){
                $APIDArr[] = $rowPP['PAID'];
            }
        }
        return $APIDArr;
    }

    public function gettaskslist(){
        $query = "SELECT * FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.TASKS";
        $statement = $this->conn->prepare($query);
        return $statement->fetchAll(PDO::FETCH_ASSOC);
    }

    public function TodayJobStatus(){
        $jobstatus = "SELECT COUNT(DISTINCT NAME) AS TOTAL FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.TASK_HISTORY()) WHERE DATABASE_NAME = 'CONFLICTREPORT".$this->dbsuffix."' AND SCHEMA_NAME = 'PUBLIC' AND DATE(COMPLETED_TIME) = DATE(CURRENT_TIMESTAMP)-1 AND STATE = 'SUCCEEDED' AND NAME IN ('TASK_9_CREATE_LOG_HISTORY', 'TASK_1_COPY_DATA')";
        $statement_job = $this->conn->prepare($jobstatus);
        $result = $statement_job->fetch(PDO::FETCH_ASSOC);
        if(!empty($result)){
            return (empty($result['TOTAL']) || $result['TOTAL'] == 2) ? FALSE : 'Refreshing conflicts... Please wait.';
        }else{
            return FALSE;
        }
    }

    

    public function getvisitbyconflictid($CONFLICTID){
        $query = "SELECT TOP 1 V1.\"ProviderID\", V1.\"AppProviderID\"
         FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS V1";
        $query .= " WHERE V1.\"CONFLICTID\" = '$CONFLICTID'";
        $statement = $this->conn->prepare($query);
        return $statement->fetch(PDO::FETCH_ASSOC);
    }

    public function getvisitbyconflictidrev($CONFLICTID, $ReverseUUID){
        $query = "SELECT TOP 1 V1.\"ProviderID\", V1.\"AppProviderID\", V1.\"CONFLICTID\"
         FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS V1";
        $query .= " WHERE V1.\"CONFLICTID\" != '$CONFLICTID' AND V1.\"ReverseUUID\" = '$ReverseUUID'";
        $statement = $this->conn->prepare($query);
        return $statement->fetch(PDO::FETCH_ASSOC);
    }

    public function getPayerConflictsCountFromView($queryParams, $PayerID, $AppPayerID, $request = null) {
        $allParams = $queryParams;
        if ($request) {
            $requestParams = $request->all();
            $allParams = array_merge($queryParams, $requestParams);
        }
        
        $whereConditions = ["\"PayerID\" = '".$PayerID."'"];
        if (!empty($allParams['GroupID'])) {
            $GroupID = (int)$allParams['GroupID'];
            $whereConditions[] = "\"GroupID\" = TRY_TO_NUMBER('$GroupID')";
        }
        if (!empty($allParams['PAdmissionID'])) {
            $whereConditions[] = "\"PA_PAdmissionID\" ILIKE '%" . addslashes($allParams['PAdmissionID']) . "%'";
        }      
        if (!empty($allParams['MedicaidID'])) {
            $whereConditions[] = "\"PA_PMedicaidNumber\" ILIKE '%" . addslashes($allParams['MedicaidID']) . "%'";
        } 
        if (!empty($allParams['PLName'])) {
            $whereConditions[] = "\"PA_PLName\" ILIKE '%" . addslashes($allParams['PLName']) . "%'";
        }
        
        if (!empty($allParams['PFName'])) {
            $whereConditions[] = "\"PA_PFName\" ILIKE '%" . addslashes($allParams['PFName']) . "%'";
        }
        if (!empty($allParams['AideLName'])) {
            $whereConditions[] = "\"AideLName\" ILIKE '%" . addslashes($allParams['AideLName']) . "%'";
        }
        
        if (!empty($allParams['AideFName'])) {
            $whereConditions[] = "\"AideFName\" ILIKE '%" . addslashes($allParams['AideFName']) . "%'";
        }
        if (!empty($allParams['status_flags']) && is_array($allParams['status_flags'])) {
            $statusFlags = array_map('addslashes', $allParams['status_flags']);
            if (in_array('R', $statusFlags)) {
                $statusFlags[] = 'D';
                $statusFlags = array_unique($statusFlags);
            }
            $statusFlagsList = "'" . implode("','", $statusFlags) . "'";
            $whereConditions[] = "\"StatusFlag\" IN ({$statusFlagsList})";
        } elseif (!empty($allParams['ConflictStatusFlag'])) {
            $statusFlag = addslashes($allParams['ConflictStatusFlag']);
            if ($statusFlag == 'R') {
                $whereConditions[] = "\"StatusFlag\" IN ('R', 'D')";
            } else {
                $whereConditions[] = "\"StatusFlag\" = '$statusFlag'";
            }
        }
        if (!empty($allParams['NoResponse'])) {
            $noResponse = addslashes($allParams['NoResponse']);
            if ($noResponse == 'Yes') {
                $whereConditions[] = "\"NoResponseFlag\" = '$noResponse'";
            } else {
                $whereConditions[] = "\"NoResponseFlag\" IS NULL";
            }
        }
        if (!empty($allParams['FlagForReview'])) {
            $flagForReview = addslashes($allParams['FlagForReview']);
            if ($flagForReview == 'Yes') {
                $whereConditions[] = "\"FlagForReview\" = '$flagForReview'";
            } else {
                $whereConditions[] = "(\"FlagForReview\" IS NULL OR \"FlagForReview\" = 'No')";
            }
        }
        if (!empty($allParams['OfficeID'])) {
            $officeId = addslashes($allParams['OfficeID']);
            if (strpos($officeId, '~') !== false) {
                $officeIdParts = explode('~', $officeId);
                $officeId = $officeIdParts[0];
            }
            $whereConditions[] = "\"OfficeID\" = '$officeId'";
        }
        if (!empty($allParams['ProviderID'])) {
            $providerId = addslashes($allParams['ProviderID']);
            if (strpos($providerId, '~') !== false) {
                $providerIdParts = explode('~', $providerId);
                $providerId = $providerIdParts[0];
            }
            $whereConditions[] = "\"ProviderID\" = '$providerId'";
        }
        
        if (!empty($allParams['PayerID'])) {
            $payerIdAppId = addslashes($allParams['PayerID']);
            $conPayerId = '-999';
            if (!empty($payerIdAppId)) {
                $payerIdAppIdArr = explode('~', $payerIdAppId);
                if (!empty($payerIdAppIdArr) && sizeof($payerIdAppIdArr) == 2) {
                    $conPayerId = $payerIdAppIdArr[0];
                } else if (!empty($payerIdAppIdArr) && sizeof($payerIdAppIdArr) == 1) {
                    $conPayerId = $payerIdAppIdArr[0];
                }
            }
            $whereConditions[] = "\"ConPayerID\" = '$conPayerId'";
        }
        if (!empty($allParams['VisitStartDate']) && !empty($allParams['VisitEndDate'])) {
            $startDate = addslashes($allParams['VisitStartDate']);
            $endDate = addslashes($allParams['VisitEndDate']);
            $whereConditions[] = "\"VisitDate\" BETWEEN '$startDate' AND '$endDate'";
        } elseif (!empty($allParams['VisitStartDate'])) {
            $startDate = addslashes($allParams['VisitStartDate']);
            $whereConditions[] = "\"VisitDate\" >= '$startDate'";
        } elseif (!empty($allParams['VisitEndDate'])) {
            $endDate = addslashes($allParams['VisitEndDate']);
            $whereConditions[] = "\"VisitDate\" <= '$endDate'";
        }
        if (!empty($allParams['BilledStartDate']) && !empty($allParams['BilledEndDate'])) {
            $startDate = addslashes($allParams['BilledStartDate']);
            $endDate = addslashes($allParams['BilledEndDate']);
            $whereConditions[] = "TO_CHAR(\"BilledDate\", 'YYYY-MM-DD') BETWEEN '$startDate' AND '$endDate'";
        } elseif (!empty($allParams['BilledStartDate'])) {
            $startDate = addslashes($allParams['BilledStartDate']);
            $whereConditions[] = "TO_CHAR(\"BilledDate\", 'YYYY-MM-DD') >= '$startDate'";
        } elseif (!empty($allParams['BilledEndDate'])) {
            $endDate = addslashes($allParams['BilledEndDate']);
            $whereConditions[] = "TO_CHAR(\"BilledDate\", 'YYYY-MM-DD') <= '$endDate'";
        }
        if (!empty($allParams['CReportedStartDate']) && !empty($allParams['CReportedEndDate'])) {
            $startDate = addslashes($allParams['CReportedStartDate']);
            $endDate = addslashes($allParams['CReportedEndDate']);
            $whereConditions[] = "TO_CHAR(\"CRDATEUNIQUE\", 'YYYY-MM-DD') BETWEEN '$startDate' AND '$endDate'";
        } elseif (!empty($allParams['CReportedStartDate'])) {
            $startDate = addslashes($allParams['CReportedStartDate']);
            $whereConditions[] = "TO_CHAR(\"CRDATEUNIQUE\", 'YYYY-MM-DD') >= '$startDate'";
        } elseif (!empty($allParams['CReportedEndDate'])) {
            $endDate = addslashes($allParams['CReportedEndDate']);
            $whereConditions[] = "TO_CHAR(\"CRDATEUNIQUE\", 'YYYY-MM-DD') <= '$endDate'";
        }
        if (!empty($allParams['AgingDays'])) {
            $AgingDays = is_numeric($allParams['AgingDays']) ? $allParams['AgingDays'] : -99;
            $whereConditions[] = "DATEDIFF(DAY, \"CRDATEUNIQUE\", GETDATE()) = '$AgingDays'";
        }
        if (!empty($allParams['ConflictType'])) {
            $conflictCondition = ConflictTypeHelper::buildConflictCondition($allParams['ConflictType'], '');
            if ($conflictCondition) {
                $whereConditions[] = $conflictCondition;
            }
        }
        $whereClause = implode(' AND ', $whereConditions);
        $countQuery = "SELECT COUNT(DISTINCT(VISIT_KEY)) AS \"count\" FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.V_PAYER_CONFLICTS_LIST WHERE {$whereClause}";    
        if($request->debug){
            echo $countQuery;
            die;
        }
        // dd($countQuery);
        $statement_count = $this->conn->prepare($countQuery);
        $total_results = $statement_count->fetch(PDO::FETCH_ASSOC);
        return (int)($total_results['count'] ?? 0);
    }

    /**
     * Build conflict type filter for summary tables
     * 
     * @param object $request
     * @return string
     */
    private function buildConflictTypeFilter($request)
    {
        $conflictTypeFilter = '';
        if ($ConflictType = $request->ConflictType) {
            if($ConflictType==1){
                $conflictTypeFilter = " AND CONTYPE = 'only_to'";
            }else if($ConflictType==2){
                $conflictTypeFilter = " AND CONTYPE = 'only_td'";
            }else if($ConflictType==3){
                $conflictTypeFilter = " AND CONTYPE = 'only_is'";
            }else if($ConflictType==4){
                $conflictTypeFilter = " AND CONTYPE = 'both_to_td'";
            }else if($ConflictType==5){
                $conflictTypeFilter = " AND CONTYPE = 'both_to_is'";
            }else if($ConflictType==6){
                $conflictTypeFilter = " AND CONTYPE = 'both_td_is'";
            }else if($ConflictType==7){
                $conflictTypeFilter = " AND CONTYPE = 'all_to_td_is'";
            }
        }
        return $conflictTypeFilter;
    }

    /**
     * Build visit date filter
     * 
     * @param object $request
     * @return string
     */
    private function buildVisitDateFilter($request)
    {
        $visitDateFilter = '';
        $VisitStartDate = $request->VisitStartDate ?? '';
        $VisitEndDate = $request->VisitEndDate ?? '';
        
        if ($VisitStartDate && $VisitEndDate) {
            $visitDateFilter = " AND VISITDATE BETWEEN '$VisitStartDate' AND '$VisitEndDate'";
        }else if ($VisitStartDate && !$VisitEndDate) {
            $visitDateFilter = " AND VISITDATE >= '$VisitStartDate'";
        }else if (!$VisitStartDate && $VisitEndDate) {
            $visitDateFilter = " AND VISITDATE <= '$VisitEndDate'";
        }
        return $visitDateFilter;
    }

    /**
     * Build conflict reported date filter
     * 
     * @param object $request
     * @return string
     */
    private function buildCrDateFilter($request)
    {
        $crDateFilter = '';
        $CReportedStartDate = $request->CReportedStartDate ?? '';
        $CReportedEndDate = $request->CReportedEndDate ?? '';
        
        if ($CReportedStartDate && $CReportedEndDate) {
            $crDateFilter = " AND CRDATEUNIQUE BETWEEN '$CReportedStartDate' AND '$CReportedEndDate'";
        }else if ($CReportedStartDate && !$CReportedEndDate) {
            $crDateFilter = " AND CRDATEUNIQUE >= '$CReportedStartDate'";
        }else if (!$CReportedStartDate && $CReportedEndDate) {
            $crDateFilter = " AND CRDATEUNIQUE <= '$CReportedEndDate'";
        }
        return $crDateFilter;
    }

    /**
     * Build provider ID filter
     * 
     * @param object $request
     * @return string
     */
    private function buildProviderIdFilter($request)
    {
        $filter = '';
        if ($ProviderID = $request->ProviderID) {
            $PProviderID = '-999';
            if(!empty($ProviderID)){
                $ProviderIDArr = explode('~', $ProviderID);
                if(!empty($ProviderIDArr) && sizeof($ProviderIDArr)==2){
                    $PProviderID = $ProviderIDArr[0];
                }else if(!empty($ProviderIDArr) && sizeof($ProviderIDArr)==1){
                    $PProviderID = $ProviderIDArr[0];
                }
            }
            $filter .= " AND PROVIDERID = '$PProviderID'";
        }
        return $filter;
    }

    /**
     * Build provider TIN filter
     * 
     * @param object $request
     * @return string
     */
    private function buildProviderTinFilter($request)
    {
        $filter = '';
        if ($ProviderTIN = $request->ProviderTIN) {
            $filter .= " AND TIN = '$ProviderTIN'";
        }
        return $filter;
    }

    /**
     * Build caregiver name filter
     * 
     * @param object $request
     * @return string
     */
    private function buildCaregiverNameFilter($request)
    {
        $filter = '';
        if ($AideName = $request->AideName) {
            $filter .= " AND CAREGIVER_NAME ILIKE '%$AideName%'";
        }
        return $filter;
    }

    /**
     * Build conflict status flag filter
     * 
     * @param object $request
     * @return string
     */
    private function buildConflictStatusFilter($request)
    {
        $filter = '';
        if ($ConflictStatusFlag = $request->ConflictStatusFlag) {
            if($ConflictStatusFlag!='All'){
                $filter .= " AND CASE
                    WHEN STATUSFLAG IN ('D', 'R') THEN 'R'
                    WHEN STATUSFLAG IN ('N') THEN 'N'
                    ELSE STATUSFLAG
                END = '$ConflictStatusFlag'";
            }
        }else{
            $filter .= " AND CASE
                WHEN STATUSFLAG IN ('D', 'R') THEN 'R'
                WHEN STATUSFLAG IN ('N') THEN 'N'
                ELSE STATUSFLAG
            END = 'U'";
        }
        return $filter;
    }

    /**
     * Build patient name filter
     * 
     * @param object $request
     * @return string
     */
    private function buildPatientNameFilter($request)
    {
        $filter = '';
        if ($PFName = $request->PFName) {
            $filter .= " AND PATIENT_FNAME ILIKE '%$PFName%'";
        }
        if ($PLName = $request->PLName) {
            $filter .= " AND PATIENT_LNAME ILIKE '%$PLName%'";
        }
        return $filter;
    }

    /**
     * Build admission ID filter
     * 
     * @param object $request
     * @return string
     */
    private function buildAdmissionIdFilter($request)
    {
        $filter = '';
        if ($AdmissionID = $request->AdmissionID) {
            $filter .= " AND ADMISSIONID = '$AdmissionID'";
        }
        return $filter;
    }

    /**
     * Build payer contract filter
     * 
     * @param object $request
     * @return string
     */
    private function buildPayerContractFilter($request)
    {
        $filter = '';
        if ($Contract = $request->Contract) {
            $filter .= " AND CONTRACT ILIKE '%$Contract%'";
        }
        return $filter;
    }

    /**
     * Build all common filters for summary tables
     * 
     * @param object $request
     * @param string $filterType - Type of filters to include (providers, caregivers, patients, payers)
     * @return string - Complete WHERE condition
     */
    private function buildSummaryTableFilterCondition($request, $filterType = 'all')
    {
        $whereCondition = '';
        
        // Always include these filters
        $whereCondition .= $this->buildConflictTypeFilter($request);
        $whereCondition .= $this->buildVisitDateFilter($request);
        $whereCondition .= $this->buildCrDateFilter($request);
        $whereCondition .= $this->buildConflictStatusFilter($request);
        
        // Include specific filters based on filter type
        switch ($filterType) {
            case 'providers':
                $whereCondition .= $this->buildProviderIdFilter($request);
                $whereCondition .= $this->buildProviderTinFilter($request);
                break;
            case 'caregivers':
                $whereCondition .= $this->buildCaregiverNameFilter($request);
                $whereCondition .= $this->buildProviderIdFilter($request);
                $whereCondition .= $this->buildProviderTinFilter($request);
                break;
            case 'patients':
                $whereCondition .= $this->buildPatientNameFilter($request);
                $whereCondition .= $this->buildAdmissionIdFilter($request);
                break;
            case 'payers':
                $whereCondition .= $this->buildPayerContractFilter($request);
                break;
            case 'all':
            default:
                $whereCondition .= $this->buildProviderIdFilter($request);
                $whereCondition .= $this->buildProviderTinFilter($request);
                $whereCondition .= $this->buildCaregiverNameFilter($request);
                $whereCondition .= $this->buildPatientNameFilter($request);
                $whereCondition .= $this->buildAdmissionIdFilter($request);
                $whereCondition .= $this->buildPayerContractFilter($request);
                break;
        }
        
        return $whereCondition;
    }

}

