<?php

namespace App\Http\Controllers;

use Illuminate\Http\Request;
use Rdr\SnowflakeJodo\SnowflakeJodo;
use PDO;
use Illuminate\Support\Facades\Auth;
use App\Exports\DashboardConflictManagementExport;
use App\Exports\DashboardPayerConflictManagementExport;
use Maatwebsite\Excel\Facades\Excel;
use App\Models\ConflictManagementModel; // Import your model
use App\Constants\DashboardConstants;
use App\Constants\DatabaseConstants;
use App\Constants\ExportConstants;
use App\Constants\MessageConstants;

class DashboardController extends Controller
{
    protected $conn;
    protected $dbsuffix; // Define a protected property for table prefix
    public function __construct()
    {
        if (!Auth::user()->hasRole(DashboardConstants::ROLE_PAYER) && !Auth::user()->hasRole(DashboardConstants::ROLE_PROVIDER)) {
            abort(403, MessageConstants::UNAUTHORIZED_MESSAGE);
        }
        $this->dbsuffix = env('DB_SUFFIX', '');
        $this->conn = SnowflakeJodo::connect();
    }

    private function getEnvironmentIds()
    {
        return [
            'Payer_Id' => env('PAYERIDS') ?: Auth::user()->Payer_Id,
            'Application_Payer_Id' => env('APAYERIDS') ?: Auth::user()->Application_Payer_Id,
            'Provider_Id' => env('PROVIDERIDS') ?: Auth::user()->Provider_Id,
            'Application_Provider_Id' => env('APROVIDERIDS') ?: Auth::user()->Application_Provider_Id,
        ];
    }

    private function assignIds($envIds)
    {
        return [
            DatabaseConstants::PAYER_ID => $envIds['Payer_Id'] ?: '-999',
            DatabaseConstants::APP_PAYER_ID => $envIds['Application_Payer_Id'] ?: '-999',
            DatabaseConstants::PROVIDER_ID => $envIds['Provider_Id'] ?: '-999',
            DatabaseConstants::APP_PROVIDER_ID => $envIds['Application_Provider_Id'] ?: '-999',
        ];
    }

    private function generateExportLinks($ctype, $DRS, $DRE)
    {
        // Determine date parameters based on user role
        $dateParams = [];
        if (Auth::user()->hasRole(DashboardConstants::ROLE_PROVIDER)) {
            $dateParams = [
                DashboardConstants::C_REPORTED_START_DATE => $DRS,
                DashboardConstants::C_REPORTED_END_DATE => $DRE
            ];
        } else {
            $dateParams = [
                'VisitStartDate' => $DRS,
                'VisitEndDate' => $DRE
            ];
        }

        return [
            DashboardConstants::LINK_MORE => convertToSslUrl(route(DashboardConstants::ROUTE_CONFLICT_MANAGEMENT, array_merge([
                DashboardConstants::FILTER => DashboardConstants::FILTER_VALUE_YES
            ], $dateParams))),
            DashboardConstants::LINK_CSV => convertToSslUrl(route('fetch.html', [
                DashboardConstants::CTYPE => $ctype, 
                'DRS' => $DRS, 
                'DRE' => $DRE, 
                DashboardConstants::EXPORT_TYPE => DashboardConstants::EXPORT_TYPE_CSV
            ])),
            DashboardConstants::LINK_XLSX => convertToSslUrl(route('fetch.html', [
                DashboardConstants::CTYPE => $ctype, 
                'DRS' => $DRS, 
                'DRE' => $DRE, 
                DashboardConstants::EXPORT_TYPE => DashboardConstants::EXPORT_TYPE_XLSX
            ]))
        ];
    }

    /**
     * Calculate total records from results array
     * 
     * @param array $results Array of results to calculate totals from
     * @param string $field Field name to sum (default: DatabaseConstants::CON_TO)
     * @return int Total count
     */
    private function calculateTotalRecords($results, $field = DatabaseConstants::CON_TO)
    {
        $total = 0;
        if (!empty($results)) {
            foreach ($results as $row) {
                $total += $row[$field] ?? 0;
            }
        }
        return $total;
    }

    public function index(Request $request)
    {
        $envIds = $this->getEnvironmentIds();
        $ids = $this->assignIds($envIds);
        $PayerID = $ids[DatabaseConstants::PAYER_ID];
        $AppPayerID = $ids[DatabaseConstants::APP_PAYER_ID];
        $ProviderID = $ids[DatabaseConstants::PROVIDER_ID];
        $AppProviderID = $ids[DatabaseConstants::APP_PROVIDER_ID];
        if (app()->environment('local')) {
            $UserID = env('DEBUG_USER_ID');
            $guid = env('DEBUG_GUID');
        } else {
            $UserID = Auth::user()->id;
            $guid = Auth::user()->guid;
        }
        $AllPayerFlag = Auth::user()->AllPayerFlag;
        $payer_state = Auth::user()->payer_state;
        $getminusdays = getminusdays();
        $DRS = date('Y-m-d', strtotime('-'.$getminusdays.' days'));
        $DRE = date('Y-m-d');
        if (auth()->user()->hasRole(DashboardConstants::ROLE_PROVIDER)) {
            /***********************Provider Dashboard Top*********************/
            $TodayTop = "
            SELECT
                SUM(TODAYTOTAL) AS TODAYTOTAL,
                SUM(TODAYSHIFTPRICE) AS TODAYSHIFTPRICE,
                SUM(TODAYOVERLAPPRICE) AS TODAYOVERLAPPRICE,
                SUM(SEVENTOTAL) AS SEVENTOTAL,
                SUM(SEVENFINALPRICE) AS SEVENFINALPRICE,
                SUM(THIRTYTOTAL) AS THIRTYTOTAL,
                SUM(THIRTYFINALPRICE) AS THIRTYFINALPRICE
            FROM
                CONFLICTREPORT".$this->dbsuffix.".PUBLIC.PROVIDER_DASHBOARD_TOP AS PDT
            INNER JOIN ANALYTICS".$this->dbsuffix.".BI.DIMUSEROFFICES AS DUO ON
                DUO.\"Office Id\" = PDT.OFFICEID
                AND DUO.\"Vendor Id\" = PDT.PROVIDERID
                AND DUO.\"User Id\" = '".$guid."'
                AND DUO.\"Vendor Type\" = 'Vendor'
            WHERE
                PDT.PROVIDERID = '".$ProviderID."'";
            $statement_TodayTop = $this->conn->prepare($TodayTop);
            $TodayTop = $statement_TodayTop->fetch(PDO::FETCH_ASSOC);
            $data['TodayTop'] = $TodayTop;
            /***********************End Provider Dashboard Top******************** */            
            $QueryFlags = [];
            $data[DatabaseConstants::RESULTS] = $QueryFlags;
            /******************************************************************* */
            $results_agency = [];
            $data['results_agency'] = $results_agency;

            /********************************************************************/
            $results_p = [];
            $data['results_caregiver'] = $results_p;

            /******************************************************************* */
            $results_p = [];
            $data['results_patient'] = $results_p;

            /******************************************************************* */
            $results_p = [];
            $data['results_payer'] = $results_p;

            return view('dashboard', $data);//Provider
        } else if (auth()->user()->hasRole(DashboardConstants::ROLE_PAYER)) {
            $results_conflict = [];
            $TotalRecords = $this->calculateTotalRecords($results_conflict, DatabaseConstants::CO_TO);
            $data['results_conflict_total'] = $TotalRecords;
            $data['results_conflict'] = $results_conflict;

            /******************************************************************* */
            $results_agency = [];
            $TotalRecords = $this->calculateTotalRecords($results_agency, DatabaseConstants::CON_TO);
            $data['results_agency_total'] = $TotalRecords;
            $data['results_agency'] = $results_agency;

            /******************************************************************* */
            $results_caregiver = [];
            $TotalRecords = $this->calculateTotalRecords($results_caregiver, DatabaseConstants::CON_TO);
            $data['results_caregiver_total'] = $TotalRecords;
            $data['results_caregiver'] = $results_caregiver;

            /******************************************************************* */
            $results_patient = [];
            $TotalRecords = $this->calculateTotalRecords($results_patient, DatabaseConstants::CON_TO);
            $data['results_patient_total'] = $TotalRecords;
            $data['results_patient'] = $results_patient;

            /******************************************************************* */
            $results_payer = [];
            $TotalRecords = $this->calculateTotalRecords($results_payer, DatabaseConstants::CON_TO);
            $data['results_payer_total'] = $TotalRecords;
            $data['results_payer'] = $results_payer;
            return view('dashboard-payer', $data);//Payer
        }
    }

    public function index_fetch_html(Request $request)
    {
        if (!Auth::user()->hasRole(DashboardConstants::ROLE_PAYER) && !Auth::user()->hasRole(DashboardConstants::ROLE_PROVIDER)) {
            abort(403, MessageConstants::UNAUTHORIZED_MESSAGE);
        }
        $envIds = $this->getEnvironmentIds();
        $ids = $this->assignIds($envIds);
        $PayerID = $ids[DatabaseConstants::PAYER_ID];
        $AppPayerID = $ids[DatabaseConstants::APP_PAYER_ID];
        $ProviderID = $ids[DatabaseConstants::PROVIDER_ID];
        $AppProviderID = $ids[DatabaseConstants::APP_PROVIDER_ID];
        if (app()->environment('local')) {
            $UserID = env('DEBUG_USER_ID');
            $guid = env('DEBUG_GUID');
        } else {
            $UserID = Auth::user()->id;
            $guid = Auth::user()->guid;
        }
        // $UserID = Auth::user()->id;
        // $guid = Auth::user()->guid;
        $AllPayerFlag = Auth::user()->AllPayerFlag;
        $payer_state = Auth::user()->payer_state;
        if (auth()->user()->hasRole(DashboardConstants::ROLE_PROVIDER)) {
            if ($request->DRS && $request->DRE && $request->ctype == 'ConflictType') {
                $QueryFlags = str_replace(
                    ['{DB_SUFFIX}', '{USER_ID}', '{PROVIDER_ID}', '{START_DATE}', '{END_DATE}'],
                    [$this->dbsuffix, $guid, $ProviderID, $request->DRS, $request->DRE],
                    DatabaseConstants::QUERY_PROVIDER_CONFLICT_TYPE
                );
                $statement_QueryFlags = $this->conn->prepare($QueryFlags);
                $QueryFlags = $statement_QueryFlags->fetch(PDO::FETCH_ASSOC);
                $results = $QueryFlags;
                $ConTypes = [
                    "EX_ST_MATCH" => ['Exact Schedule Time Match', 1],
                    "EX_VT_MATCH" => ['Exact Visit Time Match', 2],
                    "EX_ST_VT_MATCH" => ['Exact Schedule and Visit Time Match', 3],
                    "ST_OVR" => ['Schedule time overlap', 4],
                    "VT_OVR" => ['Visit Time Overlap', 5],
                    "ST_VT_OVR" => ['Schedule and Visit time overlap', 6],
                    "TD" => ['Time- Distance', 7],
                    "IN" => ['In-Service', 8],
                ];
                if($request->{DashboardConstants::EXPORT_TYPE})
                {
                    if(!in_array($request->{DashboardConstants::EXPORT_TYPE}, [DashboardConstants::EXPORT_TYPE_CSV, DashboardConstants::EXPORT_TYPE_XLSX])){
                        abort(403, MessageConstants::UNAUTHORIZED_MESSAGE);
                    }
                    return Excel::download(new DashboardConflictManagementExport(['results' => $results, 'ConTypes' => $ConTypes, 'ctype' => $request->{DashboardConstants::CTYPE}, 'ExportType' => $request->{DashboardConstants::EXPORT_TYPE}]), $request->{DashboardConstants::CTYPE}.DashboardConstants::PROVIDER_DASHBOARD_SUFFIX.date(DashboardConstants::DATE_FORMAT).'.'.strtolower($request->{DashboardConstants::EXPORT_TYPE}));
                }
                $countdat = [];
                $countdatKeyValue = [];
                $htmlDataTop = '';
                if(!empty($ConTypes)){
                    foreach ($ConTypes as $K => $V) {
                        if(empty($results[$K . '_TO'])){
                            continue;
                        }
                        $countdat[] = $results[$K . '_TO'];
                        $countdatKeyValue[$K] = $results[$K . '_TO'];
                        $htmlDataTop .= '<tr>
                            <td>' . $V[0] . '</td>
                            <td class="text-right">
                                <a class="font-bold" href="' . convertToSslUrl(route(DashboardConstants::ROUTE_CONFLICT_MANAGEMENT, [DashboardConstants::FILTER => DashboardConstants::FILTER_VALUE_YES, DashboardConstants::C_REPORTED_START_DATE => $request->DRS, DashboardConstants::C_REPORTED_END_DATE => $request->DRE, 'ConflictType' => $V[1]])) . '">' . number_format($results[$K . '_TO']) . '</a>
                            </td>
                            <td class="text-right">
                                ' . DollarF($results[$K . DatabaseConstants::_SP]) . '
                            </td>
                            <td class="text-right">
                                ' . DollarF($results[$K . DatabaseConstants::_OP]) . '
                            </td>
                            <td class="text-right">
                                ' . DollarF($results[$K . DatabaseConstants::_FP]) . '
                            </td>
                        </tr>';
                    }
                }
                $ticks = getYAxisTicks($countdat, 7);
                $minValue = ($ticks) ? min($ticks) : 0;
                $maxValue = ($ticks) ? max($ticks) : 0;
                $htmlDataBottom = '';
                $exportLinks = $this->generateExportLinks($request->{DashboardConstants::CTYPE}, $request->DRS, $request->DRE);
                return response()->json([DashboardConstants::HTML_DATA_TOP => $htmlDataTop, DashboardConstants::HTML_DATA_BOTTOM => $htmlDataBottom, 'LinkMore' => $exportLinks[DashboardConstants::LINK_MORE], 'LinkCSV' => $exportLinks[DashboardConstants::LINK_CSV], 'LinkXLSX' => $exportLinks[DashboardConstants::LINK_XLSX]]);
            } else if ($request->DRS && $request->DRE && $request->{DashboardConstants::CTYPE} == 'Agency') {
                $query_p = str_replace(
                    ['{DB_SUFFIX}', '{USER_ID}', '{PROVIDER_ID}', '{START_DATE}', '{END_DATE}', '{CON_SP}', '{CON_OP}', '{CON_FP}'],
                    [$this->dbsuffix, $guid, $ProviderID, $request->DRS, $request->DRE, DatabaseConstants::CON_SP, DatabaseConstants::CON_OP, DatabaseConstants::CON_FP],
                    DatabaseConstants::QUERY_PROVIDER_AGENCY
                );
                $statement = $this->conn->prepare($query_p);
                $results_agency = $statement->fetchAll(PDO::FETCH_ASSOC);
                if($request->{DashboardConstants::EXPORT_TYPE})
                {
                    if(!in_array($request->{DashboardConstants::EXPORT_TYPE}, [DashboardConstants::EXPORT_TYPE_CSV, DashboardConstants::EXPORT_TYPE_XLSX])){
                        abort(403, MessageConstants::UNAUTHORIZED_MESSAGE);
                    }
                    return Excel::download(new DashboardConflictManagementExport(['results' => $results_agency, 'ctype' => $request->{DashboardConstants::CTYPE}, 'ExportType' => $request->{DashboardConstants::EXPORT_TYPE}]), $request->{DashboardConstants::CTYPE}.DashboardConstants::PROVIDER_DASHBOARD_SUFFIX.date(DashboardConstants::DATE_FORMAT).'.'.strtolower($request->{DashboardConstants::EXPORT_TYPE}));
                }
                $htmlDataTop = '';
                $countdat = [];
                $countdatKeyValue = [];
                if (!empty($results_agency)) {
                    foreach ($results_agency as $rowds) {
                        if(empty($rowds['CON_TO'])){
                            continue;
                        }
                        $countdat[] = $rowds['CON_TO'];
                        $countdatKeyValue[$rowds['CON_P_NAME']] = $rowds['CON_TO'];
                        $htmlDataTop .= '<tr>
                        <td>' . $rowds['CON_P_NAME'] . '</td>
                        <td>' . $rowds['CON_TIN'] . '</td>
                        <td class="text-right">
                            <a class="font-bold" href="' . convertToSslUrl(route(DashboardConstants::ROUTE_CONFLICT_MANAGEMENT, [DashboardConstants::FILTER => DashboardConstants::FILTER_VALUE_YES, DashboardConstants::C_REPORTED_START_DATE => $request->DRS, DashboardConstants::C_REPORTED_END_DATE => $request->DRE, 'ConProviderID' => $rowds['CONPROVIDERID']])) . '">' . number_format($rowds['CON_TO']) . '</a>
                        </td>
                        <td class="text-right">
                            ' . DollarF($rowds[DatabaseConstants::CON_SP]) . '
                        </td>
                        <td class="text-right">
                            ' . DollarF($rowds[DatabaseConstants::CON_OP]) . '
                        </td>
                        <td class="text-right">
                            ' . DollarF($rowds[DatabaseConstants::CON_FP]) . '
                        </td>
                        </tr>';
                    }
                }
                $htmlDataBottom = '';
                $exportLinks = $this->generateExportLinks($request->{DashboardConstants::CTYPE}, $request->DRS, $request->DRE);
                return response()->json([DashboardConstants::HTML_DATA_TOP => $htmlDataTop, DashboardConstants::HTML_DATA_BOTTOM => $htmlDataBottom, 'LinkMore' => $exportLinks[DashboardConstants::LINK_MORE], 'LinkCSV' => $exportLinks[DashboardConstants::LINK_CSV], 'LinkXLSX' => $exportLinks[DashboardConstants::LINK_XLSX]]);

            } else if ($request->DRS && $request->DRE && $request->{DashboardConstants::CTYPE} == 'Caregiver') {
                $query_p = str_replace(
                    ['{DB_SUFFIX}', '{USER_ID}', '{PROVIDER_ID}', '{START_DATE}', '{END_DATE}', '{CON_SP}', '{CON_OP}', '{CON_FP}'],
                    [$this->dbsuffix, $guid, $ProviderID, $request->DRS, $request->DRE, DatabaseConstants::CON_SP, DatabaseConstants::CON_OP, DatabaseConstants::CON_FP],
                    DatabaseConstants::QUERY_PROVIDER_CAREGIVER
                );
                $statement = $this->conn->prepare($query_p);
                $results_p = $statement->fetchAll(PDO::FETCH_ASSOC);
                if($request->ExportType)
                {
                    if(!in_array($request->ExportType, ['CSV', 'XLSX'])){
                        abort(403, 'Sorry !! You are Unauthorized to access this page');
                    }
                    return Excel::download(new DashboardConflictManagementExport(['results' => $results_p, 'ctype' => $request->{DashboardConstants::CTYPE}, 'ExportType' => $request->{DashboardConstants::EXPORT_TYPE}]), $request->{DashboardConstants::CTYPE}.DashboardConstants::PROVIDER_DASHBOARD_SUFFIX.date(DashboardConstants::DATE_FORMAT).'.'.strtolower($request->{DashboardConstants::EXPORT_TYPE}));
                }

                $htmlDataTop = '';
                $countdat = [];
                $countdatKeyValue = [];
                if (!empty($results_p)) {
                    foreach ($results_p as $rowds) {
                        if(empty($rowds['CON_TO'])){
                            continue;
                        }
                        $countdat[] = $rowds['CON_TO'];
                        $countdatKeyValue[$rowds['C_NAME']] = $rowds['CON_TO'];
                        $htmlDataTop .= '<tr>
                        <td>' . $rowds['C_CODE'] . '</td>
                        <td>' . $rowds['C_NAME'] . '</td>
                        <td class="text-right">
                            <a class="font-bold" href="' . convertToSslUrl(route('conflict-management', ['Filter' => 'Yes', 'CReportedStartDate' => $request->DRS, 'CReportedEndDate' => $request->DRE, 'AideCode' => $rowds['C_CODE']])) . '">' . number_format($rowds['CON_TO']) . '</a>
                        </td>
                        <td class="text-right">
                            ' . DollarF($rowds[DatabaseConstants::CON_SP]) . '
                        </td>
                        <td class="text-right">
                            ' . DollarF($rowds[DatabaseConstants::CON_OP]) . '
                        </td>
                        <td class="text-right">
                            ' . DollarF($rowds[DatabaseConstants::CON_FP]) . '
                        </td>
                        </tr>';
                    }
                }
                $htmlDataBottom = '';
                $exportLinks = $this->generateExportLinks($request->ctype, $request->DRS, $request->DRE);
                return response()->json([DashboardConstants::HTML_DATA_TOP => $htmlDataTop, DashboardConstants::HTML_DATA_BOTTOM => $htmlDataBottom, 'LinkMore' => $exportLinks[DashboardConstants::LINK_MORE], 'LinkCSV' => $exportLinks[DashboardConstants::LINK_CSV], 'LinkXLSX' => $exportLinks[DashboardConstants::LINK_XLSX]]);
            } else if ($request->DRS && $request->DRE && $request->ctype == 'Patient') {
                $query_p = str_replace(
                    ['{DB_SUFFIX}', '{USER_ID}', '{PROVIDER_ID}', '{START_DATE}', '{END_DATE}', '{CON_SP}', '{CON_OP}', '{CON_FP}', '{PFNAME}', '{PLNAME}', '{PNAME}'],
                    [$this->dbsuffix, $guid, $ProviderID, $request->DRS, $request->DRE, DatabaseConstants::CON_SP, DatabaseConstants::CON_OP, DatabaseConstants::CON_FP, DatabaseConstants::PFNAME, DatabaseConstants::PLNAME, DatabaseConstants::PNAME],
                    DatabaseConstants::QUERY_PROVIDER_PATIENT
                );
                $statement = $this->conn->prepare($query_p);
                $results_p = $statement->fetchAll(PDO::FETCH_ASSOC);
                if($request->ExportType)
                {
                    if(!in_array($request->ExportType, ['CSV', 'XLSX'])){
                        abort(403, 'Sorry !! You are Unauthorized to access this page');
                    }
                    return Excel::download(new DashboardConflictManagementExport(['results' => $results_p, 'ctype' => $request->{DashboardConstants::CTYPE}, 'ExportType' => $request->{DashboardConstants::EXPORT_TYPE}]), $request->{DashboardConstants::CTYPE}.DashboardConstants::PROVIDER_DASHBOARD_SUFFIX.date(DashboardConstants::DATE_FORMAT).'.'.strtolower($request->{DashboardConstants::EXPORT_TYPE}));
                }

                $htmlDataTop = '';
                $countdat = [];
                $countdatKeyValue = [];
                if (!empty($results_p)) {
                    foreach ($results_p as $rowds) {
                        if(empty($rowds['CON_TO'])){
                            continue;
                        }
                        $countdat[] = $rowds['CON_TO'];
                        $countdatKeyValue[$rowds[DatabaseConstants::PNAME]] = $rowds['CON_TO'];
                        $htmlDataTop .= '<tr>
                        <td>' . $rowds[DatabaseConstants::PLNAME] . '</td>
                        <td>' . $rowds[DatabaseConstants::PFNAME] . '</td>
                        <td class="text-right">
                            <a class="font-bold" href="' . convertToSslUrl(route('conflict-management', ['Filter' => 'Yes', 'CReportedStartDate' => $request->DRS, 'CReportedEndDate' => $request->DRE, 'PLName' => $rowds[DatabaseConstants::PLNAME], 'PFName' => $rowds[DatabaseConstants::PFNAME]])) . '">' . number_format($rowds['CON_TO']) . '</a>
                        </td>
                        <td class="text-right">
                            ' . DollarF($rowds[DatabaseConstants::CON_SP]) . '
                        </td>
                        <td class="text-right">
                            ' . DollarF($rowds[DatabaseConstants::CON_OP]) . '
                        </td>
                        <td class="text-right">
                            ' . DollarF($rowds[DatabaseConstants::CON_FP]) . '
                        </td>
                        </tr>';
                    }
                }
                $htmlDataBottom = '';
                $exportLinks = $this->generateExportLinks($request->ctype, $request->DRS, $request->DRE);
                return response()->json([DashboardConstants::HTML_DATA_TOP => $htmlDataTop, DashboardConstants::HTML_DATA_BOTTOM => $htmlDataBottom, 'LinkMore' => $exportLinks[DashboardConstants::LINK_MORE], 'LinkCSV' => $exportLinks[DashboardConstants::LINK_CSV], 'LinkXLSX' => $exportLinks[DashboardConstants::LINK_XLSX]]);
            } else if ($request->DRS && $request->DRE && $request->ctype == 'Payer') {
                $query_p = str_replace(
                    ['{DB_SUFFIX}', '{USER_ID}', '{PROVIDER_ID}', '{START_DATE}', '{END_DATE}', '{CON_SP}', '{CON_OP}', '{CON_FP}', '{PNAME}'],
                    [$this->dbsuffix, $guid, $ProviderID, $request->DRS, $request->DRE, DatabaseConstants::CON_SP, DatabaseConstants::CON_OP, DatabaseConstants::CON_FP, DatabaseConstants::PNAME],
                    DatabaseConstants::QUERY_PROVIDER_PAYER
                );
                $statement = $this->conn->prepare($query_p);
                $results_p = $statement->fetchAll(PDO::FETCH_ASSOC);
                if($request->ExportType)
                {
                    if(!in_array($request->ExportType, ['CSV', 'XLSX'])){
                        abort(403, 'Sorry !! You are Unauthorized to access this page');
                    }
                    return Excel::download(new DashboardConflictManagementExport(['results' => $results_p, 'ctype' => $request->{DashboardConstants::CTYPE}, 'ExportType' => $request->{DashboardConstants::EXPORT_TYPE}]), $request->{DashboardConstants::CTYPE}.DashboardConstants::PROVIDER_DASHBOARD_SUFFIX.date(DashboardConstants::DATE_FORMAT).'.'.strtolower($request->{DashboardConstants::EXPORT_TYPE}));
                }

                $htmlDataTop = '';
                $countdat = [];
                $countdatKeyValue = [];
                if (!empty($results_p)) {
                    foreach ($results_p as $rowds) {
                        if(empty($rowds['CON_TO'])){
                            continue;
                        }
                        $countdat[] = $rowds['CON_TO'];
                        $countdatKeyValue[$rowds[DatabaseConstants::PNAME]] = $rowds['CON_TO'];
                        $htmlDataTop .= '<tr>
                        <td>' . $rowds[DatabaseConstants::PNAME] . '</td>
                        <td class="text-right">
                            <a class="font-bold" href="' . convertToSslUrl(route('conflict-management', ['Filter' => 'Yes', 'CReportedStartDate' => $request->DRE, 'CReportedEndDate' => $request->DRE, 'PayerID' => $rowds['PAYERID']])) . '">' . number_format($rowds['CON_TO']) . '</a>
                        </td>
                        <td class="text-right">
                            ' . DollarF($rowds[DatabaseConstants::CON_SP]) . '
                        </td>
                        <td class="text-right">
                            ' . DollarF($rowds[DatabaseConstants::CON_OP]) . '
                        </td>
                        <td class="text-right">
                            ' . DollarF($rowds[DatabaseConstants::CON_FP]) . '
                        </td>
                        </tr>';
                    }
                }
                $htmlDataBottom = '';
                $exportLinks = $this->generateExportLinks($request->ctype, $request->DRS, $request->DRE);
                return response()->json([DashboardConstants::HTML_DATA_TOP => $htmlDataTop, DashboardConstants::HTML_DATA_BOTTOM => $htmlDataBottom, 'LinkMore' => $exportLinks[DashboardConstants::LINK_MORE], 'LinkCSV' => $exportLinks[DashboardConstants::LINK_CSV], 'LinkXLSX' => $exportLinks[DashboardConstants::LINK_XLSX]]);
            }
        } else if (auth()->user()->hasRole(DashboardConstants::ROLE_PAYER)) {
            $WhereInQuery = $PayerID;
            if ($request->DRS && $request->DRE && $request->ctype == 'ConflictType') {
                $query_conflict = str_replace(
                    ['{DB_SUFFIX}', '{PAYER_ID}', '{START_DATE}', '{END_DATE}'],
                    [$this->dbsuffix, $PayerID, $request->DRS, $request->DRE],
                    DatabaseConstants::QUERY_PAYER_CONFLICT_TYPE
                );
                $statement = $this->conn->prepare($query_conflict);
                $results_conflict = $statement->fetchAll(PDO::FETCH_ASSOC);
                
                $TotalRecords = $this->calculateTotalRecords($results_conflict, DatabaseConstants::CO_TO);
                if($request->{DashboardConstants::EXPORT_TYPE})
                {
                    if(!in_array($request->{DashboardConstants::EXPORT_TYPE}, [DashboardConstants::EXPORT_TYPE_CSV, DashboardConstants::EXPORT_TYPE_XLSX])){
                        abort(403, MessageConstants::UNAUTHORIZED_MESSAGE);
                    }
                    return Excel::download(new DashboardPayerConflictManagementExport(['results' => $results_conflict, 'TotalRecords' => $TotalRecords, ExportConstants::CTYPE => $request->{DashboardConstants::CTYPE}, ExportConstants::EXPORT_TYPE => $request->{DashboardConstants::EXPORT_TYPE}]), $request->{DashboardConstants::CTYPE}.DashboardConstants::PAYER_DASHBOARD_SUFFIX.date(DashboardConstants::DATE_FORMAT).'.'.strtolower($request->{DashboardConstants::EXPORT_TYPE}));
                }
                $htmlDataTop = '';
                $htmlDataTopHead = '';
                if (!empty($results_conflict)) {
                    // Conflict type mapping from abbreviated codes to numeric IDs
                    $conflictTypeMapping = [
                        'only_to' => 1,
                        'only_td' => 2,
                        'only_is' => 3,
                        'both_to_td' => 4,
                        'both_to_is' => 5,
                        'both_td_is' => 6,
                        'all_three' => 7
                    ];
                    
                    $TotalCount = 0;
                    $TotalShiftPrice = 0;
                    $TotalOverlapPrice = 0;
                    $TotalFinalPrice = 0;
                    foreach ($results_conflict as $rowres) {
                        if(empty($rowres['CO_TO'])){
                            continue;
                        }
                        $TotalCount += $rowres['CO_TO'];
                        $TotalShiftPrice += $rowres['CO_SP'];
                        $TotalOverlapPrice += $rowres['CO_OP'];
                        $TotalFinalPrice += $rowres['CO_FP'];
                        
                        // Map conflict type to numeric ID
                        $conflictTypeId = $conflictTypeMapping[$rowres['CONTYPE']] ?? $rowres['CONTYPE'];
                        
                        $htmlDataTop .= '<tr>
                            <td>' . $rowres['CONTYPEDESC'] . '</td>
                            <td class="text-right">
                                <a class="font-bold showloaderclick" href="' . convertToSslUrl(route(DashboardConstants::ROUTE_CONFLICT_MANAGEMENT, [DashboardConstants::FILTER => DashboardConstants::FILTER_VALUE_YES, 'VisitStartDate' => $request->DRS, 'VisitEndDate' => $request->DRE, 'ConflictType' => $conflictTypeId])) . '">' . number_format($rowres['CO_TO']) . '</a>
                            </td>
                            <td class="text-right">' . calculatePercentageRe($rowres['CO_TO'], $TotalRecords) . '</td>
                            <td class="text-right">' . DollarF($rowres['CO_SP']) . '</td>
                            <td class="text-right">' . DollarF($rowres['CO_OP']) . '</td>
                            <td class="text-right">' . DollarF($rowres['CO_FP']) . '</td>
                        </tr>';
                    }
                    if(!empty($TotalCount)){
                        $htmlDataTopHead = '<tr>
                            <th class="text-right1">Total</th>
                            <th class="text-right1">' . number_format($TotalCount) . '</th>
                            <th class="text-right1"></th>
                            <th class="text-right1">' . DollarF($TotalShiftPrice) . '</th>
                            <th class="text-right1">' . DollarF($TotalOverlapPrice) . '</th>
                            <th class="text-right1">' . DollarF($TotalFinalPrice) . '</th>
                        </tr>';
                    }
                }
                $exportLinks = $this->generateExportLinks($request->{DashboardConstants::CTYPE}, $request->DRS, $request->DRE);
                return response()->json([DashboardConstants::HTML_DATA_TOP => $htmlDataTop, 'htmlDataTopHead' => $htmlDataTopHead, 'LinkMore' => $exportLinks[DashboardConstants::LINK_MORE], 'LinkCSV' => $exportLinks[DashboardConstants::LINK_CSV], 'LinkXLSX' => $exportLinks[DashboardConstants::LINK_XLSX]]);
            } else if ($request->DRS && $request->DRE && $request->ctype == 'Agency') {

                // Get agency data using CTE with LEFT JOIN (single query approach)
                $query_agency = str_replace(
                    ['{DB_SUFFIX}', '{PAYER_ID}', '{START_DATE}', '{END_DATE}'],
                    [$this->dbsuffix, $PayerID, $request->DRS, $request->DRE],
                    DatabaseConstants::QUERY_PAYER_AGENCY_COUNT
                );
                $statement = $this->conn->prepare($query_agency);
                $results_agency = $statement->fetchAll(PDO::FETCH_ASSOC);
                
                $TotalRecords = $this->calculateTotalRecords($results_agency, 'CON_TO');
                if($request->ExportType)
                {
                    if(!in_array($request->ExportType, ['CSV', 'XLSX'])){
                        abort(403, 'Sorry !! You are Unauthorized to access this page');
                    }
                    return Excel::download(new DashboardPayerConflictManagementExport(['results' => $results_agency, 'TotalRecords' => $TotalRecords, ExportConstants::CTYPE => $request->ctype, ExportConstants::EXPORT_TYPE => $request->ExportType]), $request->ctype.DashboardConstants::PAYER_DASHBOARD_SUFFIX.date(DashboardConstants::DATE_FORMAT).'.'.strtolower($request->ExportType));
                }
                $htmlDataTop = '';
                $htmlDataTopHead = '';
                if (!empty($results_agency)) {
                    $TotalRec = 0;
                    $TotalShift = 0;
                    $TotalOverlap = 0;
                    $TotalFinal = 0;
                    foreach ($results_agency as $rowds) {
                        if(empty($rowds['CON_TO'])){
                            continue;
                        }
                        $TotalRec += $rowds['CON_TO'];
                        $TotalShift += $rowds['CON_SP'];
                        $TotalOverlap += $rowds['CON_OP'];
                        $TotalFinal += $rowds['CON_FP'];
                        $htmlDataTop .= '<tr>
                        <td>' . $rowds['P_NAME'] . '</td>
                        <td>' . $rowds['TIN'] . '</td>
                        <td class="text-right">
                            <a class="font-bold showloaderclick" href="' . convertToSslUrl(route(DashboardConstants::ROUTE_CONFLICT_MANAGEMENT, [DashboardConstants::FILTER => DashboardConstants::FILTER_VALUE_YES, 'VisitStartDate' => $request->DRS, 'VisitEndDate' => $request->DRE, 'ProviderID' => $rowds['PROVIDERID']])) . '">' . number_format($rowds['CON_TO']) . '</a>
                        </td>
                        <td class="text-right">' . calculatePercentageRe($rowds['CON_TO'], $TotalRecords) . '</td>
                        <td class="text-right">
                            ' . DollarF($rowds['CON_SP']) . '
                        </td>
                        <td class="text-right">
                            ' . DollarF($rowds['CON_OP']) . '
                        </td>
                        <td class="text-right">
                        ' . DollarF($rowds['CON_FP']) . '
                        </td>
                        </tr>';
                    }
                    if(!empty($TotalRec)){
                        $htmlDataTopHead = '<tr>
                            <td class="text-right1" colspan="2"><b>Total</b></td>
                            <td class="text-right1"><b>' . number_format($TotalRec) . '</b></td>
                            <td class="text-right1"></td>
                            <td class="text-right1"><b>' . DollarF($TotalShift) . '</b></td>
                            <td class="text-right1"><b>' . DollarF($TotalOverlap) . '</b></td>
                            <td class="text-right1"><b>' . DollarF($TotalFinal) . '</b></td>
                            </tr>';
                    }
                }
                $exportLinks = $this->generateExportLinks($request->ctype, $request->DRS, $request->DRE);
                return response()->json(['htmlDataTop' => $htmlDataTop, 'htmlDataTopHead' => $htmlDataTopHead, 'LinkMore' => $exportLinks[DashboardConstants::LINK_MORE], 'LinkCSV' => $exportLinks[DashboardConstants::LINK_CSV], 'LinkXLSX' => $exportLinks[DashboardConstants::LINK_XLSX]]);
            } else if ($request->DRS && $request->DRE && $request->{DashboardConstants::CTYPE} == 'Caregiver') {

                // Get caregiver data using CTE with LEFT JOIN (single query approach)
                $query_caregiver = str_replace(
                    ['{DB_SUFFIX}', '{PAYER_ID}', '{START_DATE}', '{END_DATE}'],
                    [$this->dbsuffix, $PayerID, $request->DRS, $request->DRE],
                    DatabaseConstants::QUERY_PAYER_CAREGIVER_COUNT
                );
                $statement = $this->conn->prepare($query_caregiver);
                $results_caregiver = $statement->fetchAll(PDO::FETCH_ASSOC);
                
                $TotalRecords = $this->calculateTotalRecords($results_caregiver, 'CON_TO');
                if($request->{DashboardConstants::EXPORT_TYPE})
                {
                    if(!in_array($request->{DashboardConstants::EXPORT_TYPE}, [DashboardConstants::EXPORT_TYPE_CSV, DashboardConstants::EXPORT_TYPE_XLSX])){
                        abort(403, MessageConstants::UNAUTHORIZED_MESSAGE);
                    }
                    return Excel::download(new DashboardPayerConflictManagementExport(['results' => $results_caregiver, 'TotalRecords' => $TotalRecords, ExportConstants::CTYPE => $request->{DashboardConstants::CTYPE}, ExportConstants::EXPORT_TYPE => $request->{DashboardConstants::EXPORT_TYPE}]), $request->{DashboardConstants::CTYPE}.DashboardConstants::PAYER_DASHBOARD_SUFFIX.date(DashboardConstants::DATE_FORMAT).'.'.strtolower($request->{DashboardConstants::EXPORT_TYPE}));
                }
                $htmlDataTop = '';
                $htmlDataTopHead = '';
                if (!empty($results_caregiver)) {
                    $TotalRec = 0;
                    $TotalShift = 0;
                    $TotalOverlap = 0;
                    $TotalFinal = 0;
                    foreach ($results_caregiver as $rowds) {
                        if(empty($rowds['CON_TO'])){
                            continue;
                        }
                        $TotalRec += $rowds['CON_TO'];
                        $TotalShift += $rowds['CON_SP'];
                        $TotalOverlap += $rowds['CON_OP'];
                        $TotalFinal += $rowds['CON_FP'];
                        // Parse C_NAME to extract first and last names
                        $nameParts = explode(' ', trim($rowds['C_NAME']));
                        $firstName = !empty($nameParts) ? $nameParts[0] : '';
                        $lastName = count($nameParts) > 1 ? end($nameParts) : '';
                        
                        $htmlDataTop .= '<tr>
                    <td>' . $rowds['C_NAME'] . '</td>
                    <td class="text-right">
                        <a class="font-bold showloaderclick" href="' . convertToSslUrl(route(DashboardConstants::ROUTE_CONFLICT_MANAGEMENT, [DashboardConstants::FILTER => DashboardConstants::FILTER_VALUE_YES, 'VisitStartDate' => $request->DRS, 'VisitEndDate' => $request->DRE, 'AideFName' => $firstName, 'AideLName' => $lastName])) . '">' . number_format($rowds['CON_TO']) . '</a>
                    </td>
                    <td class="text-right">' . calculatePercentageRe($rowds['CON_TO'], $TotalRecords) . '</td>
                    <td class="text-right">
                        ' . DollarF($rowds['CON_SP']) . '
                    </td>
                    <td class="text-right">
                        ' . DollarF($rowds['CON_OP']) . '
                    </td>
                    <td class="text-right">
                        ' . DollarF($rowds['CON_FP']) . '
                    </td>
                    </tr>';
                    }
                    if(!empty($TotalRec)){
                        $htmlDataTopHead = '<tr>
                            <td class="text-right1"><b>Total</b></td>
                            <td class="text-right1"><b>' . number_format($TotalRec) . '</b></td>
                            <td class="text-right1"></td>
                            <td class="text-right1"><b>' . DollarF($TotalShift) . '</b></td>
                            <td class="text-right1"><b>' . DollarF($TotalOverlap) . '</b></td>
                            <td class="text-right1"><b>' . DollarF($TotalFinal) . '</b></td>
                            </tr>';
                    }
                }
                $exportLinks = $this->generateExportLinks($request->{DashboardConstants::CTYPE}, $request->DRS, $request->DRE);
                return response()->json([DashboardConstants::HTML_DATA_TOP => $htmlDataTop, 'htmlDataTopHead' => $htmlDataTopHead, 'LinkMore' => $exportLinks[DashboardConstants::LINK_MORE], 'LinkCSV' => $exportLinks[DashboardConstants::LINK_CSV], 'LinkXLSX' => $exportLinks[DashboardConstants::LINK_XLSX]]);
            } else if ($request->DRS && $request->DRE && $request->{DashboardConstants::CTYPE} == 'Patient') {
                // Get patient data using CTE with LEFT JOIN (single query approach)
                $query_patient = str_replace(
                    ['{DB_SUFFIX}', '{PAYER_ID}', '{START_DATE}', '{END_DATE}'],
                    [$this->dbsuffix, $PayerID, $request->DRS, $request->DRE],
                    DatabaseConstants::QUERY_PAYER_PATIENT_COUNT
                );
                $statement = $this->conn->prepare($query_patient);
                $results_patient = $statement->fetchAll(PDO::FETCH_ASSOC);
                
                $TotalRecords = $this->calculateTotalRecords($results_patient, 'CON_TO');
                if($request->{DashboardConstants::EXPORT_TYPE})
                {
                    if(!in_array($request->{DashboardConstants::EXPORT_TYPE}, [DashboardConstants::EXPORT_TYPE_CSV, DashboardConstants::EXPORT_TYPE_XLSX])){
                        abort(403, MessageConstants::UNAUTHORIZED_MESSAGE);
                    }
                    return Excel::download(new DashboardPayerConflictManagementExport(['results' => $results_patient, 'TotalRecords' => $TotalRecords, ExportConstants::CTYPE => $request->{DashboardConstants::CTYPE}, ExportConstants::EXPORT_TYPE => $request->{DashboardConstants::EXPORT_TYPE}]), $request->{DashboardConstants::CTYPE}.DashboardConstants::PAYER_DASHBOARD_SUFFIX.date(DashboardConstants::DATE_FORMAT).'.'.strtolower($request->{DashboardConstants::EXPORT_TYPE}));
                }
                $htmlDataTop = '';
                $htmlDataTopHead = '';
                if (!empty($results_patient)) {
                    $TotalRec = 0;
                    $TotalShift = 0;
                    $TotalOverlap = 0;
                    $TotalFinal = 0;
                    foreach ($results_patient as $rowds) {
                        if(empty($rowds['CON_TO'])){
                            continue;
                        }
                        $TotalRec += $rowds['CON_TO'];
                        $TotalShift += $rowds['CON_SP'];
                        $TotalOverlap += $rowds['CON_OP'];
                        $TotalFinal += $rowds['CON_FP'];
                        $htmlDataTop .= '<tr>
                        <td>' . $rowds['PNAME'] . '</td>
                        <td>' . $rowds['ADMISSIONID'] . '</td>
                        <td class="text-right">
                            <a class="font-bold showloaderclick" href="' . convertToSslUrl(route(DashboardConstants::ROUTE_CONFLICT_MANAGEMENT, [DashboardConstants::FILTER => DashboardConstants::FILTER_VALUE_YES, 'VisitStartDate' => $request->DRS, 'VisitEndDate' => $request->DRE, 'PLName' => $rowds['PLNAME'], 'PFName' => $rowds['PFNAME']])) . '">' . number_format($rowds['CON_TO']) . '</a>
                        </td>
                        <td class="text-right">' . calculatePercentageRe($rowds['CON_TO'], $TotalRecords) . '</td>
                        <td class="text-right">
                            ' . DollarF($rowds['CON_SP']) . '
                        </td>
                        <td class="text-right">
                            ' . DollarF($rowds['CON_OP']) . '
                        </td>
                        <td class="text-right">
                            ' . DollarF($rowds['CON_FP']) . '
                        </td>
                        </tr>';
                    }
                    if(!empty($TotalRec)){
                        $htmlDataTopHead = '<tr>
                            <td class="text-right1" colspan="2"><b>Total</b></td>
                            <td class="text-right1"><b>' . number_format($TotalRec) . '</b></td>
                            <td class="text-right1"></td>
                            <td class="text-right1"><b>' . DollarF($TotalShift) . '</b></td>
                            <td class="text-right1"><b>' . DollarF($TotalOverlap) . '</b></td>
                            <td class="text-right1"><b>' . DollarF($TotalFinal) . '</b></td>
                            </tr>';
                    }
                }
                $exportLinks = $this->generateExportLinks($request->{DashboardConstants::CTYPE}, $request->DRS, $request->DRE);
                return response()->json(['htmlDataTop' => $htmlDataTop, 'htmlDataTopHead' => $htmlDataTopHead, 'LinkMore' => $exportLinks[DashboardConstants::LINK_MORE], 'LinkCSV' => $exportLinks[DashboardConstants::LINK_CSV], 'LinkXLSX' => $exportLinks[DashboardConstants::LINK_XLSX]]);
            } else if ($request->DRS && $request->DRE && $request->{DashboardConstants::CTYPE} == 'Payer') {

                // Get payer data using CTE with LEFT JOIN (single query approach)
                $query_payer = str_replace(
                    ['{DB_SUFFIX}', '{PAYER_ID}', '{START_DATE}', '{END_DATE}'],
                    [$this->dbsuffix, $PayerID, $request->DRS, $request->DRE],
                    DatabaseConstants::QUERY_PAYER_PAYER_COUNT
                );
                $statement = $this->conn->prepare($query_payer);
                $results_payer = $statement->fetchAll(PDO::FETCH_ASSOC);
                
                $TotalRecords = $this->calculateTotalRecords($results_payer, 'CON_TO');
                if($request->{DashboardConstants::EXPORT_TYPE})
                {
                    if(!in_array($request->{DashboardConstants::EXPORT_TYPE}, [DashboardConstants::EXPORT_TYPE_CSV, DashboardConstants::EXPORT_TYPE_XLSX])){
                        abort(403, MessageConstants::UNAUTHORIZED_MESSAGE);
                    }
                    return Excel::download(new DashboardPayerConflictManagementExport(['results' => $results_payer, 'TotalRecords' => $TotalRecords, ExportConstants::CTYPE => $request->{DashboardConstants::CTYPE}, ExportConstants::EXPORT_TYPE => $request->{DashboardConstants::EXPORT_TYPE}]), $request->{DashboardConstants::CTYPE}.DashboardConstants::PAYER_DASHBOARD_SUFFIX.date(DashboardConstants::DATE_FORMAT).'.'.strtolower($request->{DashboardConstants::EXPORT_TYPE}));
                }
                $htmlDataTop = '';
                $htmlDataTopHead = '';
                if (!empty($results_payer)) {
                    $TotalRec = 0;
                    $TotalShift = 0;
                    $TotalOverlap = 0;
                    $TotalFinal = 0;
                    foreach ($results_payer as $rowds) {
                        if(empty($rowds['CON_TO'])){
                            continue;
                        }
                        $TotalRec += $rowds['CON_TO'];
                        $TotalShift += $rowds['CON_SP'];
                        $TotalOverlap += $rowds['CON_OP'];
                        $TotalFinal += $rowds['CON_FP'];
                        $htmlDataTop .= '<tr>
                        <td>' . $rowds['PNAME'] . '</td>
                        <td class="text-right">
                            <a class="font-bold showloaderclick" href="' . convertToSslUrl(route(DashboardConstants::ROUTE_CONFLICT_MANAGEMENT, [DashboardConstants::FILTER => DashboardConstants::FILTER_VALUE_YES, 'VisitStartDate' => $request->DRS, 'VisitEndDate' => $request->DRE, 'PayerID' => $rowds['CONPAYERID']])) . '">' . number_format($rowds['CON_TO']) . '</a>
                        </td>
                        <td class="text-right">' . calculatePercentageRe($rowds['CON_TO'], $TotalRecords) . '</td>
                        <td class="text-right">
                            ' . DollarF($rowds['CON_SP']) . '
                        </td>
                        <td class="text-right">
                            ' . DollarF($rowds['CON_OP']) . '
                        </td>
                        <td class="text-right">
                            ' . DollarF($rowds['CON_FP']) . '
                        </td>
                        </tr>';
                    }
                    if(!empty($TotalRec)){
                        $htmlDataTopHead = '<tr>
                            <td class="text-right1"><b>Total</b></td>
                            <td class="text-right1"><b>' . number_format($TotalRec) . '</b></td>
                            <td class="text-right1"></td>
                            <td class="text-right1"><b>' . DollarF($TotalShift) . '</b></td>
                            <td class="text-right1"><b>' . DollarF($TotalOverlap) . '</b></td>
                            <td class="text-right1"><b>' . DollarF($TotalFinal) . '</b></td>
                            </tr>';
                    }
                }
                $exportLinks = $this->generateExportLinks($request->{DashboardConstants::CTYPE}, $request->DRS, $request->DRE);
                return response()->json(['htmlDataTop' => $htmlDataTop, 'htmlDataTopHead' => $htmlDataTopHead, 'LinkMore' => $exportLinks[DashboardConstants::LINK_MORE], 'LinkCSV' => $exportLinks[DashboardConstants::LINK_CSV], 'LinkXLSX' => $exportLinks[DashboardConstants::LINK_XLSX]]);
            }
        }

    }

    public function index_get_notifications_html(Request $request)
    {
        if (!Auth::user()->hasRole(DashboardConstants::ROLE_PROVIDER)) {
            abort(403, MessageConstants::UNAUTHORIZED_MESSAGE);
        }
        $envIds = $this->getEnvironmentIds();
        $ids = $this->assignIds($envIds);
        $ProviderID = $ids[DatabaseConstants::PROVIDER_ID];
        $AppProviderID = $ids[DatabaseConstants::APP_PROVIDER_ID];
        $UserID = Auth::user()->id;
        $conflictManagementModel = new ConflictManagementModel();
        $fetchtype = $request->fetchtype;
        // Fetch the total count of notifications
        $result_top_10 = [];
        $result_count = 0;
        if($fetchtype=='count'){
            $result_count = $conflictManagementModel->InNotificationCount($ProviderID, $AppProviderID, $UserID);
        }else if($fetchtype=='data'){
            $result_top_10 = $conflictManagementModel->InNotificationLatest($ProviderID, $AppProviderID, $UserID);
        }else{
            $result_count = $conflictManagementModel->InNotificationCount($ProviderID, $AppProviderID, $UserID);
            $result_top_10 = $conflictManagementModel->InNotificationLatest($ProviderID, $AppProviderID, $UserID);
        }

        $html = '';
        if (!empty($result_top_10)) {
            foreach ($result_top_10 as $notification) {
                $date = date('m/d/Y', strtotime($notification['CreatedDate']));
                $link = convertToSslUrl(route('conflict-detail', ['CONFLICTID' => $notification['CONFLICTID']]));
                $bgClass = empty($notification['ReadUnreadFlag']) ? 'bg-c9eeed' : '';
    
                $message = '';
                switch ($notification['NotificationType']) {
                    case 'New Conflict':
                        $message = "A new conflict has been reported.";
                        break;
                    case 'No Response':
                        $message = "ConflictID: {$notification['CONFLICTID']} has been marked as unresponsive.";
                        break;
                    case 'Resolved':
                        $message = "ConflictID: {$notification['CONFLICTID']} has been resolved.";
                        break;
                    case 'From Payer':
                        $message = "Payer {$notification['Contract']} has sent you a notice that ConflictID: {$notification['CONFLICTID']} is still unresolved. Please work on resolving it as soon as possible.";
                        break;
                    case 'Communication From Payer':
                        $message = "Payer {$notification['Contract']} has sent you a message for ConflictID: {$notification['CONFLICTID']}";
                        break;
                    case 'Communication From Provider':
                        $message = "Agency {$notification['Contract']} has sent you a message for ConflictID: {$notification['CONFLICTID']}";
                        break;
                }
    
                // Append to HTML
                $html .= "<li class='{$bgClass}'>";
                $html .= "{$date} - {$message} <a class='showloaderclick' href='{$link}'>Click here</a> to view conflict.";
                $html .= "</li>";
            }
        } else {
            $html .= "<li>No new message found.</li>";
        }
        return response()->json(['count' => $result_count, 'html' => $html]);
    }
}