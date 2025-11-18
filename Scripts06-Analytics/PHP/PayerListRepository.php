<?php

namespace App\Repositories;

use Rdr\SnowflakeJodo\SnowflakeJodo;
use PDO;
use App\DTOs\PayerWidgetData;
use App\Helpers\ConflictTypeHelper;

class PayerListRepository
{
    protected $conn;
    protected $dbsuffix;
    protected $tablePrefix;

    public function __construct()
    {
        $this->dbsuffix = env('DB_SUFFIX', '');
        $this->conn = SnowflakeJodo::connect();
        $this->tablePrefix = "CONFLICTREPORT{$this->dbsuffix}.PUBLIC.";
    }

    private function executeSelect(string $sql): array
    {
        try {
            $statement = $this->conn->prepare($sql);
            $result = $statement->fetch(PDO::FETCH_ASSOC);
            return $result ?: [];
        } catch (\Exception $e) {
            \Log::error('PayerListRepository executeSelect error', ['sql' => $sql, 'error' => $e->getMessage()]);
            throw $e;
        }
    }

    private function executeSelectAll(string $sql): array
    {
        try {
            $statement = $this->conn->prepare($sql);
            $result = $statement->fetchAll(PDO::FETCH_ASSOC);
            return $result ?: [];
        } catch (\Exception $e) {
            \Log::error('PayerListRepository executeSelectAll error', ['sql' => $sql, 'error' => $e->getMessage()]);
            throw $e;
        }
    }

    /**
     * Build WHERE clause conditions for filtering
     * 
     * @param array $payerIds Array of payer IDs
     * @param array $filters Array of filter parameters
     * @param string $tablePrefix Table prefix for column references (e.g., "V1." or "")
     * @return string Complete WHERE clause
     */
    private function buildWhereClause(array $payerIds, array $filters, string $tablePrefix = ''): string
    {
        if (empty($payerIds)) {
            return "1 = 0"; // Return condition that matches no records
        }

        $escapedIds = array_map('addslashes', $payerIds);
        $inList = "'" . implode("','", $escapedIds) . "'";
        $whereConditions = ["{$tablePrefix}\"PayerID\" IN ({$inList})"];
        if (!empty($filters['GroupID'])) {
            $whereConditions[] = "{$tablePrefix}\"GroupID\" = '" . addslashes($filters['GroupID']) . "'";
        }
        if (!empty($filters['PAdmissionID'])) {
            $whereConditions[] = "{$tablePrefix}\"PA_PAdmissionID\" ILIKE '%" . addslashes($filters['PAdmissionID']) . "%'";
        }
        if (!empty($filters['MedicaidID'])) {
            $whereConditions[] = "{$tablePrefix}\"PA_PMedicaidNumber\" ILIKE '%" . addslashes($filters['MedicaidID']) . "%'";
        }
        if (!empty($filters['PLName'])) {
            $whereConditions[] = "{$tablePrefix}\"PA_PLName\" ILIKE '%" . addslashes($filters['PLName']) . "%'";
        }
        if (!empty($filters['PFName'])) {
            $whereConditions[] = "{$tablePrefix}\"PA_PFName\" ILIKE '%" . addslashes($filters['PFName']) . "%'";
        }
        if (!empty($filters['AideLName'])) {
            $whereConditions[] = "{$tablePrefix}\"AideLName\" ILIKE '%" . addslashes($filters['AideLName']) . "%'";
        }
        if (!empty($filters['AideFName'])) {
            $whereConditions[] = "{$tablePrefix}\"AideFName\" ILIKE '%" . addslashes($filters['AideFName']) . "%'";
        }
        if (!empty($filters['status_flags']) && is_array($filters['status_flags'])) {
            $statusFlags = array_map('addslashes', $filters['status_flags']);
            if (in_array('R', $statusFlags)) {
                $statusFlags[] = 'D';
                $statusFlags = array_unique($statusFlags);
            }
            $statusFlagsList = "'" . implode("','", $statusFlags) . "'";
            $whereConditions[] = "{$tablePrefix}\"StatusFlag\" IN ({$statusFlagsList})";
        } elseif (!empty($filters['ConflictStatusFlag'])) {
            $statusFlag = addslashes($filters['ConflictStatusFlag']);
            if ($statusFlag == 'R') {
                $whereConditions[] = "{$tablePrefix}\"StatusFlag\" IN ('R', 'D')";
            } else {
                $whereConditions[] = "{$tablePrefix}\"StatusFlag\" = '$statusFlag'";
            }
        }
        if (!empty($filters['FlagForReview'])) {
            $flagForReview = addslashes($filters['FlagForReview']);
            if ($flagForReview == 'Yes') {
                $whereConditions[] = "{$tablePrefix}\"FlagForReview\" = '$flagForReview'";
            } else {
                $whereConditions[] = "({$tablePrefix}\"FlagForReview\" IS NULL OR {$tablePrefix}\"FlagForReview\" = 'No')";
            }
        }
        if (!empty($filters['PayerID'])) {
            if (is_array($filters['PayerID'])) {
                $selectedPayerIds = array_map('addslashes', $filters['PayerID']);
                $payerIdsList = "'" . implode("','", $selectedPayerIds) . "'";
                $whereConditions[] = "{$tablePrefix}\"PayerID\" IN ({$payerIdsList})";
            } else {
                $payerId = addslashes($filters['PayerID']);
                if (strpos($payerId, '~') !== false) {
                    $payerIdParts = explode('~', $payerId);
                    $payerId = $payerIdParts[0];
                }
                $whereConditions[] = "{$tablePrefix}\"PayerID\" = '$payerId'";
            }
        }

        if (!empty($filters['OfficeID'])) {
            $officeId = addslashes($filters['OfficeID']);
            if (strpos($officeId, '~') !== false) {
                $officeIdParts = explode('~', $officeId);
                $officeId = $officeIdParts[0];
            }
            $whereConditions[] = "{$tablePrefix}\"OfficeID\" = '$officeId'";
        }
        
        if (!empty($filters['ProviderID'])) {
            if (is_array($filters['ProviderID'])) {
                $selectedProviderIds = array_map('addslashes', $filters['ProviderID']);
                $providerIdsList = "'" . implode("','", $selectedProviderIds) . "'";
                $whereConditions[] = "{$tablePrefix}\"ProviderID\" IN ({$providerIdsList})";
            } else {
                $providerId = addslashes($filters['ProviderID']);
                if (strpos($providerId, '~') !== false) {
                    $providerIdParts = explode('~', $providerId);
                    $providerId = $providerIdParts[0];
                }
                $whereConditions[] = "{$tablePrefix}\"ProviderID\" = '$providerId'";
            }
        }
        
        if (!empty($filters['Contract'])) {
            $contract = addslashes($filters['Contract']);
            $whereConditions[] = "{$tablePrefix}\"Contract\" ILIKE '%$contract%'";
        }
        

        if (!empty($filters['selected_provider_ids']) && is_array($filters['selected_provider_ids'])) {
            $escapedProviderIds = array_map('addslashes', $filters['selected_provider_ids']);
            $providerIdsList = "'" . implode("','", $escapedProviderIds) . "'";
            $whereConditions[] = "{$tablePrefix}\"ProviderID\" IN ({$providerIdsList})";
        }
        
        if (!empty($filters['selected_service_code_ids']) && is_array($filters['selected_service_code_ids'])) {
            $serviceCodeConditions = [];
            $specificServiceCodes = array_filter($filters['selected_service_code_ids'], function($serviceCode) {
                return $serviceCode !== '(blank)';
            });
            if (!empty($specificServiceCodes)) {
                $serviceCodeIn = implode("','", array_map('addslashes', $specificServiceCodes));
                $serviceCodeConditions[] = "UPPER({$tablePrefix}\"ServiceCode\") IN ('{$serviceCodeIn}')";
            }
            if (in_array('(blank)', $filters['selected_service_code_ids'])) {
                $serviceCodeConditions[] = "({$tablePrefix}\"ServiceCode\" IS NULL OR TRIM({$tablePrefix}\"ServiceCode\") = '')";
            }
            if (!empty($serviceCodeConditions)) {
                $whereConditions[] = "(" . implode(' OR ', $serviceCodeConditions) . ")";
            }
        }
        
        if (!empty($filters['selected_county_ids']) && is_array($filters['selected_county_ids'])) {
            $countyConditions = [];
            $specificCounties = array_filter($filters['selected_county_ids'], function($county) {
                return $county !== '(blank)';
            });
            if (!empty($specificCounties)) {
                $countyIn = implode("','", array_map('addslashes', $specificCounties));
                if ($tablePrefix === 'V1.') {
                    $countyField = "UPPER(COALESCE({$tablePrefix}\"PA_PCounty\", {$tablePrefix}\"P_PCounty\"))";
                } else {                    // For view query, use COUNTY field directly
                    $countyField = "UPPER({$tablePrefix}\"COUNTY\")";
                }
                $countyConditions[] = "{$countyField} IN ('{$countyIn}')";
            }
            
            if (in_array('(blank)', $filters['selected_county_ids'])) {
                if ($tablePrefix === 'V1.') {
                    $blankCondition = "(COALESCE({$tablePrefix}\"PA_PCounty\", {$tablePrefix}\"P_PCounty\") IS NULL OR TRIM(COALESCE({$tablePrefix}\"PA_PCounty\", {$tablePrefix}\"P_PCounty\")) = '')";
                } else {
                    $blankCondition = "({$tablePrefix}\"COUNTY\" IS NULL OR TRIM({$tablePrefix}\"COUNTY\") = '')";
                }
                $countyConditions[] = $blankCondition;
            }
            if (!empty($countyConditions)) {
                $whereConditions[] = "(" . implode(' OR ', $countyConditions) . ")";
            }
        }
        
        if (!empty($filters['VisitStartDate']) && !empty($filters['VisitEndDate'])) {
            $startDate = addslashes($filters['VisitStartDate']);
            $endDate = addslashes($filters['VisitEndDate']);
            $whereConditions[] = "{$tablePrefix}\"VisitDate\" BETWEEN '$startDate' AND '$endDate'";
        } elseif (!empty($filters['VisitStartDate'])) {
            $startDate = addslashes($filters['VisitStartDate']);
            $whereConditions[] = "{$tablePrefix}\"VisitDate\" >= '$startDate'";
        } elseif (!empty($filters['VisitEndDate'])) {
            $endDate = addslashes($filters['VisitEndDate']);
            $whereConditions[] = "{$tablePrefix}\"VisitDate\" <= '$endDate'";
        }
        
        // Billed Date filters
        if (!empty($filters['BilledStartDate']) && !empty($filters['BilledEndDate'])) {
            $startDate = addslashes($filters['BilledStartDate']);
            $endDate = addslashes($filters['BilledEndDate']);
            $whereConditions[] = "TO_CHAR({$tablePrefix}\"BilledDate\", 'YYYY-MM-DD') BETWEEN '$startDate' AND '$endDate'";
        } elseif (!empty($filters['BilledStartDate'])) {
            $startDate = addslashes($filters['BilledStartDate']);
            $whereConditions[] = "TO_CHAR({$tablePrefix}\"BilledDate\", 'YYYY-MM-DD') >= '$startDate'";
        } elseif (!empty($filters['BilledEndDate'])) {
            $endDate = addslashes($filters['BilledEndDate']);
            $whereConditions[] = "TO_CHAR({$tablePrefix}\"BilledDate\", 'YYYY-MM-DD') <= '$endDate'";
        }
        
        // Conflict Reported Date filters
        if (!empty($filters['CReportedStartDate']) && !empty($filters['CReportedEndDate'])) {
            $startDate = addslashes($filters['CReportedStartDate']);
            $endDate = addslashes($filters['CReportedEndDate']);
            $whereConditions[] = "TO_CHAR({$tablePrefix}\"CRDATEUNIQUE\", 'YYYY-MM-DD') BETWEEN '$startDate' AND '$endDate'";
        } elseif (!empty($filters['CReportedStartDate'])) {
            $startDate = addslashes($filters['CReportedStartDate']);
            $whereConditions[] = "TO_CHAR({$tablePrefix}\"CRDATEUNIQUE\", 'YYYY-MM-DD') >= '$startDate'";
        } elseif (!empty($filters['CReportedEndDate'])) {
            $endDate = addslashes($filters['CReportedEndDate']);
            $whereConditions[] = "TO_CHAR({$tablePrefix}\"CRDATEUNIQUE\", 'YYYY-MM-DD') <= '$endDate'";
        }
        
        // Conflict Type filter (numbers mapping like ConflictManagementModel)
        if (!empty($filters['ConflictType'])) {
            $conflictType = (int)$filters['ConflictType'];
            $condition = $this->buildConflictTypeCondition($conflictType);
            if (!empty($condition)) {
                // Replace V1. prefix with the provided table prefix
                $condition = str_replace('V1."', "{$tablePrefix}\"", $condition);
                $whereConditions[] = $condition;
            }
        }   
        return implode(' AND ', $whereConditions);
    }
    
    public function getVisitsPayer(array $payerIds, array $filters, int $page = 1, int $perPage = 10): array
    {
        if (empty($payerIds)) {
            return [
                'data' => [],
                'total' => 0,
                'per_page' => $perPage,
                'current_page' => $page,
                'last_page' => 1
            ];
        }

        // Build WHERE clause using common method
        $whereClause = $this->buildWhereClause($payerIds, $filters, 'V1.');
        
        // Calculate pagination
        $offset = ($page - 1) * $perPage;
        
        if ($perPage === -1) {
            $perPage = 10000;
            $offset = 0;
        }
        
        // Build the main query exactly like ConflictManagementModel's getVisitsPayer
        // Optimized with proper indexing and query structure
        $SelectQuery = "SELECT
            V1.\"ID\",
            V1.\"GroupID\",
            V1.\"SSN\",
            V1.\"CaregiverID\",
            V1.\"AppCaregiverID\",
            V1.\"VisitID\",
            V1.\"AppVisitID\",
            V1.\"VisitDate\",
            V1.\"SchStartTime\",
            V1.\"SchEndTime\",
            V1.\"VisitStartTime\",
            V1.\"VisitEndTime\",
            V1.\"EVVStartTime\",
            V1.\"EVVEndTime\",
            V1.\"ShVTSTTime\",
            V1.\"ShVTENTime\",
            V1.\"CaregiverID\",
            V1.\"AppCaregiverID\",
            V1.\"AideCode\",
            V1.\"AideFName\",
            V1.\"AideLName\",
            COALESCE(V1.\"AideSSN\", V1.\"SSN\") AS \"AideSSN\",
            V1.\"G_CRDATEUNIQUE\" AS \"CRDATEUNIQUE\",
            V1.\"PayerID\",
            V1.\"Contract\",
            V1.\"InServiceFlag\",
            V1.\"PTOFlag\",
            V1.\"FlagForReview\",
            V1.\"PayerID\" AS \"APID\",
            UPPER(COALESCE(V1.\"PA_PCounty\", V1.\"P_PCounty\")) AS \"COUNTY\",
            ROW_NUMBER() OVER (PARTITION BY V1.\"GroupID\" ORDER BY V1.\"PayerID\" DESC) AS RN
        FROM
            {$this->tablePrefix}CONFLICTVISITMAPS AS V1
        INNER JOIN {$this->tablePrefix}CONFLICTS AS V2 ON V2.\"CONFLICTID\" = V1.\"CONFLICTID\"
        WHERE {$whereClause}";
        
        // Use CTE optimization like ConflictManagementModel
        $NewQuery = 'WITH ConflictVisitMaps AS (' . $SelectQuery . ')';
        
        $total = 0; // Initialize total to prevent undefined variable error
        try {
            $total = $this->getCountFromView($payerIds, $filters);
        } catch (\Exception $e) {
            \Log::error('PayerListRepository: Error getting count from view', ['error' => $e->getMessage()]);
        }
        
        // Calculate pagination
        $lastPage = max(1, ceil($total / $perPage));
        
        // Handle sorting parameters
        $sortField = $filters['SortByField'] ?? 'GroupID';
        $sortDirection = $filters['SortByAD'] ?? 'DESC';
        
        // Validate sort field to prevent SQL injection
        $allowedSortFields = ['GroupID', 'CRDATEUNIQUE', 'SSN', 'AideLName', 'AideFName', 'VisitDate'];
        if (!in_array($sortField, $allowedSortFields)) {
            $sortField = 'GroupID';
        }
        
        // Validate sort direction
        $sortDirection = strtoupper($sortDirection) === 'ASC' ? 'ASC' : 'DESC';
        
        // Build the final optimized query with pagination
        // Use proper sorting and limit optimization
        $query = $NewQuery . "SELECT sq.* FROM ConflictVisitMaps sq 
                 WHERE sq.RN = 1 
                 ORDER BY sq.\"{$sortField}\" {$sortDirection} 
                 LIMIT {$perPage} OFFSET {$offset}";
        
        // Execute the optimized query
        $data = $this->executeSelectAll($query);
        
        // Clear large variables to free memory
        unset($query, $NewQuery, $SelectQuery);
        
        return [
            'data' => $data,
            'total' => $total,
            'per_page' => $perPage,
            'current_page' => $page,
            'last_page' => $lastPage,
            'from' => $offset + 1,
            'to' => min($offset + $perPage, $total)
        ];
    }

    public function getChildVisits(array $groupIds, array $payerIds, array $aggregatorPayerIds, array $filters = []): array
    {
        if (empty($groupIds) || empty($payerIds)) {
            return [];
        }

        $escapedGroupIds = array_map('addslashes', $groupIds);
        $groupIdsList = "'" . implode("','", $escapedGroupIds) . "'";
        $aggregatorPayerIdsList = "'" . implode("','", $aggregatorPayerIds) . "'";
        $escapedPayerIds = array_map('addslashes', $payerIds);
        $payerIdsList = "'" . implode("','", $escapedPayerIds) . "'";
        $SQLChild = "SELECT DISTINCT 
            V1.\"CONFLICTID\",
            V1.\"VisitID\" AS \"AVID\",
            V1.\"GroupID\",
            V1.\"ProviderName\",
            DO.\"Federal Tax Number\" AS \"AgencyTIN\",
            DO.\"NPI\" AS \"AgencyNPI\",
            V1.\"Contract\",
            V1.\"ContractType\",
            V1.\"PayerID\",
            V1.\"AppPayerID\",
            V1.\"PayerID\" AS \"APID\",
            V1.\"VisitStartTime\",
            V1.\"VisitEndTime\",
            V1.\"SchStartTime\",
            V1.\"SchEndTime\",
            V1.\"EVVStartTime\",
            V1.\"EVVEndTime\",
            V1.\"ShVTSTTime\",
            V1.\"ShVTENTime\",
            V1.\"Office\",
            V1.\"EVVType\",
            V1.\"ServiceCode\",
            V1.\"DistanceMilesFromLatLng\",
            DATEDIFF(day, V1.\"CRDATEUNIQUE\", CURRENT_DATE) AS \"AgingDays\",
            CASE 
                WHEN V1.\"PayerID\" IN ({$aggregatorPayerIdsList}) THEN (V1.\"BilledRateMinute\" * 60)
                ELSE 0
            END AS \"BilledRate\",
            V1.\"BilledDate\",
            V1.\"BilledHours\",
            V1.\"TotalBilledAmount\",
            V1.\"VisitDate\",
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
            V1.\"PA_PStatus\",
            V1.\"AgencyContact\",
            V1.\"AgencyPhone\",
            V1.\"ShVTSTTime\",
            V1.\"ShVTENTime\",
            V1.\"SameSchTimeFlag\", 
            V1.\"SameVisitTimeFlag\", 
            V1.\"SchAndVisitTimeSameFlag\", 
            V1.\"SchOverAnotherSchTimeFlag\", 
            V1.\"VisitTimeOverAnotherVisitTimeFlag\", 
            V1.\"SchTimeOverVisitTimeFlag\", 
            V1.\"DistanceFlag\", 
            V1.\"InServiceFlag\", 
            V1.\"PTOFlag\",
            TIMESTAMPDIFF(MINUTE, V1.\"ShVTSTTime\", V1.\"ShVTENTime\") AS \"TotalMinutes\"   
        FROM {$this->tablePrefix}CONFLICTVISITMAPS AS V1
        INNER JOIN {$this->tablePrefix}CONFLICTS AS V2 
            ON V2.\"CONFLICTID\" = V1.\"CONFLICTID\"
        LEFT JOIN ANALYTICS{$this->dbsuffix}.BI.DIMOFFICE AS DO 
            ON DO.\"Office Id\" = V1.\"OfficeID\"
        INNER JOIN (
            SELECT 
                a.\"GroupID\",
                a.\"CONFLICTID\",
                a.\"BilledRateMinute\",
                grp.\"GroupSize\",
                CASE 
                    WHEN a.\"BilledRateMinute\" = 0 OR a.\"APID\" NOT IN ({$aggregatorPayerIdsList}) THEN 0
                    ELSE CONFLICTREPORT{$this->dbsuffix}.PUBLIC.GETFULLSHIFTTIME(
                        a.\"BILLABLEMINUTESFULLSHIFT\", 
                        a.\"ShVTSTTime\", 
                        a.\"ShVTENTime\"
                    ) * a.\"BilledRateMinute\"
                END AS \"ShiftPrice\",
                SUM(
                    COALESCE(
                        CONFLICTREPORT{$this->dbsuffix}.PUBLIC.GETOVERLAPTIME(
                            NULL,
                            a.\"ShVTSTTime\",
                            a.\"ShVTENTime\",
                            b.\"ShVTSTTime\",
                            b.\"ShVTENTime\"
                        ), 0
                    )
                ) AS \"OverlapTime\",
                CASE 
                    WHEN a.\"BilledRateMinute\" = 0 OR a.\"APID\" NOT IN ({$aggregatorPayerIdsList}) THEN 0
                    WHEN grp.\"GroupSize\" = 2 AND a.\"BILLABLEMINUTESOVERLAP\" IS NOT NULL
                        THEN a.\"BILLABLEMINUTESOVERLAP\" * a.\"BilledRateMinute\"
                    ELSE SUM(
                        COALESCE(
                            CONFLICTREPORT{$this->dbsuffix}.PUBLIC.GETOVERLAPTIME(
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
                FROM {$this->tablePrefix}CONFLICTVISITMAPS 
                WHERE \"GroupID\" IN ({$groupIdsList})
            ) a
            LEFT JOIN (
                SELECT DISTINCT 
                    \"GroupID\", \"CONFLICTID\", \"ShVTSTTime\", \"ShVTENTime\"
                FROM {$this->tablePrefix}CONFLICTVISITMAPS 
                WHERE \"GroupID\" IN ({$groupIdsList})
            ) b
                ON a.\"GroupID\" = b.\"GroupID\" AND a.\"CONFLICTID\" <> b.\"CONFLICTID\"
            INNER JOIN (
                SELECT \"GroupID\", COUNT(DISTINCT \"CONFLICTID\") AS \"GroupSize\"
                FROM {$this->tablePrefix}CONFLICTVISITMAPS
                WHERE \"GroupID\" IN ({$groupIdsList})
                GROUP BY \"GroupID\"
            ) grp 
                ON grp.\"GroupID\" = a.\"GroupID\"
            GROUP BY 
                a.\"GroupID\", a.\"CONFLICTID\", a.\"BilledRateMinute\", 
                a.\"BILLABLEMINUTESOVERLAP\", a.\"BILLABLEMINUTESFULLSHIFT\", 
                a.\"ShVTSTTime\", a.\"ShVTENTime\", grp.\"GroupSize\", a.\"APID\"
        ) AS CVMCH 
        ON CVMCH.\"GroupID\" = V1.\"GroupID\" AND CVMCH.\"CONFLICTID\" = V1.\"CONFLICTID\"
        WHERE V1.\"GroupID\" IN ({$groupIdsList})
        ORDER BY V1.\"CONFLICTID\" ASC";

        $results = $this->executeSelectAll($SQLChild);
        
        // Clear large SQL query to free memory
        unset($SQLChild);
        
        // Post-processing logic from ConflictManagementModel
        foreach ($results as &$row) {
            if (
                (empty($row['VisitStartTime']) || empty($row['VisitEndTime'])) &&
                isset($row['InServiceFlag']) && $row['InServiceFlag'] === 'Y'
            ) {
                $row['VisitStartTime'] = $row['ShVTSTTime'];
                $row['VisitEndTime'] = $row['ShVTENTime'];
            }
        }
        unset($row); // Clear reference to free memory
        
        // Map conflict types to user-friendly names for each result
        foreach ($results as &$result) {
            $result['ConTypes'] = $this->mapConflictTypes($result);
        }
        unset($result); // Clear reference to free memory
        
        return $results;
    }

    private function getCountFromView(array $payerIds, array $filters): int
    {
        if (empty($payerIds)) {
            return 0;
        }

        // Build WHERE clause using common method (no table prefix for view)
        $whereClause = $this->buildWhereClause($payerIds, $filters, '');
        $countQuery = "SELECT COUNT(DISTINCT(VISIT_KEY)) AS \"count\" FROM {$this->tablePrefix}V_PAYER_CONFLICTS_LIST WHERE {$whereClause}";
        $totalResult = $this->executeSelect($countQuery);
        return (int)($totalResult['count'] ?? 0);
    }
    private function buildConflictTypeCondition(int $conflictType): string
    {
        return ConflictTypeHelper::buildConflictCondition($conflictType, 'V1');
    }
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
    public function getVisitDetail($GroupID, array $payerIds)
    {
        $GroupID = (int)$GroupID;
        $payerIdsList = "'" . implode("','", $payerIds) . "'";
        
        $query = "SELECT TOP 1 
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
            V1.\"CRDATEUNIQUE\", 
            V1.\"InServiceFlag\", 
            V1.\"PTOFlag\", 
            V1.\"FlagForReview\", 
            V1.\"PayerID\",
            V1.\"PayerID\" AS \"APID\"
        FROM {$this->tablePrefix}CONFLICTVISITMAPS AS V1 
        INNER JOIN {$this->tablePrefix}CONFLICTS AS V2 
            ON V2.\"CONFLICTID\" = V1.\"CONFLICTID\"
        WHERE V1.\"PayerID\" IN ({$payerIdsList})
        AND V1.\"GroupID\" IS NOT NULL
        AND V1.\"GroupID\" = {$GroupID}";
        
        return $this->executeSelect($query);
    }
    public function getConflictDetailData($GroupID, array $payerIds, array $aggregatorPayerIds)
    {
        $payerIdsList = "'" . implode("','", $payerIds) . "'";
        $aggregatorPayerIdsList = "'" . implode("','", $aggregatorPayerIds) . "'";
        
        $SQLChild = "SELECT DISTINCT 
            V1.\"CONFLICTID\",
            V1.\"VisitID\" AS \"AVID\",
            V1.\"GroupID\",
            V1.\"ProviderName\",
            V1.\"ProviderID\",
            V1.\"AppProviderID\",
            V1.\"PayerID\",
            V1.\"AppPayerID\",
            V1.\"PayerID\" AS \"APID\",
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
            CASE 
                WHEN V1.\"PayerID\" IN ({$aggregatorPayerIdsList}) THEN (V1.\"BilledRateMinute\" * 60)
                ELSE 0
            END AS \"BilledRate\",
            CASE 
                WHEN V1.\"PayerID\" IN ({$aggregatorPayerIdsList}) THEN V1.\"BilledHours\"
                ELSE 0
            END AS \"BilledHours\",
            CASE 
                WHEN V1.\"PayerID\" IN ({$aggregatorPayerIdsList}) THEN V1.\"TotalBilledAmount\"
                ELSE 0
            END AS \"TotalBilledAmount\",
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
            V1.\"IsMissed\", 
            V1.\"MissedVisitReason\",
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
        FROM {$this->tablePrefix}CONFLICTVISITMAPS AS V1
        INNER JOIN {$this->tablePrefix}CONFLICTS AS V2 
            ON V2.\"CONFLICTID\" = V1.\"CONFLICTID\"
        INNER JOIN (
            SELECT 
                a.\"GroupID\",
                a.\"CONFLICTID\",
                a.\"BilledRateMinute\",
                a.\"BILLABLEMINUTESOVERLAP\",
                grp.\"GroupSize\" AS \"GroupSize\",
                CASE 
                    WHEN a.\"BilledRateMinute\" = 0 OR a.\"APID\" NOT IN ({$aggregatorPayerIdsList}) THEN 0
                    ELSE CONFLICTREPORT{$this->dbsuffix}.PUBLIC.GETFULLSHIFTTIME(
                        a.\"BILLABLEMINUTESFULLSHIFT\", 
                        a.\"ShVTSTTime\", 
                        a.\"ShVTENTime\"
                    ) * a.\"BilledRateMinute\"
                END AS \"ShiftPrice\",
                SUM(
                    COALESCE(
                        CONFLICTREPORT{$this->dbsuffix}.PUBLIC.GETOVERLAPTIME(
                            NULL,
                            a.\"ShVTSTTime\",
                            a.\"ShVTENTime\",
                            b.\"ShVTSTTime\",
                            b.\"ShVTENTime\"
                        ), 0
                    )
                ) AS \"OverlapTime\",
                CASE 
                    WHEN a.\"BilledRateMinute\" = 0 OR a.\"APID\" NOT IN ({$payerIdsList}) THEN 0
                    WHEN grp.\"GroupSize\" = 2 AND a.\"BILLABLEMINUTESOVERLAP\" IS NOT NULL
                        THEN a.\"BILLABLEMINUTESOVERLAP\" * a.\"BilledRateMinute\"
                    ELSE SUM(
                        COALESCE(
                            CONFLICTREPORT{$this->dbsuffix}.PUBLIC.GETOVERLAPTIME(
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
                FROM {$this->tablePrefix}CONFLICTVISITMAPS
                WHERE \"GroupID\" = {$GroupID}
            ) a
            LEFT JOIN (
                SELECT DISTINCT
                    \"GroupID\", \"CONFLICTID\", \"ShVTSTTime\", \"ShVTENTime\"
                FROM {$this->tablePrefix}CONFLICTVISITMAPS
                WHERE \"GroupID\" = {$GroupID}
            ) b
                ON a.\"GroupID\" = b.\"GroupID\" AND a.\"CONFLICTID\" <> b.\"CONFLICTID\"
            INNER JOIN (
                SELECT \"GroupID\", COUNT(DISTINCT \"CONFLICTID\") AS \"GroupSize\"
                FROM {$this->tablePrefix}CONFLICTVISITMAPS
                WHERE \"GroupID\" = {$GroupID}
                GROUP BY \"GroupID\"
            ) grp
                ON grp.\"GroupID\" = a.\"GroupID\"
            GROUP BY
                a.\"GroupID\", a.\"CONFLICTID\", a.\"BilledRateMinute\",
                a.\"BILLABLEMINUTESOVERLAP\", a.\"BILLABLEMINUTESFULLSHIFT\",
                a.\"ShVTSTTime\", a.\"ShVTENTime\", grp.\"GroupSize\", a.\"APID\"
        ) AS CVMCH
        ON CVMCH.\"GroupID\" = V1.\"GroupID\" AND CVMCH.\"CONFLICTID\" = V1.\"CONFLICTID\"
        WHERE V1.\"GroupID\" = {$GroupID}
        ORDER BY V1.\"CONFLICTID\" ASC";

        $results = $this->executeSelectAll($SQLChild);
        
        // Clear large SQL query to free memory
        unset($SQLChild);

        // Post-processing logic from ConflictManagementModel
        foreach ($results as &$row) {
            if (
                (empty($row['VisitStartTime']) || empty($row['VisitEndTime'])) &&
                isset($row['InServiceFlag']) && $row['InServiceFlag'] === 'Y'
            ) {
                $row['VisitStartTime'] = $row['ShVTSTTime'];
                $row['VisitEndTime'] = $row['ShVTENTime'];
            }
        }
        unset($row); // Clear reference to free memory

        // Map conflict types to user-friendly names for each result
        foreach ($results as &$result) {
            $result['ConTypes'] = $this->mapConflictTypes($result);
        }
        unset($result); // Clear reference to free memory

        return $results;
    }

    public function getNextPrevRecord($currentGroupId, array $payerIds, array $filters, string $nextPrev): ?string
    {
        if (empty($payerIds)) {
            return null;
        }

        $escapedIds = array_map('addslashes', $payerIds);
        $inList = "'" . implode("','", $escapedIds) . "'";
        
        // Build WHERE clause using common method
        $whereClause = $this->buildWhereClause($payerIds, $filters, 'V1.');
        
        if ($nextPrev === 'Next') {
            $whereClause .= " AND V1.\"GroupID\" < " . (int)$currentGroupId;
            $orderBy = "ORDER BY V1.\"GroupID\" DESC";
        } else {
            $whereClause .= " AND V1.\"GroupID\" > " . (int)$currentGroupId;
            $orderBy = "ORDER BY V1.\"GroupID\" ASC";
        }
        
        $query = "SELECT TOP 1 V1.\"GroupID\"
                  FROM {$this->tablePrefix}CONFLICTVISITMAPS AS V1
                  INNER JOIN {$this->tablePrefix}CONFLICTS AS V2 ON V2.\"CONFLICTID\" = V1.\"CONFLICTID\"
                  WHERE {$whereClause}
                  {$orderBy}";
        
        $result = $this->executeSelect($query);
        
        if (!empty($result['GroupID'])) {
            return convertToSslUrl(route('conflict-detail', ['CONFLICTID' => $result['GroupID']]));
        }
        
        return null;
    }
}
