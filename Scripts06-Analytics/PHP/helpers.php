<?php 
use Illuminate\Support\Facades\Auth;
function RedirectURI()
 {
     return urlencode(Request::fullUrl());
 }

 function DollarF($number, $for=2) {
    if(empty($number)){
        return '$0.00';
    }
    // Format the number with two decimal places and commas as thousands separators
    $formattedNumber = number_format($number, $for);
    // Add a dollar sign in front of the formatted number
    $formattedCurrency = '$' . $formattedNumber;
    return $formattedCurrency;
}

function getMinutes($starttime, $endtime){
    if(empty($starttime) || empty($endtime)){
        return 0;
    }
    $date1 = new DateTime($starttime);
    $date2 = new DateTime($endtime);
    // Swap values if $starttime is later than $endtime
    if ($starttime > $endtime) {
        $temp = $starttime;
        $starttime = $endtime;
        $endtime = $temp;
    }

    $interval = $date1->diff($date2);

    return ($interval->days * 24 * 60) + ($interval->h * 60) + $interval->i;
}

function getYAxisTicks($data, $numTicks = 10) {
    if(empty($data)){
        return [];
    }
    $minValue = min($data);
    $minValue = 0;
    $maxValue = custom_round(max($data));


    $ticks = [];
    $range = $maxValue - $minValue;
    
    // Calculate the raw interval
    $interval = $range / ($numTicks - 1);
    
    // Round the interval to the nearest 500
    $roundedInterval = custom_round($interval);

    // Generate the tick values
    $maxup = false;
    for ($i = 0; $i < $numTicks; $i++) {
        if($maxup==true){
            break;
        }
        $tickValue = $minValue + ($i * $roundedInterval);
        if ($tickValue > $maxValue) {
            $maxup = true;
        }
        $ticks[] = custom_round($tickValue);
    }
    // echo "<pre>";
    // print_r($ticks);
    // die;
    return array_reverse($ticks);
}

function calculatePercentage($dataPoint, $min, $max) {
    if ($min == $max) {
        return 0; // Avoid division by zero
    }
    $min = 0;
    $max = custom_round($max);
    return ($dataPoint - $min) / ($max - $min) * 100;
}

function calculatePercentageRe($part, $total) {
    if ($total == 0) {
        return number_format(0, 2).'%'; // To avoid division by zero
    }
    $percentage = ($part / $total) * 100;
    return number_format($percentage, 2).'%';
}

function custom_round($number) {
    // Divide the number by 10, round up to the nearest integer, and multiply back by 10
    if($number <= 25){
        return ceil($number / 5) * 5;
    }else if($number > 25 && $number <= 50){
        return ceil($number / 10) * 10;
    } else if($number > 50 && $number <= 100){
        return ceil($number / 20) * 20;
    } else {
        return ceil($number / 200) * 200;
    }
}

function getClassBasedOnPercentage($percentage) {
    if ($percentage >= 90) {
        return 'success';
    } elseif ($percentage >= 75) {
        return 'primary';
    } elseif ($percentage >= 50) {
        return 'alert';
    } elseif ($percentage >= 25) {
        return 'warning';
    } else {
        return 'bg-primary-light';
    }
}

function maskSSN($ssn) {
    if (empty($ssn)) {
        return null;
    }
    return "XXX-XX-" . substr($ssn, -4);
}

// function maskDigits($input) {
//     // Use a regex to match all digits except the first two
//     return preg_replace_callback('/\d+/', function ($matches) {
//         // Get the first two digits
//         $firstTwo = substr($matches[0], 0, 2);
//         // Mask the remaining digits
//         $masked = str_repeat('X', strlen($matches[0]) - 2);
//         return $firstTwo . $masked;
//     }, $input);
// }

function maskDigits($input) {
    // Blank input check
    if (empty($input)) {
        return $input;
    }
    return preg_replace_callback('/\d+/', function ($matches) {
        $number = $matches[0];
        
        // Agar number 2 se chhota hai, usko waise hi return karo
        if (strlen($number) <= 2) {
            return $number;
        }

        $firstTwo = substr($number, 0, 2);
        $masked = str_repeat('X', strlen($number) - 2);
        
        return $firstTwo . $masked;
    }, $input);
}

if (!function_exists('getCombinations')) {
    function getCombinations($array) {
        $combinations = [];

        foreach ($array as $key => $value) {
            $subset = array_values(array_diff($array, [$value]));
            $combinations[$value] = $subset;
        }

        return $combinations;
    }
}
if (!function_exists('convertToSslUrl')) {
    function convertToSslUrl($url) {
        // Parse the URL into components

        if (app()->environment(['local', 'testing'])) {
            return $url;
        }
        
        $parsedUrl = parse_url($url);

        // Check if the URL is already HTTPS
        if (isset($parsedUrl['scheme']) && $parsedUrl['scheme'] === 'https') {
            return $url; // Return as it is already secure
        }

        // Build the HTTPS URL
        $secureUrl = 'https://';

        // Add the host
        if (isset($parsedUrl['host'])) {
            $secureUrl .= $parsedUrl['host'];
        }

        // Add the path, if any
        if (isset($parsedUrl['path'])) {
            $secureUrl .= $parsedUrl['path'];
        }

        // Add the query string, if any
        if (isset($parsedUrl['query'])) {
            $secureUrl .= '?' . $parsedUrl['query'];
        }

        // Add the fragment, if any
        if (isset($parsedUrl['fragment'])) {
            $secureUrl .= '#' . $parsedUrl['fragment'];
        }

        return $secureUrl;
    }
}
if (!function_exists('ofcquery')) {
    function ofcquery($guid=''){
        if($guid){
            return "SELECT \"Office Id\" FROM ANALYTICS".env('DB_SUFFIX', '').".BI.DIMUSEROFFICES WHERE \"User Id\" = '".$guid."' AND \"Vendor Type\" = 'Vendor'";
        }else if (Auth::user()->hasRole('Provider')) {
            if($officeIds = Auth::user()->officeIds){
                return $officeIds;
            }else{
                return "'-999'";
            }
        }
        return false;
    }
}
if (!function_exists('getminusdays')) {
    function getminusdays(){
        // Target date (1st January 2025)
        $targetDate = strtotime('2025-01-01');
        // Current date (today)
        $today_re = strtotime(date('Y-m-d'));
        // Difference in seconds
        $diffInSeconds = $targetDate - $today_re;
        // Convert seconds to days
        $days = $diffInSeconds / (60 * 60 * 24);
        $dayret = 7;
        if (Auth::user()->hasRole('Provider')) {
            if(Auth::user()->Application_Provider_Id=='8246'){
                $dayret = (int) abs($days);
            }
        }else if (Auth::user()->hasRole('Payer')) {
            if(Auth::user()->Application_Payer_Id=='70503'){
                $dayret = (int) abs($days);
            }
        }
        return $dayret;
    }
}

if (!function_exists('getVendorInfo')) {
    function getVendorInfo($userId) {
        try {
            $dbsuffix = env('DB_SUFFIX', '');
            $conn = \Rdr\SnowflakeJodo\SnowflakeJodo::connect();
            $sql = "SELECT \"Application Vendor Id\" as vendorid, \"Vendor Id\" as gvid FROM ANALYTICS".env('DB_SUFFIX', '').".BI.DIMUSER WHERE \"User Id\" = '" . addslashes($userId) . "'";
            $statement = $conn->prepare($sql);
            $result = $statement->fetch(PDO::FETCH_ASSOC);
            return $result ?: null;
        } catch (\Exception $e) {
            error_log("getVendorInfo error: " . $e->getMessage());
            return null;
        }
    }
}
if (!function_exists('getAggregatorInfo')) {
    function getAggregatorInfo($userId) {
        try {
            $dbsuffix = env('DB_SUFFIX', '');
            $conn = \Rdr\SnowflakeJodo\SnowflakeJodo::connect();
            $sql = "SELECT \"Aggregator Database Name\" as aggregator_db_name FROM ANALYTICS".env('DB_SUFFIX', '').".BI.DIMUSER WHERE \"User Id\" = '" . addslashes($userId) . "'";
            $statement = $conn->prepare($sql);
            $result = $statement->fetch(PDO::FETCH_ASSOC);
            return $result ?: null;
        } catch (\Exception $e) {
            error_log("getAggregatorInfo error: " . $e->getMessage());
            return null;
        }
    }
}

if (!function_exists('getOrderByFieldForExport')) {
    /**
     * Get the ORDER BY field name for export queries based on show_widget filter
     */
    function getOrderByFieldForExport(string $showWidget): string
    {
        $widgetMapping = [
            'Final Value Impact' => 'CON_FP',
            'Overlap Value Impact' => 'CON_OP',
            'Shift Value Impact' => 'CON_SP',
            'Conflict Count' => 'CON_TO'
        ];
        
        return $widgetMapping[$showWidget] ?? 'CON_TO';
    }
}

if (!function_exists('getOrderByExpressionForExport')) {
    /**
     * Get the ORDER BY expression for export queries with correct CTE alias
     * For CON_TO, use first CTE alias; for impact fields, use second CTE alias
     */
    function getOrderByExpressionForExport(string $showWidget, string $firstCteAlias, string $secondCteAlias): string
    {
        $orderByField = getOrderByFieldForExport($showWidget);
        
        // If ordering by CON_TO, use first CTE alias; otherwise use second CTE alias
        if ($orderByField === 'CON_TO') {
            return $firstCteAlias . '.' . $orderByField;
        } else {
            return $secondCteAlias . '.' . $orderByField;
        }
    }
}

if (!function_exists('filterExportDataByMetric')) {
    /**
     * Filter export data to exclude records where the selected metric is 0 or null
     * This matches the UI behavior where records with 0 values are not shown in charts
     */
    function filterExportDataByMetric(array $data, string $showWidget): array
    {
        $metricField = getOrderByFieldForExport($showWidget);
        
        return array_filter($data, function($row) use ($metricField) {
            $value = $row[$metricField] ?? null;
            
            // Filter out null values and 0 values (matching UI behavior)
            if ($value === null) {
                return false;
            }
            
            // For numeric values, exclude 0
            $numericValue = is_numeric($value) ? (float)$value : 0;
            return $numericValue != 0;
        });
    }
}
