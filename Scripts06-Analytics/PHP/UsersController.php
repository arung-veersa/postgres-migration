<?php

namespace App\Http\Controllers;

use App\Http\Controllers\Controller;
use App\Models\User;
use Illuminate\Http\Request;
use Spatie\Permission\Models\Role;
use Hash;
use Illuminate\Support\Facades\Auth;
use DB;
use Illuminate\Support\Facades\URL;
use Rdr\SnowflakeJodo\SnowflakeJodo;
use PDO;

class UsersController extends Controller
{

    protected $conn;
    protected $dbsuffix; // Define a protected property for table prefix
    public function __construct()
    {
        // Snowflake connection
        // $this->conn = SnowflakeJodo::connect();
        $this->conn = SnowflakeJodo::connect();
        $this->dbsuffix = env('DB_SUFFIX', '');
    }

    public function index(Request $request)
    {
        if (!Auth::user()->hasRole('Super Admin')) {
            abort(403, 'Sorry !! You are Unauthorized to access this page');
        }

        $results = User::GetUsers($request);
        $data['results'] = $results;

        $data['seo_title'] = 'Users Master';
        $data['is_page'] = 'users';
        $data['roles'] = Role::all();
        $data['states'] = DB::table('states')->get();
        return view('users.index', $data);
    }

    public function UserAdd(Request $request)
    {
        if (!Auth::user()->hasRole('Super Admin')) {
            abort(403, 'Sorry !! You are Unauthorized to access this page');
        }
        $clientidre = 'nullable';
        $AllPayerFlag = FALSE;
        if ($request->role == 'Governing Bodies' && $request->AllPayerFlag == '') {
            $clientidre = 'required';
            $AllPayerFlag = FALSE;
        } else if ($request->role == 'Governing Bodies' && $request->AllPayerFlag == '1') {
            $clientidre = 'nullable';
            $AllPayerFlag = TRUE;
        }
        $request->validate(
            [
                'role' => 'required',
                'client_id' => 'required_if:role,Provider,Payer',
                'client_id2' => $clientidre,
                'payer_state' => 'required_if:role,Governing Bodies',
                'first_name' => 'required',
                'last_name' => 'required',
                'phone' => 'required|regex:/^[(]?\d{3}[)]?[(\s)?.-]\d{3}[\s.-]\d{4}$/',
                'email' => 'required|max:100|email|unique:users',
                'address1' => 'required',
                'address2' => 'nullable',
                'city' => 'required',
                'zip_code' => 'required|numeric|digits_between:5,5',
                'state' => 'required',
                'status' => 'required',
                'password' => 'required|regex:/^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[@$!%*?&#])[A-Za-z\d@$!%*?&#]{8,20}$/',
            ],
            [
                'role.required' => 'The role field is required.',
                'client_id2.required' => 'The field is required.',
                'client_id.required_if' => 'The field is required.',
                'payer_state.required' => 'The payer state field is required.',
                'first_name.required' => 'The first name field is required.',
                'last_name.required' => 'The last name field is required.',
                'phone.required' => 'The phone number field is required.',
                'phone.numeric' => 'The phone number must be a number.',
                'phone.digits' => 'The phone number must be 10 digits.',
                'email.required' => 'The email field is required.',
                'email.email' => 'Please enter a valid email address.',
                'address1.required' => 'The address field is required.',
                'city.required' => 'The city field is required.',
                'zip_code.required' => 'The zip code field is required.',
                'zip_code.numeric' => 'The zip code must be a number.',
                'state.required' => 'The state field is required.',
                'status.required' => 'The status field is required.',
                'password.required' => 'The password field is required.',
                'password.confirmed' => 'The password confirmation does not match.',
                'password.min' => 'The password must be at least 8 characters.',
                'password.regex' => 'The password should be 8-20 characters, with at least one uppercase letter, one lowercase letter, one number and one special character @ $ ! % * ? & #'
            ]
        );
        // Create New User
        $user = new User();
        $user->first_name = $request->first_name ? ucfirst($request->first_name) : null;
        $user->last_name = $request->last_name ? ucfirst($request->last_name) : null;
        $user->name = ucfirst($request->first_name) . ' ' . ucfirst($request->last_name);

        $client_id = $request->client_id ?? null;
        if (!empty($client_id)) {
            $exploded_values = explode("~", $client_id);

            $id = $exploded_values[0];
            $application_id = $exploded_values[1];
            $application_name = $exploded_values[2];
        }


        if ($request->role == 'Provider') {
            $user->Provider_Id = $id ?? null;
            $user->Application_Provider_Id = $application_id ?? null;
            $user->Provider_Name = $application_name ?? null;
            $user->Payer_Id = null;
            $user->Application_Payer_Id = null;
            $user->Payer_Name = null;

        } else if ($request->role == 'Payer') {
            $user->Payer_Id = $id ?? null;
            $user->Application_Payer_Id = $application_id ?? null;
            $user->Payer_Name = $application_name ?? null;
            $user->Provider_Id = null;
            $user->Application_Provider_Id = null;
            $user->Provider_Name = null;
        } else {
            $user->Payer_Id = null;
            $user->Application_Payer_Id = null;
            $user->Payer_Name = null;
            $user->Provider_Id = null;
            $user->Application_Provider_Id = null;
            $user->Provider_Name = null;
        }

        $user->phone = $request->phone ?? null;
        $user->AllPayerFlag = $AllPayerFlag == TRUE ? 1 : null;
        $user->payer_state = $request->role == 'Governing Bodies' ? $request->payer_state : null;
        $user->email = $request->email ?? null;
        $user->address1 = $request->address1 ?? null;
        $user->address2 = $request->address2 ?? null;
        $user->city = $request->city ?? null;
        $user->zip_code = $request->zip_code ?? null;
        $user->state = $request->state ?? null;
        $user->status = $request->status ?? null;
        $user->email_verified_at = NOW();
        $user->password = Hash::make($request->password);
        $user->created_by = Auth()->id();
        $user->created_at = NOW();
        $user->assignRole($request->role);
        $user->save();

        $client_id2 = $request->client_id2 ?? [];
        if (!empty($client_id2) && $AllPayerFlag == FALSE && $request->role == 'Governing Bodies') {
            foreach ($client_id2 as $client_id) {
                $exploded_values = explode("~", $client_id);
                $id = $exploded_values[0] ?? null;
                $application_id = $exploded_values[1] ?? null;
                $application_name = $exploded_values[2] ?? null;
                $query = "INSERT INTO CONFLICTREPORT".$this->dbsuffix.".PUBLIC.GOVBODIESPAYERS
                (\"PayerID\", \"AppPayerID\", \"UserID\", \"PAYER_NAME\", created_at)
                VALUES ('$id', '$application_id', '$user->id', '$application_name', CURRENT_TIMESTAMP)";

                $stmt = $this->conn->prepare($query);
                $stmt->fetch(PDO::FETCH_ASSOC);
            }
        }

        session()->flash('success', 'User has been created !!');
        return 'success';
    }


    public function GetUserDetails(Request $request)
    {
        if (!Auth::user()->hasRole('Super Admin')) {
            abort(403, 'Sorry !! You are Unauthorized to access this page');
        }
        $user = User::with('roles')->find($request->id);
        if (empty($user)) {
            abort(404);
        }

        $check_query = "SELECT *
                FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.GOVBODIESPAYERS
                WHERE \"UserID\" = $user->id";

        $check_stmt = $this->conn->prepare($check_query);
        $governing_bodies_payers = $check_stmt->fetchAll(PDO::FETCH_ASSOC);


        $user->governing_bodies_payers = $governing_bodies_payers;
        return $user;
    }


    public function UserEditSave(Request $request, $id)
    {
        if (!Auth::user()->hasRole('Super Admin')) {
            abort(403, 'Sorry !! You are Unauthorized to access this page');
        }

        $user = User::find($id);
        if (empty($user)) {
            abort(404);
        }
        $clientidre = 'nullable';
        $AllPayerFlag = FALSE;
        if ($request->role == 'Governing Bodies' && $request->AllPayerFlag == '') {
            $clientidre = 'required';
            $AllPayerFlag = FALSE;
        } else if ($request->role == 'Governing Bodies' && $request->AllPayerFlag == '1') {
            $clientidre = 'nullable';
            $AllPayerFlag = TRUE;
        }
        $request->validate(
            [
                'role' => 'required',
                'client_id' => 'required_if:role,Provider,Payer',
                'client_id2' => $clientidre,
                'payer_state' => 'required_if:role,Governing Bodies',
                'first_name' => 'required',
                'last_name' => 'required',
                'phone' => 'required|regex:/^[(]?\d{3}[)]?[(\s)?.-]\d{3}[\s.-]\d{4}$/',
                'email' => 'required|max:100|email|unique:users,email,' . $id,
                'address1' => 'required',
                'address2' => 'nullable',
                'city' => 'required',
                'zip_code' => 'required|numeric|digits_between:5,5',
                'state' => 'required',
                'status' => 'required',
            ],
            [
                'role.required' => 'The role field is required.',
                'client_id2.required' => 'The field is required.',
                'client_id.required_if' => 'The field is required.',
                'payer_state.required' => 'The payer state field is required.',
                'first_name.required' => 'The first name field is required.',
                'last_name.required' => 'The last name field is required.',
                'phone.required' => 'The phone number field is required.',
                'phone.numeric' => 'The phone number must be a number.',
                'phone.digits' => 'The phone number must be 10 digits.',
                'email.required' => 'The email field is required.',
                'email.email' => 'Please enter a valid email address.',
                'address1.required' => 'The address field is required.',
                'city.required' => 'The city field is required.',
                'zip_code.required' => 'The zip code field is required.',
                'zip_code.numeric' => 'The zip code must be a number.',
                'state.required' => 'The state field is required.',
                'status.required' => 'The status field is required.',
            ]
        );

        // Edit User
        $user->first_name = $request->first_name ? ucfirst($request->first_name) : null;
        $user->last_name = $request->last_name ? ucfirst($request->last_name) : null;
        $user->name = ucfirst($request->first_name) . ' ' . ucfirst($request->last_name);
        $client_id = $request->client_id ?? null;
        if (!empty($client_id)) {
            $exploded_values = explode("~", $client_id);

            $id = $exploded_values[0];
            $application_id = $exploded_values[1];
            $application_name = $exploded_values[2];
        }


        if ($request->role == 'Provider') {
            $user->Provider_Id = $id ?? null;
            $user->Application_Provider_Id = $application_id ?? null;
            $user->Provider_Name = $application_name ?? null;
            $user->Payer_Id = null;
            $user->Application_Payer_Id = null;
            $user->Payer_Name = null;

        } else if ($request->role == 'Payer') {
            $user->Payer_Id = $id ?? null;
            $user->Application_Payer_Id = $application_id ?? null;
            $user->Payer_Name = $application_name ?? null;
            $user->Provider_Id = null;
            $user->Application_Provider_Id = null;
            $user->Provider_Name = null;
        } else {
            $user->Payer_Id = null;
            $user->Application_Payer_Id = null;
            $user->Payer_Name = null;
            $user->Provider_Id = null;
            $user->Application_Provider_Id = null;
            $user->Provider_Name = null;
        }
        $user->phone = $request->phone ?? null;
        $user->AllPayerFlag = $AllPayerFlag == TRUE ? 1 : null;
        $user->payer_state = $request->role == 'Governing Bodies' ? $request->payer_state : null;
        $user->email = $request->email ?? null;
        $user->address1 = $request->address1 ?? null;
        $user->address2 = $request->address2 ?? null;
        $user->city = $request->city ?? null;
        $user->zip_code = $request->zip_code ?? null;
        $user->state = $request->state ?? null;
        $user->status = $request->status ?? null;
        $user->updated_by = Auth()->id();
        $user->updated_at = NOW();
        $user->syncRoles($request->role);
        $user->save();

        $client_id2 = $request->client_id2 ?? [];
        $existing_client_ids = []; // Keep track of existing client_ids in the updated list
        if (!empty($client_id2) && $AllPayerFlag == FALSE && $request->role == 'Governing Bodies') {
            foreach ($client_id2 as $client_id) {
                $exploded_values = explode("~", $client_id);
                $id = $exploded_values[0] ?? null;
                $application_id = $exploded_values[1] ?? null;
                $application_name = $exploded_values[2] ?? null;

                // Check if a record exists for the current user and Payer_Id
                $check_query = "SELECT COUNT(*) AS record_count
                                    FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.GOVBODIESPAYERS
                                    WHERE \"UserID\" = '$user->id'
                                    AND \"PayerID\" = '$id'";

                $check_stmt = $this->conn->prepare($check_query);
                $check_result = $check_stmt->fetch(PDO::FETCH_ASSOC);

                if ($check_result['RECORD_COUNT'] > 0) {
                    $existing_client_ids[] = $id; // Add existing client_id to the list
                    // If a record exists, update it
                    $update_query = "UPDATE CONFLICTREPORT".$this->dbsuffix.".PUBLIC.GOVBODIESPAYERS
                                        SET 
                                            \"AppPayerID\" = '$application_id',
                                            \"PAYER_NAME\" = '$application_name',
                                            \"UPDATED_AT\" = CURRENT_TIMESTAMP
                                        WHERE 
                                            \"UserID\" = '$user->id'
                                            AND \"PayerID\" = '$id'";

                    $update_stmt = $this->conn->prepare($update_query);
                    $update_stmt->fetch(PDO::FETCH_ASSOC);
                } else {
                    // If no record exists, create a new one
                    $insert_query = "INSERT INTO CONFLICTREPORT".$this->dbsuffix.".PUBLIC.GOVBODIESPAYERS
                                        (\"PayerID\", \"AppPayerID\", \"UserID\", \"PAYER_NAME\", \"CREATED_AT\")
                                        VALUES ('$id', '$application_id', '$user->id', '$application_name', CURRENT_TIMESTAMP)";
                    $insert_stmt = $this->conn->prepare($insert_query);
                    $insert_stmt->fetch(PDO::FETCH_ASSOC);
                }
            }
        }
        if (!empty($existing_client_ids)) {
            $delete_query = "DELETE FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.GOVBODIESPAYERS WHERE \"UserID\" = '$user->id' AND \"PayerID\" NOT IN ('" . implode("','", $existing_client_ids) . "')";
            // Delete records for client_ids that were not present in the updated list        
            $delete_stmt = $this->conn->prepare($delete_query);
            $delete_stmt->fetch(PDO::FETCH_ASSOC);
        }
        if($AllPayerFlag == TRUE){
            $delete_query = "DELETE FROM CONFLICTREPORT".$this->dbsuffix.".PUBLIC.GOVBODIESPAYERS WHERE \"UserID\" = '$user->id'";
            // Delete records for client_ids that were not present in the updated list        
            $delete_stmt = $this->conn->prepare($delete_query);
            $delete_stmt->fetch(PDO::FETCH_ASSOC);
        }

        session()->flash('success', 'User has been updated !!');
        return 'success';
    }


    //Change Password
    public function changePassword(Request $request, $id)
    {
        if (!Auth::user()->hasRole('Super Admin')) {
            abort(403, 'Sorry !! You are Unauthorized to access this page');
        }

        $user = User::find($id);
        if (empty($user)) {
            abort(404);
        }

        $data['user'] = $user;
        $data['seo_title'] = 'Change Password (' . $user->name . ')';
        $data['is_page'] = 'users';

        return view('users.change-password', $data);
    }

    public function submitChangePassword(Request $request, $id)
    {
        if (!Auth::user()->hasRole('Super Admin')) {
            abort(403, 'Sorry !! You are Unauthorized to access this page');
        }

        $user = User::find($id);
        if (empty($user)) {
            abort(404);
        }

        $validated = $request->validate(
            [
                'NewPassword' => 'required|regex:/^(?=.*[a-z])(?=.*[A-Z])(?=.*\d)(?=.*[@$!%*?&#])[A-Za-z\d@$!%*?&#]{8,20}$/',
                'ConNewPassword' => 'required|same:NewPassword',
            ],
            [
                'NewPassword.required' => 'The field is required.',
                'NewPassword.regex' => 'The password should be 8-20 characters, with at least one uppercase letter, one lowercase letter, one number and one special character @ $ ! % * ? & #',
                'ConNewPassword.required' => 'The field is required.',
                'ConNewPassword.same' => 'The password and confirm password do not match.',
                'ConNewPassword.regex' => 'The password should be 8-20 characters, with at least one uppercase letter, one lowercase letter, one number and one special character @ $ ! % * ? & #',
            ]
        );

        $user->password = ($request->NewPassword) ? Hash::make($request->NewPassword) : NULL;
        $user->updated_at = date('Y-m-d H:i:s');
        $user->updated_by = Auth::user()->id;
        $user->save();

        if ($request->redirect) {
            // Validate the redirect URL
            $url = $request->redirect;
        
            // Check if the URL is valid and within the application's domain
            if (URL::isValidUrl($url) && str_starts_with($url, config('app.url'))) {
                return redirect($url)->with('success', 'Password has been updated successfully.!!');
            }
        
            // If the redirect is invalid or external, default to a safe location
            return redirect()->route('users')->with('success', 'Password has been updated successfully.!!');
        } else {
            return redirect()->route('users')->with('success', 'Password has been updated successfully.!!');
        }
    }


    public function UserDelete(Request $request, $id)
    {
        if (!Auth::user()->hasRole('Super Admin')) {
            abort(403, 'Sorry !! You are Unauthorized to access this page');
        }
        // if (is_null($this->user) || !$this->user->can('user.delete')) {
        //     abort(403, 'Sorry !! You are Unauthorized!');
        // }

        $user = User::find($id);
        if (empty($user)) {
            abort(404);
        }

        try {
            $cli = User::find($id);
            User::where('id', $id)->delete();
            session()->flash('success', 'User has been deleted !!');
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

    public function ClientInfoGet(Request $request)
    {
        if (!Auth::user()->hasRole('Payer') && !Auth::user()->hasRole('Provider')) {
            abort(403, 'Sorry !! You are Unauthorized to access this page');
        }
        $search = $request->q ?? '';
        $payer_state = $request->payer_state ?? '';
        $page = $request->page ?? 1;
        $pageSize = 50; // Number of results per page
        $offset = ($page - 1) * $pageSize;

        $filteredResults = $request->filteredResults ?? [];

        $noshowingids = array();
        foreach ($filteredResults as $result) {
            $parts = explode('~', $result);
            $id = $parts[0]; // Extracting the ID from the result
            $noshowingids[] = $id; // Storing the ID in the $ids array
        }

        if ($request->role == 'Provider') {
            $showonlyconflict = $request->showonlyconflict ? true : false;
            $providerIds = env('PROVIDERIDS');
            $aproviderIds = env('APROVIDERIDS');
            if (!empty($providerIds)) {
                // Use the value of providerIds from .env
                $ProviderID = $providerIds;
            } else {
                // Use the default variable
                $ProviderID = Auth::user()->Provider_Id; // Replace with your default value
            }
            if (!empty($aproviderIds)) {
                // Use the value of providerIds from .env
                $AppProviderID = $aproviderIds;
            } else {
                // Use the default variable
                $AppProviderID = Auth::user()->Application_Provider_Id; // Replace with your default value
            }
            $query = "SELECT DISTINCT CONCAT(D.\"Provider Id\", '~', D.\"Application Provider Id\", '~', D.\"Provider Name\") AS \"id\", CONCAT(D.\"Provider Name\", ' (', D.\"Application Provider Id\", ')') AS \"text\" FROM ANALYTICS".$this->dbsuffix.".BI.DIMPROVIDER AS D";
            if($showonlyconflict && !empty($ProviderID) && !empty($AppProviderID)){
                $query .= " INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS CVM ON CVM.\"ConProviderID\" = D.\"Provider Id\" AND CVM.\"ConProviderID\" IS NOT NULL AND CVM.\"ProviderID\" = '".$ProviderID."'";
            }
            $query .= " WHERE D.\"Is Active\" = TRUE AND D.\"Is Demo\" = FALSE";
            $query_count = "SELECT COUNT(DISTINCT D.\"Provider Id\") AS \"count\" FROM ANALYTICS".$this->dbsuffix.".BI.DIMPROVIDER AS D";
            if($showonlyconflict && !empty($ProviderID) && !empty($AppProviderID)){
                $query_count .= " INNER JOIN CONFLICTREPORT".$this->dbsuffix.".PUBLIC.CONFLICTVISITMAPS AS C ON C.\"ConProviderID\" = D.\"Provider Id\" AND C.\"ConProviderID\" IS NOT NULL AND C.\"ProviderID\" = '".$ProviderID."'";
            }
            $query_count .= " WHERE D.\"Is Active\" = TRUE AND D.\"Is Demo\" = FALSE";
            if (!empty($search)) {
                $query .= " AND D.\"Provider Name\" ILIKE '%$search%'";
                $query_count .= " AND D.\"Provider Name\" ILIKE '%$search%'";
            }

            //Not showing query
            if (!empty($noshowingids)) {
                $idsList = "'" . implode("','", $noshowingids) . "'";
                $query .= " AND \"Provider Id\" NOT IN ($idsList)";
            }

            $query .= " ORDER BY CONCAT(D.\"Provider Name\", ' (', D.\"Application Provider Id\", ')') ASC";
        } else if ($request->role == 'Payer' || $request->role == 'Governing Bodies') {
            $query = "SELECT CONCAT(\"Payer Id\", '~', \"Application Payer Id\", '~', \"Payer Name\") AS \"id\", CONCAT(\"Payer Name\", ' (', \"Application Payer Id\", ')') AS \"text\" FROM ANALYTICS".$this->dbsuffix.".BI.DIMPAYER WHERE \"Is Active\" = TRUE AND \"Is Demo\" = FALSE";
            $query_count = "SELECT COUNT(\"Payer Id\") AS \"count\" FROM ANALYTICS".$this->dbsuffix.".BI.DIMPAYER WHERE \"Is Active\" = TRUE AND \"Is Demo\" = FALSE";
            if (!empty($search)) {
                $query .= " AND \"Payer Name\" ILIKE '%$search%'";
                $query_count .= " AND \"Payer Name\" ILIKE '%$search%'";
            }
            if (!empty($payer_state)) {
                $query .= " AND LOWER(\"Payer State\") = LOWER('$payer_state')";
            }
            $query .= " ORDER BY CONCAT(\"Payer Name\", ' (', \"Application Payer Id\", ')') ASC";
        } else {
            $query = '';
        }

        if (!empty($request->role)) {
            $query .= ' LIMIT ' . $pageSize . ' OFFSET ' . $offset;
            $statement = $this->conn->prepare($query);
            $statement_count = $this->conn->prepare($query_count);

            $total_results = $statement_count->fetch(PDO::FETCH_ASSOC);
            $rowCount = $total_results['count'];
            $more = ($offset + $pageSize) < $rowCount;
            $results = $statement->fetchAll(PDO::FETCH_ASSOC);
        } else {
            $results = [];
            $more = [];
        }
        return response()->json(['items' => $results, 'more' => $more]);
    }

    public function ClientInfoGetAll(Request $request)
    {
        if (!Auth::user()->hasRole('Payer') && !Auth::user()->hasRole('Provider')) {
            abort(403, 'Sorry !! You are Unauthorized to access this page');
        }
        $search = $request->q ?? '';
        $page = $request->page ?? 1;
        $pageSize = 50; // Number of results per page
        $offset = ($page - 1) * $pageSize;

        $filteredResults = $request->filteredResults ?? [];

        $noshowingids = array();
        foreach ($filteredResults as $result) {
            $parts = explode('~', $result);
            $id = $parts[0]; // Extracting the ID from the result
            $noshowingids[] = $id; // Storing the ID in the $ids array
        }

        $query = "SELECT CONCAT(\"Provider Id\", '~', \"Application Provider Id\", '~', \"Provider Name\") AS \"id\", CONCAT(\"Provider Name\", ' (', \"Application Provider Id\", ')') AS \"text\" FROM ANALYTICS".$this->dbsuffix.".BI.DIMPROVIDER WHERE \"Is Active\" = TRUE AND \"Is Demo\" = FALSE";
        $query_count = "SELECT COUNT(\"Provider Id\") AS \"count\" FROM ANALYTICS".$this->dbsuffix.".BI.DIMPROVIDER WHERE \"Is Active\" = TRUE AND \"Is Demo\" = FALSE";
        if (!empty($search)) {
            $query .= " AND \"Provider Name\" ILIKE '%$search%'";
            $query_count .= " AND \"Provider Name\" ILIKE '%$search%'";
        }

        //Not showing query
        if (!empty($noshowingids)) {
            $idsList = "'" . implode("','", $noshowingids) . "'";
            $query .= " AND \"Provider Id\" NOT IN ($idsList)";
        }

        $query .= " ORDER BY CONCAT(\"Provider Name\", ' (', \"Application Provider Id\", ')') ASC";

        $query .= ' LIMIT ' . $pageSize . ' OFFSET ' . $offset;
        $statement = $this->conn->prepare($query);
        $statement_count = $this->conn->prepare($query_count);

        $total_results = $statement_count->fetch(PDO::FETCH_ASSOC);
        $rowCount = $total_results['count'];
        $more = ($offset + $pageSize) < $rowCount;
        $results = $statement->fetchAll(PDO::FETCH_ASSOC);
        return response()->json(['items' => $results, 'more' => $more]);
    }
}
