-- ANALYTICS.BI.DIMCAREGIVER source

create or replace view ANALYTICS.BI.DIMCAREGIVER(
	"Caregiver Id" COMMENT 'Caregiver Primary Key',
	"Application Caregiver Id",
	"Environment",
	"Caregiver Initials" COMMENT 'Initials',
	"Caregiver Fullname" COMMENT 'Full Name (calculated as \"Caregiver Firstname\"+\"Caregiver Lastname\")',
	"Caregiver Firstname" COMMENT 'First Name',
	"Caregiver Middlename" COMMENT 'Middle Name',
	"Caregiver Lastname" COMMENT 'Last Name',
	"Date of Birth" COMMENT 'Date of Birth',
	NPI COMMENT 'National Provider Identifier (NPI) **[Read more](https://www.cms.gov/Regulations-and-Guidance/Administrative-Simplification/NationalProvIdentStand)**',
	"License Number" COMMENT 'License Number',
	"Caregiver Code" COMMENT 'Caregiver Code',
	"Alternate Code" COMMENT 'Alternate Code',
	SSN COMMENT 'Social Security Number',
	"Employment Type" COMMENT 'Employment Type',
	"Ethnicity" COMMENT 'Ethnicity',
	"Hiring Status" COMMENT 'Hiring Status',
	"Registry Checked Date" COMMENT 'Registry Checked Date',
	"Registry Is Checked" COMMENT 'Registry Is Checked',
	"Office Caregiver Code" COMMENT 'Office Caregiver Code',
	"State Registry Number" COMMENT 'State Registry Number',
	"State Registered Date" COMMENT 'State Registered Date',
	"Status" COMMENT 'Status',
	"Primary Office Id" COMMENT 'Primary Office Id. Foreign key (dimOffice.\"Office Id\")',
	"Primary Environment Office Id",
	"Provider Id" COMMENT 'Provider Id. Foreign key (dimProvider.\"Provider Id\")',
	"Application Provider Id",
	"Caregiver Team" COMMENT 'Caregiver Team',
	"Caregiver Branch" COMMENT 'Caregiver Branch',
	"Caregiver Location" COMMENT 'Caregiver Location',
	"Created Date Time" COMMENT 'Created Date Time',
	"Gender" COMMENT 'Gender',
	"Hire Date",
	"Rehire Date",
	"Terminated Date",
	"Caregiver Name - ID" COMMENT 'Caregiver Report Application Identifier for system purposes',
	"Unique Caregiver Id",
	"Updated Datatimestamp",
	"Usage Tags" COMMENT 'Tags for system purposes',
	"Language 1",
	"Language 2",
	"Language 3",
	"Language 4",
	"Source System"
) WITH ROW ACCESS POLICY ANALYTICS.CORE.DATA_RESTRICT_POLICY ON ("Usage Tags")
 COMMENT='Dimension representing Caregivers, with a row per unique Caregiver. Primary Key - \"Caregiver Id\"'
 as (
    
 
with hha_caregiver as
(
    select

			GLOBAL_CAREGIVER_ID            as "Caregiver Id",
            environment_caregiver_id       as "Application Caregiver Id",
            environment                    as "Environment",
			INITIALS                       as "Caregiver Initials",
			CAREGIVER_NAME                 as "Caregiver Fullname",
			FIRST_NAME                     as "Caregiver Firstname",
			MIDDLE_NAME                    as "Caregiver Middlename",
			LAST_NAME                      as "Caregiver Lastname",
			DATE_OF_BIRTH                  as "Date of Birth",
			NPI                            as "NPI",
			LICENSE_NUMBER                 as "License Number",
			CODE                           as "Caregiver Code",
			ALTERNATE_CODE                 as "Alternate Code",
			SSN                            as "SSN",
			EMPLOYMENT_TYPE                as "Employment Type",
			ETHNICITY                      as "Ethnicity",
			HIRING_STATUS                  as "Hiring Status",
			REGISTRY_CHECKED_DATE          as "Registry Checked Date",
			REGISTRY_IS_CHECKED            as "Registry Is Checked",
			OFFICE_CAREGIVER_CODE          as "Office Caregiver Code", 
			STATE_REGISTRY_NUMBER          as "State Registry Number",
			STATE_REGISTRY_REGISTERED_DATE as "State Registered Date",
--			OFFICES                        as "Secondary Office List",
			STATUS                         as "Status",
            GLOBAL_OFFICE_ID               as "Primary Office Id",
            ENVIRONMENT_OFFICE_ID          as "Primary Environment Office Id",
            GLOBAL_PROVIDER_ID             as "Provider Id",
            ENVIRONMENT_PROVIDER_ID        as "Application Provider Id",
            team                           as "Caregiver Team",
            branch                         as "Caregiver Branch",
            location                       as "Caregiver Location",
            created_datetime               as "Created Date Time",
            gender                         as "Gender",
            hire_date                      as "Hire Date",
            rehire_date                    as "Rehire Date",
            terminated_date                as "Terminated Date",
            caregiver_name_id              as "Caregiver Name - ID",

            unique_caregiver_id            as "Unique Caregiver Id",
            updated_data_timestamp         as "Updated Datatimestamp",
            usage_tags                     as "Usage Tags",
            language1                      as "Language 1",
            language2                      as "Language 2",
            language3                      as "Language 3",
            language4                      as "Language 4"

    from core.caregiver

	where IS_DELETED = false
),

evv_caregiver as (
    select
        "Caregiver Id",
        "Application Caregiver Id",
        "Environment",
        "Caregiver Initials",
        "Caregiver Fullname",
        "Caregiver Firstname",
        "Caregiver Middlename",
        "Caregiver Lastname",
        "Date of Birth",
        "NPI",
        "License Number",
        "Caregiver Code",
        "Alternate Code",
        "SSN",
        "Employment Type",
        "Ethnicity",
        "Hiring Status",
        "Registry Checked Date",
        "Registry Is Checked",
        "Office Caregiver Code",
        "State Registry Number",
        "State Registered Date",
        "Status",
        "Primary Office Id",
        "Primary Environment Office Id",
        "Provider Id",
        "Application Provider Id",
        "Caregiver Team",
        "Caregiver Branch",
        "Caregiver Location",
        "Created Date Time",
        "Gender",
        "Hire Date",
        "Rehire Date",
        "Terminated Date",
        "Caregiver Name - ID",
        "Unique Caregiver Id",
        "Updated Datatimestamp",
        "Usage Tags",
        "Language 1",
        "Language 2",
        "Language 3",
        "Language 4"
    from core.evv_caregiver
)

select
    *, 'hha' as "Source System"
from hha_caregiver

union all

select
    *, 'evv' as "Source System"
from evv_caregiver
  )
/* {"app": "dbt", "dbt_version": "2025.11.4+d8fa9da", "profile_name": "user", "target_name": "prod", "node_id": "model.hha.DimCaregiver"} */;


-- ANALYTICS.BI.DIMCONTRACT source

create or replace view ANALYTICS.BI.DIMCONTRACT(
	"Contract Id" COMMENT 'Contract Primary Key',
	"Application Contract Id",
	"Environment",
	"Contract Name" COMMENT 'Contract Name',
	"Contract Start Date" COMMENT 'Contract Start Date',
	"Contract End Date" COMMENT 'Contract End Date',
	"Contact Person" COMMENT 'Contact Person',
	NPI COMMENT 'National Provider Identifier (NPI) **[Read more](https://www.cms.gov/Regulations-and-Guidance/Administrative-Simplification/NationalProvIdentStand)**',
	"Federal Tax Number" COMMENT 'Otherwise known as Employer Identification Number (EIN) **[Read more]( https://www.irs.gov/businesses/small-businesses-self-employed/employer-id-numbers/)**',
	"Is Private Pay" COMMENT 'Is Private Pay',
	"Is Medic Aid",
	"ICD Code" COMMENT 'International Classification of Diseases (ICD) Code **[Read more](https://www.who.int/standards/classifications/classification-of-diseases)**',
	"ICD Code Effective Date" COMMENT 'ICD Code Effective Date\"',
	"Is Active" COMMENT 'Is Active',
	"Address Type" COMMENT 'Address Type',
	"Address Line 1" COMMENT 'Address Line 1',
	"Address Line 2" COMMENT 'Address Line 2',
	"Address County" COMMENT 'Address County',
	"Address City" COMMENT 'Address City',
	"Address State" COMMENT 'Address State',
	"Address ZIP" COMMENT 'Address ZIP',
	"Address Latitude" COMMENT 'Address Latitude',
	"Address Longitude" COMMENT 'Address Longitude',
	"Phone Number 1" COMMENT 'Phone Number 1',
	"Phone Description 1" COMMENT 'Phone Description 1',
	"Phone Number 2" COMMENT 'Phone Number 2',
	"Phone Description 2" COMMENT 'Phone Description 2',
	"Contract Notes" COMMENT 'Contract Notes',
	"Office List",
	"Authorization Required" COMMENT 'Authorization Required',
	"Contract Type Name",
	"Unique Contract Id",
	"Updated Datatimestamp",
	"Usage Tags" COMMENT 'Tags for system purposes',
	"Source System"
) WITH ROW ACCESS POLICY ANALYTICS.CORE.DATA_RESTRICT_POLICY ON ("Usage Tags")
 COMMENT='Dimension representing Contracts, with a row per unique Contract. Primary Key - \"Contract Id\"'
 as (
    

with base_contract as (
    select *
    from core.contract
    where is_deleted = false

),

core_office as (
    select global_office_id,
            name
    from core.office
),

--Add a label and {} to make it easier to query using lateral flatten
contract_data as (
    select
        global_contract_id,
        parse_json('{"offices": ' || offices || '}')::variant as new_offices_json 
    from base_contract
),

flattened as (
    select
        global_contract_id,
        f.value:globalOfficeId::varchar as globalOfficeId
    from contract_data
    ,lateral flatten(input => new_offices_json:offices) f

),

--Office names have commas so use a different delimter
list_aggregated as (
  select distinct
    global_contract_id                                              as la_global_contract_id,
    listagg( cof.name, '||' )
        within group ( order by cof.name )
        over ( partition by global_contract_id ) ::varchar          as office_name_list                     --varchar(2000) results in truncation
  from flattened
  left outer join core_office cof
    on flattened.globalOfficeId = cof.global_office_id
  
)

, hha_contract as (
    select 
        global_contract_id				as "Contract Id",
        environment_contract_id         as "Application Contract Id",
        environment                     as "Environment",
        name							as "Contract Name",
        start_date						as "Contract Start Date",
        end_date						as "Contract End Date",
        contact_person					as "Contact Person",
        npi,
        tax								as "Federal Tax Number",
        is_private_pay					as "Is Private Pay",
        is_medic_aid					as "Is Medic Aid",
        icd_code						as "ICD Code",
        icd_code_effective_date			as "ICD Code Effective Date",
        is_active						as "Is Active",
        address_type					as "Address Type",
        address_line1					as "Address Line 1", 
        address_line2					as "Address Line 2",
        address_county					as "Address County",
        address_city					as "Address City",
        address_state					as "Address State",
        address_zip						as "Address ZIP",
        address_latitude				as "Address Latitude",
        address_longitude				as "Address Longitude",
        phone_number1					as "Phone Number 1",
        phone_description1				as "Phone Description 1",
        phone_number2					as "Phone Number 2",
        phone_description2				as "Phone Description 2",
        notes							as "Contract Notes",
        la.office_name_list				as "Office List",
        authorization_required			as "Authorization Required",
        contract_type_name              as "Contract Type Name",
        unique_contract_id              as "Unique Contract Id",
        updated_data_timestamp          as "Updated Datatimestamp",
        usage_tags                      as "Usage Tags"

    from base_contract bc
    left outer join list_aggregated la on
        bc.global_contract_id = la.la_global_contract_id
)

, evv_contract as 
(
    select 
        "Contract Id",
        "Application Contract Id",
        "Environment",
        "Contract Name",
        "Contract Start Date",
        "Contract End Date",
        "Contact Person",
        npi,
        "Federal Tax Number",
        "Is Private Pay",
        "Is Medic Aid",
        "ICD Code",
        "ICD Code Effective Date",
        "Is Active",
        "Address Type",
        "Address Line 1", 
        "Address Line 2",
        "Address County",
        "Address City",
        "Address State",
        "Address ZIP",
        "Address Latitude",
        "Address Longitude",
        "Phone Number 1",
        "Phone Description 1",
        "Phone Number 2",
        "Phone Description 2",
        "Contract Notes",
        "Office List",
        "Authorization Required",
        "Contract Type Name",
        "Unique Contract Id",
        "Updated Datatimestamp",
        "Usage Tags"

    from core.evv_contract
    where "Is Deleted" = false

)

select *, 'hha' as "Source System" from hha_contract
union all 
select *, 'evv' as "Source System" from evv_contract
  )
/* {"app": "dbt", "dbt_version": "2025.11.4+d8fa9da", "profile_name": "user", "target_name": "prod", "node_id": "model.hha.DimContract"} */;


-- ANALYTICS.BI.DIMOFFICE source

create or replace view ANALYTICS.BI.DIMOFFICE(
	"Office Id" COMMENT 'Office Primary Key',
	"Application Office Id",
	"Environment",
	"Office Name" COMMENT 'Name',
	"Office Code" COMMENT 'Code',
	"Is Active" COMMENT 'Is Active',
	NPI COMMENT 'National Provider Identifier (NPI) **[Read more](https://www.cms.gov/Regulations-and-Guidance/Administrative-Simplification/NationalProvIdentStand)**',
	"Federal Tax Number" COMMENT 'Otherwise known as Employer Identification Number (EIN) **[Read more]( https://www.irs.gov/businesses/small-businesses-self-employed/employer-id-numbers/)**',
	"Address Type" COMMENT 'Address Type',
	"Address Primary" COMMENT 'Primary Address',
	"Address Line 1" COMMENT 'Address Line 1',
	"Address Line 2" COMMENT 'Address Line 2',
	"Address County" COMMENT 'Address County',
	"Address City" COMMENT 'Address City',
	"Address ZIP" COMMENT 'Address ZIP Code',
	"Address State" COMMENT 'Address State',
	"Address Latitude" COMMENT 'Address Latitude',
	"Address Longitude" COMMENT 'Address Longitude',
	"Phone Number 1" COMMENT 'Phone Number 1',
	"Phone Number 1 Description" COMMENT 'Phone Number 1 Description',
	"Phone Number 2" COMMENT 'Phone Number 2',
	"Phone Number 2 Description" COMMENT 'Phone Number 2 Description',
	"Timezone" COMMENT 'Time Zone',
	"Provider Id",
	"Application Provider Id",
	"Unique Office Id",
	"Updated Datatimestamp",
	"Usage Tags" COMMENT 'Tags for system purposes',
	"Source System"
) WITH ROW ACCESS POLICY ANALYTICS.CORE.DATA_RESTRICT_POLICY ON ("Usage Tags")
 COMMENT='Dimension representing Offices, with a row per unique Office. Primary Key - \"Office Id\"'
 as (
    

with hha_office as
(
    select

			GLOBAL_OFFICE_ID          as "Office Id",
            ENVIRONMENT_OFFICE_ID     as "Application Office Id",
            environment               as "Environment",
			NAME                      as "Office Name",
			CODE                      as "Office Code",
			IS_ACTIVE                 as "Is Active",
			NPI                       as "NPI",
			TAX_ID                    as "Federal Tax Number",
			ADDRESS_TYPE              as "Address Type",
			ADDRESS_PRIMARY           as "Address Primary",
			ADDRESS_LINE1             as "Address Line 1",
			ADDRESS_LINE2             as "Address Line 2",
			ADDRESS_COUNTY            as "Address County",
			ADDRESS_CITY              as "Address City",
			ADDRESS_ZIP               as "Address ZIP",
			ADDRESS_STATE             as "Address State",
			ADDRESS_LATITUDE          as "Address Latitude",
			ADDRESS_LONGITUDE         as "Address Longitude",
			PHONE_NUMBER1             as "Phone Number 1",
			PHONE_NUMBER_DESCRIPTION1 as "Phone Number 1 Description",
			PHONE_NUMBER2             as "Phone Number 2",
			PHONE_NUMBER_DESCRIPTION2 as "Phone Number 2 Description",
			TIMEZONE                  as "Timezone",
            global_provider_id        as "Provider Id",
            ENVIRONMENT_PROVIDER_ID   as "Application Provider Id",
            unique_office_id          as "Unique Office Id",
            updated_data_timestamp    as "Updated Datatimestamp",
            usage_tags                as "Usage Tags"
			
    from core.office

),

evv_office as 

(
    Select 
    
			"Office Id",
            "Application Office Id",
            "Environment",
            "Office Name",
            "Office Code",
            "Is Active",
            "NPI",
            "Federal Tax Number",
            "Address Type",
            "Address Primary",
            "Address Line 1",
            "Address Line 2",
            "Address County",
            "Address City",
            "Address ZIP",
            "Address State",
            "Address Latitude",
            "Address Longitude",
            "Phone Number 1",
            "Phone Number 1 Description",
            "Phone Number 2",
            "Phone Number 2 Description",
            "Timezone",
            "Provider Id",
            "Application Provider Id",
            "Unique Office Id",
            "Updated Datatimestamp",
            "Usage Tags"

    from core.evv_office

)

select
    *, 'hha' as "Source System"
from hha_office

union all

select
    *, 'evv' as "Source System"
from evv_office
  )
/* {"app": "dbt", "dbt_version": "2025.11.4+d8fa9da", "profile_name": "user", "target_name": "prod", "node_id": "model.hha.DimOffice"} */;


-- ANALYTICS.BI.DIMPATIENT source

create or replace view ANALYTICS.BI.DIMPATIENT(
	"Patient Id" COMMENT 'Patient Primary Key',
	"Application Patient Id",
	"Application Patient ID",
	"Environment",
	"Patient Name" COMMENT 'Name',
	"Patient Firstname",
	"Patient Middlename",
	"Patient Lastname",
	"Medical Number" COMMENT 'Medical Number',
	"Medicaid Number",
	"Date of Birth" COMMENT 'Date of Birth',
	"Status" COMMENT 'Status',
	"Gender" COMMENT 'Gender',
	"Payer Priority Code" COMMENT 'Payer Priority Code',
	"Payer TAL" COMMENT 'Payer TAL',
	"Has Visit" COMMENT 'Does patient have any visits',
	"Is Authorized" COMMENT 'Does Patient have any authorizations',
	"Admission Id" COMMENT 'Admission Id',
	"Is Authorization Elapsing" COMMENT 'Is Authorization Elapsing',
	"Is Payer Created Patient" COMMENT 'Did Payer Create Patient',
	"Payer Id" COMMENT 'Payer Id. Foreign key (dimPayer.\"Payer Id\")',
	"Application Payer Id",
	"Provider Id" COMMENT 'Provider Id. Foreign key (dimProvider.\"Provider Id\")',
	"Application Provider Id",
	"Office Id",
	"Application Office Id",
	"Patient Team",
	"Member Team Name",
	"Patient Branch",
	"Patient Location",
	"Unique Patient Id",
	"Updated Datatimestamp",
	"Usage Tags" COMMENT 'Tags for system purposes',
	"Primary Language",
	"Secondary Language",
	"Source System"
) WITH ROW ACCESS POLICY ANALYTICS.CORE.DATA_RESTRICT_POLICY ON ("Usage Tags")
 COMMENT='Dimension representing Patients, with a row per unique Patients. Primary Key - \"Patient Id\"'
 as (
    

with hha_patient as
(
    select

			GLOBAL_PATIENT_ID           as "Patient Id",
            environment_patient_id      as "Application Patient Id",
            environment_patient_id      as "Application Patient ID",
            environment                 as "Environment",
			PATIENT_NAME                as "Patient Name",
			FIRST_NAME                  as "Patient Firstname",
			MIDDLE_NAME                 as "Patient Middlename",
			LAST_NAME                   as "Patient Lastname",
			MR_NUMBER                   as "Medical Number",
			MEDICAID_NUMBER             as "Medicaid Number",
			DATE_OF_BIRTH               as "Date of Birth",
			STATUS                      as "Status",
			GENDER                      as "Gender",
			PAYER_PRIORITY_CODE         as "Payer Priority Code",
			PAYER_TAL                   as "Payer TAL",
			HAS_VISIT                   as "Has Visit",
			IS_AUTHORIZED               as "Is Authorized",
			ADMISSION_ID                as "Admission Id",
			false                       as "Is Authorization Elapsing",
			IS_PAYER                    as "Is Payer Created Patient", 
			global_payer_id	            as "Payer Id",
            environment_payer_id        as "Application Payer Id",
			global_provider_id          as "Provider Id",
            ENVIRONMENT_PROVIDER_ID     as "Application Provider Id",
            global_office_id            as "Office Id",
            ENVIRONMENT_OFFICE_ID       as "Application Office Id",
			team_name                   as "Patient Team",
            member_team_name            as "Member Team Name",
			branch_name                 as "Patient Branch",
			location                    as "Patient Location",
            unique_patient_id           as "Unique Patient Id",
            updated_data_timestamp      as "Updated Datatimestamp",
            usage_tags                  as "Usage Tags",
            primary_language            as "Primary Language",
            secondary_language          as "Secondary Language"

    from core.patient

),

evv_patient as
(
    select

			"Patient Id",
            "Application Patient Id",
            "Application Patient ID",
            "Environment",
			"Patient Name",
			"Patient Firstname",
			"Patient Middlename",
			"Patient Lastname",
			"Medical Number",
			"Medicaid Number",
			"Date of Birth",
			"Status",
			"Gender",
			"Payer Priority Code",
			"Payer TAL",
			"Has Visit",
			"Is Authorized",
			"Admission Id",
			"Is Authorization Elapsing",
			"Is Payer Created Patient", 
			"Payer Id",
            "Application Payer Id",
			"Provider Id",
            "Application Provider Id",
            "Office Id",
            "Application Office Id",
			"Patient Team",
            "Member Team Name",
			"Patient Branch",
			"Patient Location",
            "Unique Patient Id",
            "Updated Datatimestamp",
            "Usage Tags",
            "Primary Language",
            "Secondary Language"

    from core.evv_patient
    where "Is Deleted" = false

)


select
    *, 'hha' as "Source System"
from hha_patient

union all

select
    *, 'evv' as "Source System"
from evv_patient
  )
/* {"app": "dbt", "dbt_version": "2025.11.4+d8fa9da", "profile_name": "user", "target_name": "prod", "node_id": "model.hha.DimPatient"} */;


-- ANALYTICS.BI.DIMPATIENTADDRESS source

create or replace view ANALYTICS.BI.DIMPATIENTADDRESS(
	"Patient Address Id" COMMENT 'Patient Address Primary Key.',
	"Application Patient Address Id" COMMENT 'Source system identifier for the patient address record.',
	"Environment" COMMENT 'Indicates the originating data environment, e.g., PROD-Sandata:EVV or HHA-Prod.',
	"Updated Timestamp" COMMENT 'Timestamp of the most recent update to the patient address record from either HHA or EVV source.',
	"Application Created UTC Timestamp" COMMENT 'Timestamp when the patient address record was first created in its source application.',
	"Application Updated UTC Timestamp" COMMENT 'Timestamp when the patient address record was last updated in its source application.',
	"Address Type" COMMENT 'Type of address, such as HOME, WORK, or OTHER, as provided by the source system.',
	"Address Line 1" COMMENT 'First line of the address (street number and name).',
	"Address Line 2" COMMENT 'Second line of the address, e.g., apartment, suite, or unit.',
	"City" COMMENT 'City name of the patient address.',
	"Address State" COMMENT 'State or region code of the patient address.',
	"Zip Code" COMMENT 'ZIP or postal code associated with the patient address.',
	"County" COMMENT 'County name associated with the address.',
	"Address Cross Street" COMMENT 'Cross street or intersection information, when available.',
	"Primary Address" COMMENT 'Boolean flag indicating whether this is the patient’s primary address on record.',
	"Latitude" COMMENT 'Latitude coordinate of the address.',
	"Longitude" COMMENT 'Longitude coordinate of the address.',
	"Address Notes" COMMENT 'Free-text notes or comments associated with the address record.',
	"Is Using Google API" COMMENT 'Indicates if the address coordinates were derived or validated using Google API services.',
	"Payer Id" COMMENT 'Payer identifier associated with the patient address.  Foreign Key to DimPayer.',
	"Application Payer Id" COMMENT 'Original payer identifier from the source system.',
	"Provider Id" COMMENT 'Provider identifier associated with the patient address. Foreign key to DimProvider.',
	"Application Provider Id" COMMENT 'Original provider identifier from the source application.',
	"Patient Id" COMMENT 'Patient Primary Key.  Foreign key to DimPatient.',
	"Application Patient Id" COMMENT 'Original patient identifier from the source system.',
	"Usage Tags" COMMENT 'Tags for system purposes',
	"Source System"
) WITH ROW ACCESS POLICY ANALYTICS.CORE.DATA_RESTRICT_POLICY ON ("Usage Tags")
 COMMENT='Dimension representing Patient Addresses, with one row per unique patient address record.'
 as (
    

WITH hha_patient_address as
(
    SELECT
        global_patient_address_id        as "Patient Address Id",
        environment_patient_address_id   as "Application Patient Address Id",
        environment                      as "Environment",
        updated_data_timestamp           as "Updated Timestamp",
        address_created_timestamp        as "Application Created UTC Timestamp",
        address_updated_timestamp        as "Application Updated UTC Timestamp",
        ADDRESS_TYPES                    as "Address Type",
        ADDRESS_LINE1                    as "Address Line 1",
        ADDRESS_LINE2                    as "Address Line 2",
        ADDRESS_CITY                     as "City",
        address_state                    as "Address State",
        ADDRESS_ZIP                      as "Zip Code",
        ADDRESS_COUNTY                   as "County",
        address_cross_street             as "Address Cross Street",
        ADDRESS_IS_PRIMARY               as "Primary Address",
        address_latitude                 as "Latitude",
        address_longitude                as "Longitude",
        address_notes                    as "Address Notes",
        is_using_google_api              as "Is Using Google API",
        global_payer_id                  as "Payer Id",
        environment_payer_id             as "Application Payer Id",
        global_provider_id               as "Provider Id",
        environment_provider_id          as "Application Provider Id",
        GLOBAL_PATIENT_ID                as "Patient Id",
        environment_patient_id           as "Application Patient Id",
--        is_deleted                       as "Is Deleted",
        usage_tags                       as "Usage Tags"

    FROM core.patient_address

    WHERE IS_DELETED = false
),

evv_patient_address as
(
    SELECT
        "Patient Address Id",
        "Application Patient Address Id",
        "Environment",
        "Updated Timestamp",
        "Application Created UTC Timestamp",
        "Application Updated UTC Timestamp",
        "Address Type",
        "Address Line 1",
        "Address Line 2",
        "City",
        "Address State",
        "Zip Code",
        "County",
        "Address Cross Street",
        "Primary Address",
        "Latitude",
        "Longitude",
        "Address Notes",
        "Is Using Google API",
        "Payer Id",
        "Application Payer Id",
        "Provider Id",
        "Application Provider Id",
        "Patient Id",
        "Application Patient Id",
        "Usage Tags"
    FROM core.evv_patient_address

    WHERE "Is Deleted" = false

)

SELECT
    *, 'hha' as "Source System"
FROM hha_patient_address

UNION all

SELECT
    *, 'evv' as "Source System"
FROM evv_patient_address
  )
/* {"app": "dbt", "dbt_version": "2025.11.4+d8fa9da", "profile_name": "user", "target_name": "prod", "node_id": "model.hha.DimPatientAddress"} */;


-- ANALYTICS.BI.DIMPAYER source

create or replace view ANALYTICS.BI.DIMPAYER(
	"Payer Id" COMMENT 'Payer Primary Key',
	"Payer Name" COMMENT 'Tags or system purposesf',
	"Payer Initials" COMMENT 'Initials',
	"Is Active" COMMENT 'Is Active',
	"Is Demo" COMMENT 'Is Demo',
	"Payer State" COMMENT 'Payer State',
	"Unbilling Process" COMMENT 'Unbilling Process',
	"Initiative Names" COMMENT 'Initiative Names',
	"Application Payer Id",
	"Environment",
	"Created Date Time",
	"Unique Payer Id",
	"Updated Datatimestamp",
	"Usage Tags" COMMENT 'Tags for system purposes',
	"Source System"
) WITH ROW ACCESS POLICY ANALYTICS.CORE.DATA_RESTRICT_POLICY ON ("Usage Tags")
 COMMENT='Dimension representing Payers, with a row per unique Payer. Primary Key - \"Payer Id\"'
 as (
    

with hha_payer as
(
    select
            GLOBAL_PAYER_ID              as "Payer Id",
            NAME                         as "Payer Name",
            INITIALS                     as "Payer Initials",
            IS_ACTIVE                    as "Is Active",
            IS_DEMO                      as "Is Demo",
            STATE                        as "Payer State",
            UNBILLING_PROCESS            as "Unbilling Process",
            INITIATIVE_NAMES             as "Initiative Names",
            ENVIRONMENT_PAYER_ID         as "Application Payer Id",
            environment                  as "Environment",
            CREATED_DATETIME             as "Created Date Time",
            unique_payer_id              as "Unique Payer Id",
            updated_data_timestamp       as "Updated Datatimestamp",
            usage_tags                   as "Usage Tags"

    from core.payer
)
, evv_payer as 
(
    select 
            "Payer Id",
            "Payer Name",
            "Payer Initials",
            "Is Active",
            "Is Demo",
            "Payer State",
            "Unbilling Process",
            "Initiative Names",
            "Application Payer Id",
            "Environment",
            "Created Date Time",
            "Unique Payer Id",
            "Updated Datatimestamp",
            "Usage Tags"

    from core.evv_payer
)

select
    *, 'hha' as "Source System"
from hha_payer

union all

select
    *, 'evv' as "Source System"
from evv_payer
  )
/* {"app": "dbt", "dbt_version": "2025.11.4+d8fa9da", "profile_name": "user", "target_name": "prod", "node_id": "model.hha.DimPayer"} */;


-- ANALYTICS.BI.DIMPAYERPROVIDER source

create or replace view ANALYTICS.BI.DIMPAYERPROVIDER(
	"Payer Id",
	"Application Payer Id",
	"Contract Id",
	"Application Contract Id",
	"Provider Id",
	"Application Provider Id",
	"Environment",
	"Not Rounding Visit Time",
	"Chha Vendor Id",
	"Bill Unbalanced",
	"Bill Missed Visit",
	"Bill Temp Aide",
	"Bill Non Aide Compliant",
	"Bill Non POC Compliant",
	"Bill Overlapping Patient",
	"Bill Overlapping Aide",
	"Bill Restricted Aide",
	"Bill Timesheet Not Approved",
	"Bill Insufficient Duty Minutes",
	"Bill Missing Clinical Document",
	"Bill Non Custom Validation Compliance",
	"Bill Non Mediciand Compliance",
	"Bill Open Event",
	"Bill Over 24 Hours",
	"Bill Unauthorized",
	"Usage Tags"
) WITH ROW ACCESS POLICY ANALYTICS.CORE.DATA_RESTRICT_POLICY ON ("Usage Tags")
 as (
    

with payer_provider as
(
    select
        GLOBAL_PAYER_ID AS "Payer Id"
        ,environment_payer_id as "Application Payer Id"
        ,GLOBAL_CONTRACT_ID AS "Contract Id"
        ,environment_contract_id as "Application Contract Id"
        ,GLOBAL_PROVIDER_ID AS "Provider Id"
        ,environment_provider_id as "Application Provider Id"
        ,ENVIRONMENT AS "Environment"
        ,NOT_ROUNDING_VISIT_TIME AS "Not Rounding Visit Time"
        ,
        chhavendorid                                   as "Chha Vendor Id",
        billunbalanced                                 as "Bill Unbalanced",
        billmissedvisit                                as "Bill Missed Visit",
        billtempaide                                   as "Bill Temp Aide",
        billnonaidecompliant                           as "Bill Non Aide Compliant",
        billnonpoccompliant                            as "Bill Non POC Compliant",
        billoverlappingpatient                         as "Bill Overlapping Patient",
        billoverlappingaide                            as "Bill Overlapping Aide",
        billrestrictedaide                             as "Bill Restricted Aide",
        billtimesheetnotapproved                       as "Bill Timesheet Not Approved",
        billinsufficientdutyminutes                    as "Bill Insufficient Duty Minutes",
        billmissingclinicaldocument                    as "Bill Missing Clinical Document",
        billnoncustomvalidationcompliance              as "Bill Non Custom Validation Compliance",
        billnonmedicaidcompliance                      as "Bill Non Mediciand Compliance",
        billopenevent                                  as "Bill Open Event",
        billover24hours                                as "Bill Over 24 Hours",
        billunauthorized                               as "Bill Unauthorized"

        ,usage_tags as "Usage Tags"
    from core.payer_provider
)
select * from payer_provider
  )
/* {"app": "dbt", "dbt_version": "2025.11.4+d8fa9da", "profile_name": "user", "target_name": "prod", "node_id": "model.hha.DimPayerProvider"} */;


-- ANALYTICS.BI.DIMPROVIDER source

create or replace view ANALYTICS.BI.DIMPROVIDER(
	"Provider Id" COMMENT 'Provider Primary Key',
	"Application Provider Id",
	"Environment",
	"Environemnt",
	"Provider Name" COMMENT 'Name',
	"Provider Initial" COMMENT 'Initial',
	"Provider Code" COMMENT 'Code',
	"Live Date" COMMENT 'Live Date',
	NPI COMMENT 'National Provider Identifier (NPI) **[Read more](https://www.cms.gov/Regulations-and-Guidance/Administrative-Simplification/NationalProvIdentStand)**',
	"Federal Tax Number" COMMENT 'Otherwise known as Employer Identification Number (EIN) **[Read more]( https://www.irs.gov/businesses/small-businesses-self-employed/employer-id-numbers/)**',
	"MPI Number" COMMENT 'MPI Number',
	"Address Type" COMMENT 'Address Type',
	"Primary Address" COMMENT 'Is Primary Address',
	"Address Line 1" COMMENT 'Address Line 1',
	"Address Line 2" COMMENT 'Address Line 2',
	"Address County" COMMENT 'Address County',
	"Address City" COMMENT 'Address City',
	"Address State" COMMENT 'Address State',
	"Address ZIP" COMMENT 'Address ZIP Code',
	"Address Latitude" COMMENT 'Address Latitude',
	"Address Longitude" COMMENT 'Address Longitude',
	"Phone Number 1" COMMENT 'Phone Number 1',
	"Phone Description 1" COMMENT 'Phone Description 1',
	"Phone Number 2" COMMENT 'Phone Number 2',
	"Phone Description 2" COMMENT 'Phone Description 2\"',
	"Phone Number 3" COMMENT 'Phone Number 3',
	"Phone Description 3" COMMENT 'Phone Description 3',
	"Product Type" COMMENT 'Product Type',
	"Is Active" COMMENT 'Is Active',
	"Is Demo" COMMENT 'Is Demo',
	"Has Confirmed Visit" COMMENT 'Has Confirmed Visit',
	"Entry Type" COMMENT 'Entry Type',
	"Region Type",
	"General Type",
	"Platform Type",
	"HHAX Unique Id" COMMENT 'HHAX Unique Id',
	"Provider Version" COMMENT 'Version',
	"Provider Minor Version" COMMENT 'Minor Version',
	"App Version Id" COMMENT 'App Version Id',
	"Conflict Start Date",
	"Read Only",
	"Using Conflict Report" COMMENT 'Using Conflict Report',
	"Display Time Format",
	"Provider Time Zone",
	"Is Third Party Provider" COMMENT 'Indicates whether this is a third party provider or not',
	"Unique Provider Id",
	"Updated Datatimestamp",
	"Usage Tags" COMMENT 'Tags for system purposes',
	"Source System"
) WITH ROW ACCESS POLICY ANALYTICS.CORE.DATA_RESTRICT_POLICY ON ("Usage Tags")
 COMMENT='Dimension representing Providers, with a row per unique Provider. Primary Key - \"Provider Id\"'
 as (
    

with hha_provider as
(
    select

			GLOBAL_PROVIDER_ID        as "Provider Id",
            environment_provider_id   as "Application Provider Id",
            environment               as "Environment",
            environment               as "Environemnt",
			NAME                      as "Provider Name",
			INITIAL                   as "Provider Initial",
			CODE                      as "Provider Code",
			LIVE_DATE                 as "Live Date",
			NPI                       as "NPI",
			TAX_ID                    as "Federal Tax Number",
			MPI_NUMBER                as "MPI Number",
			ADDRESS_TYPE              as "Address Type",
			ADDRESS_PRIMARY           as "Primary Address",
			ADDRESS_LINE1             as "Address Line 1",
			ADDRESS_LINE2             as "Address Line 2",
			ADDRESS_COUNTY            as "Address County",
			ADDRESS_CITY              as "Address City",
			ADDRESS_STATE             as "Address State",
			ADDRESS_ZIP               as "Address ZIP",
			ADDRESS_LATITUDE          as "Address Latitude",
			ADDRESS_LONGITUDE         as "Address Longitude",
			PHONE_NUMBER1             as "Phone Number 1",
			PHONE_NUMBER_DESCRIPTION1 as "Phone Description 1",
			PHONE_NUMBER2             as "Phone Number 2",
			PHONE_NUMBER_DESCRIPTION2 as "Phone Description 2",
			PHONE_NUMBER3             as "Phone Number 3",
			PHONE_NUMBER_DESCRIPTION3 as "Phone Description 3",
			PRODUCT                   as "Product Type",
			IS_ACTIVE                 as "Is Active",
			IS_DEMO                   as "Is Demo",
			HAS_CONFIRMED_VISIT       as "Has Confirmed Visit",
			ENTRY_TYPE                as "Entry Type",
			REGION_TYPE               as "Region Type",
			GENERAL_TYPE              as "General Type",
			PLATFORM_TYPE             as "Platform Type",
            HHAX_UNIQUE_ID            as "HHAX Unique Id",
            PROVIDER_VERSION          as "Provider Version",                 
            PROVIDER_MINOR_VERSION    as "Provider Minor Version",
            APP_VERSION_ID            as "App Version Id",
            CONFLICT_START_DATE       as "Conflict Start Date",
            READ_ONLY                 as "Read Only",
            USING_CONFLICT_REPORT     as "Using Conflict Report",
            DISPLAY_TIME_FORMAT       as "Display Time Format",
            PROVIDER_TIME_ZONE        as "Provider Time Zone",
            is_third_party_provider   as "Is Third Party Provider",
            unique_provider_id        as "Unique Provider Id",
            updated_data_timestamp    as "Updated Datatimestamp",
            USAGE_TAGS                as "Usage Tags"          

    from core.provider

),

evv_provider as 
(

    Select 
            "Provider Id",
            "Application Provider Id",
            "Environment",
            "Environemnt",
            "Provider Name",
            "Provider Initial",
            "Provider Code",
            "Live Date",
            "NPI",
            "Federal Tax Number",
            "MPI Number",
            "Address Type",
            "Primary Address",
            "Address Line 1",
            "Address Line 2",
            "Address County",
            "Address City",
            "Address State",
            "Address ZIP",
            "Address Latitude",
            "Address Longitude",
            "Phone Number 1",
            "Phone Description 1",
            "Phone Number 2",
            "Phone Description 2",
            "Phone Number 3",
            "Phone Description 3",
            "Product Type",
            "Is Active",
            "Is Demo",
            "Has Confirmed Visit",
            "Entry Type",
            "Region Type",
            "General Type",
            "Platform Type",
            "HHAX Unique Id",
            "version Provider Version",
            "Provider Minor Version",
            "App Version Id",
            "Conflict Start Date",
            "Read Only",
            "Using Conflict Report",
            "Display Time Format",
            "Provider Time Zone",
            "Is Third Party Provider",
            "Unique Provider Id",
            "Updated Datatimestamp",
            "Usage Tags"

    from core.evv_provider

)

select
    *, 'hha' as "Source System"
from hha_provider

union all

select
    *, 'evv' as "Source System"
from evv_provider
  )
/* {"app": "dbt", "dbt_version": "2025.11.4+d8fa9da", "profile_name": "user", "target_name": "prod", "node_id": "model.hha.DimProvider"} */;


-- ANALYTICS.BI.DIMSERVICECODE source

create or replace view ANALYTICS.BI.DIMSERVICECODE(
	"Service Code Id" COMMENT 'Sevice Code Primary Key',
	"Application Service Code Id",
	"Environment",
	"Rate Type" COMMENT 'Rate Type',
	"Service Code" COMMENT 'Service Code',
	"Export Code" COMMENT 'Export Code',
	"Weekday Export Code" COMMENT 'Weekday Export Code',
	"Weekend Export Code" COMMENT 'Weekend Export Code',
	"Service Type" COMMENT 'Service Type',
	"Service Category" COMMENT 'Service Category',
	"Procedure Code" COMMENT 'Procedure Code',
	"Modifier 1" COMMENT 'Modifier 1',
	"Modifier 2" COMMENT 'Modifier 2',
	"Modifier 3" COMMENT 'Modifier 3',
	"Modifier 4" COMMENT 'Modifier 4',
	"Modifier Description" COMMENT 'Modifier Description',
	"Bypass Prebilling Validation" COMMENT 'Bypass Prebilling Validation',
	"Bypass Billing Review Validation" COMMENT 'Bypass Billing Review Validation',
	"PA Mandated" COMMENT 'PA Mandated',
	"Vendor Type" COMMENT 'Vendor Type - Payer or Provider',
	"Used Export Code",
	"Vendor Id" COMMENT 'Vendor Id. Payer Id or Provider Id depending on Vendor Type value. Foreign key (dimPayer.\"Payer Id\" or dimProvider.\"Provider Id\")',
	"Application Vendor Id",
	"Provider Id",
	"Application Provider Id",
	"Payer Id",
	"Application Payer Id",
	"Office Id",
	"Application Office Id",
	"Contract Id",
	"Application Contract Id",
	"Discipline",
	"Discipline Id",
	"Discipline Type",
	"No Auth Required for Billing Flag",
	"Unique Service Code Id",
	"Updated Datatimestamp",
	"Is Active",
	"Usage Tags" COMMENT 'Tags for system purposes',
	"Source System"
) WITH ROW ACCESS POLICY ANALYTICS.CORE.DATA_RESTRICT_POLICY ON ("Usage Tags")
 COMMENT='Dimension representing Service Codes, with a row per unique Service Code. Primary Key - \"Service Code Id\"'
 as (
    

with hha_service_code as
(
    select
	
			global_service_code_id				as "Service Code Id",
            environment_service_code_id         as "Application Service Code Id",
            environment                         as "Environment",
			rate_type							as "Rate Type",
			service_code						as "Service Code",
			export_code							as "Export Code",
			weekday_export_code					as "Weekday Export Code",
			weekend_export_code					as "Weekend Export Code",
			service_type						as "Service Type",
			service_category					as "Service Category",
			procedure_code						as "Procedure Code",
			modifier1							as "Modifier 1",
			modifier2							as "Modifier 2",
			modifier3							as "Modifier 3",
			modifier4							as "Modifier 4",
			modifier_description				as "Modifier Description",
			bypass_prebilling_validation		as "Bypass Prebilling Validation",
			bypass_billing_review_validation	as "Bypass Billing Review Validation",
			pa_mandated							as "PA Mandated",
			vendor_type							as "Vendor Type",
            used_export_code                    as "Used Export Code",
			case
				when vendor_type = 'Payer'
					then global_payer_id
				when vendor_type = 'Provider'
					then global_provider_id
				else null end ::varchar(50)		as "Vendor Id",
			case
				when vendor_type = 'Payer'
					then environment_payer_id
				when vendor_type = 'Provider'
					then environment_provider_id
				else null end ::varchar(50)		as "Application Vendor Id",
            global_provider_id                  as "Provider Id",
            environment_provider_id             as "Application Provider Id",
            global_payer_id                     as "Payer Id",
            environment_payer_id                as "Application Payer Id",
            global_office_id                    as "Office Id",
            environment_office_id               as "Application Office Id",
            global_contract_id                  as "Contract Id",
            environment_contract_id             as "Application Contract Id",         
            discipline                          as "Discipline",
            discipline_id                       as "Discipline Id",
            discipline_type                     as "Discipline Type",
            no_auth_required_for_billing        as "No Auth Required for Billing Flag",
            unique_service_code_id              as "Unique Service Code Id",
            updated_data_timestamp              as "Updated Datatimestamp",
            is_active                           as "Is Active",
            usage_tags                          as "Usage Tags"

    from core.service_code

	where is_deleted = false
),

evv_service_code as (
    select
            "Service Code Id",
            "Application Service Code Id",
            "Environment",
            "Rate Type",
            "Service Code",
            "Export Code",
            "Weekday Export Code",
            "Weekend Export Code",
            "Service Type",
            "Service Category",
            "Procedure Code",
            "Modifier 1",
            "Modifier 2",
            "Modifier 3",
            "Modifier 4",
            "Modifier Description",
            "Bypass Prebilling Validation",
            "Bypass Billing Review Validation",
            "PA Mandated",
            "Vendor Type",
            "Used Export Code",
            "Vendor Id",
            "Application Vendor Id",
            "Provider Id",
            "Application Provider Id",
            "Payer Id",
            "Application Payer Id",
            "Office Id",
            "Application Office Id",
            "Contract Id",
            "Application Contract Id",
            "Discipline",
            "Discipline Id",
            "Discipline Type",
            "No Auth Required for Billing Flag",
            "Unique Service Code Id",
            "Updated Datatimestamp",
            "Is Active",
            "Usage Tags"
    from core.evv_service_code
)
select
    *, 'hha' as "Source System"
from hha_service_code

union all

select
    *, 'evv' as "Source System"
from evv_service_code
  )
/* {"app": "dbt", "dbt_version": "2025.11.4+d8fa9da", "profile_name": "user", "target_name": "prod", "node_id": "model.hha.DimServiceCode"} */;


-- ANALYTICS.BI.DIMUSER source

create or replace view ANALYTICS.BI.DIMUSER(
	"User Id" COMMENT 'User Primary Key',
	"Username" COMMENT 'Derived name for User to make it unique. This is to enable external users access to Tableau dashboards and ultimately filter rows as part of the Tableau row-level security implementation',
	"User Fullname" COMMENT 'Full Name (calculated->\"User Firstname\"+\"User Lastname\")',
	"User Firstname" COMMENT 'First Name',
	"User Lastname" COMMENT 'Last Name',
	"User Email Address" COMMENT 'Email Address',
	"Is Support User" COMMENT 'Is Support User',
	"Vendor Type" COMMENT 'Vendor Type - Payer or Provider',
	"Vendor Id" COMMENT 'Vendor Id. Payer Id or Provider Id depending on Vendor Type value. Foreign key (dimPayer.\"Payer Id\" or dimProvider.\"Provider Id\")',
	"Application Vendor Id",
	"Application User Id" COMMENT 'Primary Key in Source mssql DB. Note, due to loading data from multiple environments this field can''t be used for filters and joins. Use \"User Id\" field to correctly identify the record',
	"Environment",
	"Aggregator Database Name" COMMENT 'aggregator database name to which the aggregator usernames belong',
	"Usage Tags" COMMENT 'Tags for system purposes',
	"Source System"
) WITH ROW ACCESS POLICY ANALYTICS.CORE.DATA_RESTRICT_POLICY ON ("Usage Tags")
 COMMENT='Dimension representing Users, with a row per unique User. Primary Key - \"User Id\"'
 as (
    

with hha_user as
(
    select
        global_user_id                        as "User Id",
        name                                  as "Username",
        full_name                             as "User Fullname",
        first_name                            as "User Firstname",
        last_name                             as "User Lastname",
        email_address                         as "User Email Address",
        is_support                            as "Is Support User",
        vendor_type                           as "Vendor Type",
        global_vendor_id                      as "Vendor Id",
        environment_vendor_id                 as "Application Vendor Id",
        environment_user_id::int::varchar(50) as "Application User Id",
        environment                           as "Environment",
        aggregator_database_name              as "Aggregator Database Name",
        usage_tags                            as "Usage Tags"

    from core.user

    where is_deleted = false
)
,
evv_user as
(
    select
        global_user_id                        as "User Id",
        name                                  as "Username",
        full_name                             as "User Fullname",
        first_name                            as "User Firstname",
        last_name                             as "User Lastname",
        email_address                         as "User Email Address",
        null                                  as "Is Support User",
        'Vendor'                              as "Vendor Type",
        global_vendor_id                      as "Vendor Id",
        environment_vendor_id                 as "Application Vendor Id",
        environment_user_id::int::varchar(50) as "Application User Id",
        environment                           as "Environment",
        null                                  as "Aggregator Database Name",
        usage_tags                            as "Usage Tags"

    from core.evv_user

    where is_deleted = false
)


select *, 'hha' as "Source System"  from hha_user
union all
select *, 'evv' as "Source System"  from evv_user
  )
/* {"app": "dbt", "dbt_version": "2025.11.4+d8fa9da", "profile_name": "user", "target_name": "prod", "node_id": "model.hha.DimUser"} */;


-- ANALYTICS.BI.DIMUSEROFFICES source

create or replace view ANALYTICS.BI.DIMUSEROFFICES(
	"User Id",
	"Application User Id",
	"Environment",
	"Vendor Type",
	"Vendor Id",
	"Application Vendor Id",
	"Office Id",
	"Application Office Id",
	"Usage Tags",
	"Source System"
) WITH ROW ACCESS POLICY ANALYTICS.CORE.DATA_RESTRICT_POLICY ON ("Usage Tags")
 as (
    

with hha_user_offices as
(
    select
        	global_user_id 			              as "User Id",
            environment_user_id::int::varchar(50) as "Application User Id",
            environment                           as "Environment",
        	vendor_type				              as "Vendor Type",
			global_vendor_id		              as "Vendor Id",
            environment_vendor_id                 as "Application Vendor Id",
            global_office_id                      as "Office Id",
            environment_office_id                 as "Application Office Id",
            usage_tags                            as "Usage Tags"

    from core.user_offices
)
,
evv_user_offices as
(
    select
        	global_user_id 			              as "User Id",
            environment_user_id::int::varchar(50) as "Application User Id",
            environment                           as "Environment",
        	'Vendor'				              as "Vendor Type",
			global_vendor_id		              as "Vendor Id",
            environment_vendor_id                 as "Application Vendor Id",
            global_vendor_id                      as "Office Id",
            environment_vendor_id                 as "Application Office Id",
            usage_tags                            as "Usage Tags"

    from core.evv_user
    where environment_vendor_id is not null
      and is_deleted = false
)

select *, 'hha' as "Source System"  from hha_user_offices
union all
select *, 'evv' as "Source System"  from evv_user_offices
  )
/* {"app": "dbt", "dbt_version": "2025.11.4+d8fa9da", "profile_name": "user", "target_name": "prod", "node_id": "model.hha.DimUserOffices"} */;


-- ANALYTICS.BI.FACTCAREGIVERABSENCE source

create or replace view ANALYTICS.BI.FACTCAREGIVERABSENCE(
	"Environment",
	"Caregiver Vacation Id",
	"Application Caregiver Id",
	"Global Caregiver Id",
	"Start Date",
	"End Date",
	"Duration",
	"Notes",
	"Caregiver Absence Type Id",
	"Absence Type Name",
	"Absence Type Description",
	"Active",
	"Application Service Code Id",
	"Service Code Id",
	"Application Contract Id",
	"Contract Id",
	"Application Provider Id",
	"Provider Id",
	"Application Office Id",
	"Office Id",
	"Created Date",
	"Created By",
	"Updated Date",
	"Updated By",
	"Usage Tags",
	"Source System"
) WITH ROW ACCESS POLICY ANALYTICS.CORE.DATA_RESTRICT_POLICY ON ("Usage Tags")
 as (
     

with hha_caregiver_absence as
(
 select
            environment 				as "Environment",
            caregiver_vacation_id		as "Caregiver Vacation Id",
            environment_caregiver_id    as "Application Caregiver Id",
            global_caregiver_id			as "Global Caregiver Id",
            start_date					as "Start Date",
            end_date					as "End Date",
            duration					as "Duration",
            notes						as "Notes",
            caregiver_absence_type_id	as "Caregiver Absence Type Id",
            absence_type_name			as "Absence Type Name",
            absence_type_description	as "Absence Type Description",       
            active						as "Active",
            environment_service_code_id	as "Application Service Code Id",
            global_service_code_id	    as "Service Code Id",
            environment_contract_id		as "Application Contract Id",
            global_contract_id  		as "Contract Id",
            environment_provider_id		as "Application Provider Id",
            global_provider_id		    as "Provider Id",
            environment_office_id		as "Application Office Id",
            global_office_id		    as "Office Id",
            created_date                as "Created Date",
            created_by                  as "Created By",
            updated_date                as "Updated Date",
            updated_by                  as "Updated By",
            usage_tags                  as "Usage Tags"
    
    from core.caregiver_absence
    where is_deleted = false
)

, evv_caregiver_absence as
(
    select 
	        "Environment",
	        "Caregiver Vacation Id" ,     
	        "Application Caregiver Id",
	        "Global Caregiver Id", 
	        "Start Date",
	        "End Date",
	        "Duration",
	        "Notes",
            "Caregiver Absence Type Id",
            "Absence Type Name",
            "Absence Type Description",
            "Active",
            "Application Service Code Id",
            "Service Code Id",
            "Application Contract Id",
            "Contract Id",
            "Application Provider Id",
            "Provider Id",
            "Application Office Id",
            "Office Id",
            "Created Date",
            "Created By",
            "Updated Date",
            "Updated By",
            "Usage Tags"
    
    from core.evv_caregiver_absence
    where "Is Deleted" = false
)

select *, 'hha' as "Source System" 
from hha_caregiver_absence

union all 

select *, 'evv' as "Source System" 
from evv_caregiver_absence
  )
/* {"app": "dbt", "dbt_version": "2025.11.4+d8fa9da", "profile_name": "user", "target_name": "prod", "node_id": "model.hha.FactCaregiverAbsence"} */;


-- ANALYTICS.BI.FACTCAREGIVERINSERVICE source

create or replace view ANALYTICS.BI.FACTCAREGIVERINSERVICE(
	"Environment" COMMENT 'environment of data',
	"Application Caregiver Inservice Id" COMMENT 'environment specific sql unique key',
	"Application Caregiver Id" COMMENT 'caregiver id',
	"Caregiver Id",
	"Application Inservice Id",
	"Description" COMMENT 'describes inservice activities',
	"Topics" COMMENT 'describes topics in inservice activities',
	"Inservice start date",
	"From time",
	"Inservice end date",
	"End time",
	"Created By",
	"Created Date",
	"Updated By",
	"Updated Date",
	"Allow for Inservice Overlap",
	"Application Provider Id",
	"Provider Id",
	"Application Office Id",
	"Office Id",
	"Usage Tags",
	"Source System"
) WITH ROW ACCESS POLICY ANALYTICS.CORE.DATA_RESTRICT_POLICY ON ("Usage Tags")
 COMMENT='caregiver inservice data'
 as (
    

with hha_caregiver_inservice_data as
(
    select 
	ENVIRONMENT as "Environment" ,
	ENVIRONMENT_AIDE_INSERVICE_ID as "Application Caregiver Inservice Id" ,     
	ENVIRONMENT_CAREGIVER_ID as "Application Caregiver Id" ,
	GLOBAL_CAREGIVER_ID as "Caregiver Id" , 
	ENVIRONMENT_INSERVICE_ID as "Application Inservice Id" ,
	DESCRIPTION as "Description" ,
	TOPICS as "Topics" ,
	INSERVICE_START_DATE as "Inservice start date" ,
	FROM_TIME as "From time" ,
	INSERVICE_END_DATE as "Inservice end date" ,
	END_TIME as "End time" ,
	CREATED_BY as "Created By" ,
	CREATED_DATE as "Created Date" ,
	UPDATED_BY as "Updated By" ,
	UPDATED_DATE as "Updated Date" ,
	ALLOW_FOR_INSERVICE_OVERLAP as "Allow for Inservice Overlap" ,
	ENVIRONMENT_PROVIDER_ID as "Application Provider Id" ,
	GLOBAL_PROVIDER_ID as "Provider Id" ,
	ENVIRONMENT_OFFICE_ID as "Application Office Id",
	GLOBAL_OFFICE_ID as "Office Id" ,
    usage_tags as "Usage Tags"
    
    from core.caregiver_inservice
    where is_deleted = false
)

, evv_caregiver_inservice_data as
(
    select 
	"Environment" ,
	"Application Caregiver Inservice Id" ,     
	"Application Caregiver Id" ,
	"Caregiver Id" , 
	"Application Inservice Id" ,
	"Description" ,
	"Topics" ,
	"Inservice start date" ,
	"From time" ,
	"Inservice end date" ,
	"End time" ,
	"Created By" ,
	"Created Date" ,
	"Updated By" ,
	"Updated Date" ,
	"Allow for Inservice Overlap" ,
	"Application Provider Id" ,
	"Provider Id" ,
	"Application Office Id",
	"Office Id" ,
    "Usage Tags"
    
    from core.evv_caregiver_inservice
    where "Is Deleted" = false
)

select *, 'hha' as "Source System" 
from hha_caregiver_inservice_data

union all 

select *, 'evv' as "Source System" 
from evv_caregiver_inservice_data
  )
/* {"app": "dbt", "dbt_version": "2025.11.4+d8fa9da", "profile_name": "user", "target_name": "prod", "node_id": "model.hha.FactCaregiverInService"} */;


-- ANALYTICS.BI.FACTVISITCALLPERFORMANCE_CR source

create or replace view ANALYTICS.BI.FACTVISITCALLPERFORMANCE_CR(
	"Visit Id",
	"Application Visit Id",
	"External Visit Id",
	"External EVV MS Id",
	"External Source",
	"Visit Date Id",
	"Visit Date",
	"Scheduled Start Time",
	"Scheduled End Time",
	"Confirmed Scheduled Start Time",
	"Confirmed Scheduled End Time",
	"Visit Start Time",
	"Visit End Time",
	"Is Skilled Visit",
	"Is Missed",
	"Edit Notes",
	"Is Confirmed",
	"Discipline",
	"Discipline Type",
	"External Invoice Number",
	"Contract Bill Rate",
	"Scheduled Duration Minutes",
	"Schedule Type",
	"Bill Type",
	"Contract Adjusted Minutes",
	"Visit Source",
	"Visit Time",
	"Visit Time With Time Format",
	"Scheduled Time",
	"Confirmed Scheduled Time",
	"Scheduled Duration Hours",
	"Visit Duration Hours",
	"Is Short Visit",
	"Is Late Start",
	"Is Short Visit (15)",
	"Is Late Start (15)",
	"Short Duration Minutes",
	"Late Duration Minutes",
	"Short Visit Range",
	"Late Duration Range",
	"Visit Duration Range",
	"Contract Type",
	"Contract Usage Type",
	"Approved Travel Time Hours",
	"Coordinator Name",
	"Has Live In Caregiver",
	"Employer Internal Number",
	"Employer Name",
	"Missed Visit Notes",
	"Call In Device Type",
	"Call In Phone Number",
	"Call In Time",
	"Call In Time Minutes",
	"Call In Rounded (15)",
	"Call In GPS Coordinates",
	"Call In Address",
	"Call In Linked Date",
	"Call In Linked By User",
	"Call Out Device Type",
	"Call Out Phone Number",
	"Call Out Time",
	"Call Out Time Minutes",
	"Call Out Rounded (15)",
	"Call Out GPS Coordinates",
	"Call Out Address",
	"Call Out Linked Date",
	"Call Out Linked By User",
	"Payer Id",
	"Application Payer Id",
	"Provider Id",
	"Application Provider Id",
	"Office Id",
	"Application Office Id",
	"Patient Id",
	"Application Patient Id",
	"Caregiver Id",
	"Application Caregiver Id",
	"Service Code Id",
	"Application Service Code Id",
	"Linked Service Code Id",
	"Application Linked Service Code Id",
	"Orig Service Code Id",
	"Application Orig Service Code Id",
	"Contract Id",
	"Application Contract Id",
	"Payer Patient Id",
	"Application Payer Patient Id",
	"Provider Patient Id",
	"Application Provider Patient Id",
	"Invoice Id",
	"Application Invoice Id",
	"Reason Id",
	"Action Taken Reason Id",
	ENVIRONMENT,
	"Service Hours",
	"Billed Hours",
	"Billed Units",
	"Billed Rate",
	"Total Billed Amount",
	"Billed",
	"Invoice Number",
	"Invoice Created Date",
	"Invoice Date",
	"External Claim Number",
	"Payment Status",
	"Payment Amount",
	"Adjustment Amount",
	"Travel Time Adjustment Amount",
	"Write off Amount",
	"Other Adjustment Amount",
	"Days to Bill",
	"Days to pay",
	"Admitting Diagnosis",
	"Primary Diagnosis",
	"Diagnosis Code",
	"Diagnosis Description",
	"Auth Billing Hours Difference",
	"Missed Visit Reason",
	"EVV Complete",
	"Call In Duties Performed",
	"Call Out Duties Performed",
	"Call In Call Duration",
	"Call Out Call Duration",
	"Confirmed Hours Used",
	"Action Taken",
	"Non Payer Address",
	"Non Payer Phone Number",
	"Created Date Time",
	"Authorization Status",
	"Billing Batch Number",
	"Billing Batch Date",
	"Billing Batch Status",
	"Visit Date Month",
	"ExceptionInd",
	"Manual Edit",
	"Is Exception",
	"Exception Id",
	"Exception Description",
	"Exception Reason",
	"Visit Updated Timestamp",
	"Visit Updated User Id",
	"Application Visit Updated User Id",
	"Permanent Deleted",
	"Bill Rate Non-Billed",
	"Bill Hours Non-Billed",
	"Bill Units Non-Billed",
	"Bill Type Non-Billed",
	"Bill Amount Non-Billed",
	"Usage Tags"
) as (
    

with data as
(
    select
        * exclude ("Usage Tags"),
        OBJECT_INSERT("Usage Tags",'object','VISIT_CALL_PERFORMANCE_CR',true)::variant as "Usage Tags"
    from
    core.visit_call_performance_complete
    where "Permanent Deleted" = false
    and
        "Visit Date" >= cast('2025-11-16 21:05:44.869213-05:00' as date)-interval '18 month' /*and CURRENT_DATE() + 60*/

    
        and
        (
        "Usage Tags":providerState='NY' and "Usage Tags":payerState in ('NY','<NULL>','UNKNOWN')
        or
        "Usage Tags":payerState='NY' and "Usage Tags":providerState in ('NY','<NULL>','UNKNOWN')
        )
    

)
select * from data
  )
/* {"app": "dbt", "dbt_version": "2025.11.4+d8fa9da", "profile_name": "user", "target_name": "prod", "node_id": "model.hha.FactVisitCallPerformance_CR"} */;


-- ANALYTICS.BI.FACTVISITCALLPERFORMANCE_DELETED_CR source

create or replace view ANALYTICS.BI.FACTVISITCALLPERFORMANCE_DELETED_CR(
	"Visit Id",
	"Application Visit Id",
	"External Visit Id",
	"External EVV MS Id",
	"External Source",
	"Visit Date Id",
	"Visit Date",
	"Scheduled Start Time",
	"Scheduled End Time",
	"Confirmed Scheduled Start Time",
	"Confirmed Scheduled End Time",
	"Visit Start Time",
	"Visit End Time",
	"Is Skilled Visit",
	"Is Missed",
	"Edit Notes",
	"Is Confirmed",
	"Discipline",
	"Discipline Type",
	"External Invoice Number",
	"Contract Bill Rate",
	"Scheduled Duration Minutes",
	"Schedule Type",
	"Bill Type",
	"Contract Adjusted Minutes",
	"Visit Source",
	"Visit Time",
	"Visit Time With Time Format",
	"Scheduled Time",
	"Confirmed Scheduled Time",
	"Scheduled Duration Hours",
	"Visit Duration Hours",
	"Is Short Visit",
	"Is Late Start",
	"Is Short Visit (15)",
	"Is Late Start (15)",
	"Short Duration Minutes",
	"Late Duration Minutes",
	"Short Visit Range",
	"Late Duration Range",
	"Visit Duration Range",
	"Contract Type",
	"Contract Usage Type",
	"Approved Travel Time Hours",
	"Coordinator Name",
	"Has Live In Caregiver",
	"Employer Internal Number",
	"Employer Name",
	"Missed Visit Notes",
	"Call In Device Type",
	"Call In Phone Number",
	"Call In Time",
	"Call In Time Minutes",
	"Call In Rounded (15)",
	"Call In GPS Coordinates",
	"Call In Address",
	"Call In Linked Date",
	"Call In Linked By User",
	"Call Out Device Type",
	"Call Out Phone Number",
	"Call Out Time",
	"Call Out Time Minutes",
	"Call Out Rounded (15)",
	"Call Out GPS Coordinates",
	"Call Out Address",
	"Call Out Linked Date",
	"Call Out Linked By User",
	"Payer Id",
	"Application Payer Id",
	"Provider Id",
	"Application Provider Id",
	"Office Id",
	"Application Office Id",
	"Patient Id",
	"Application Patient Id",
	"Caregiver Id",
	"Application Caregiver Id",
	"Service Code Id",
	"Application Service Code Id",
	"Linked Service Code Id",
	"Application Linked Service Code Id",
	"Orig Service Code Id",
	"Application Orig Service Code Id",
	"Contract Id",
	"Application Contract Id",
	"Payer Patient Id",
	"Application Payer Patient Id",
	"Provider Patient Id",
	"Application Provider Patient Id",
	"Invoice Id",
	"Application Invoice Id",
	"Reason Id",
	"Action Taken Reason Id",
	ENVIRONMENT,
	"Service Hours",
	"Billed Hours",
	"Billed Units",
	"Billed Rate",
	"Total Billed Amount",
	"Billed",
	"Invoice Number",
	"Invoice Created Date",
	"Invoice Date",
	"External Claim Number",
	"Payment Status",
	"Payment Amount",
	"Adjustment Amount",
	"Travel Time Adjustment Amount",
	"Write off Amount",
	"Other Adjustment Amount",
	"Days to Bill",
	"Days to pay",
	"Admitting Diagnosis",
	"Primary Diagnosis",
	"Diagnosis Code",
	"Diagnosis Description",
	"Auth Billing Hours Difference",
	"Missed Visit Reason",
	"EVV Complete",
	"Call In Duties Performed",
	"Call Out Duties Performed",
	"Call In Call Duration",
	"Call Out Call Duration",
	"Confirmed Hours Used",
	"Action Taken",
	"Non Payer Address",
	"Non Payer Phone Number",
	"Created Date Time",
	"Authorization Status",
	"Billing Batch Number",
	"Billing Batch Date",
	"Billing Batch Status",
	"Visit Date Month",
	"ExceptionInd",
	"Manual Edit",
	"Is Exception",
	"Exception Id",
	"Exception Description",
	"Exception Reason",
	"Visit Updated Timestamp",
	"Visit Updated User Id",
	"Application Visit Updated User Id",
	"Permanent Deleted",
	"Bill Rate Non-Billed",
	"Bill Hours Non-Billed",
	"Bill Units Non-Billed",
	"Bill Type Non-Billed",
	"Bill Amount Non-Billed",
	"Usage Tags"
) as (
    

with data as
(
    select
        * exclude ("Usage Tags"),
        OBJECT_INSERT("Usage Tags",'object','VISIT_CALL_PERFORMANCE_CR',true)::variant as "Usage Tags"
    from
    core.visit_call_performance_deleted_complete
    where
        "Visit Date" >= cast('2025-11-16 21:05:44.869213-05:00' as date)-interval '18 month' /*and CURRENT_DATE() + 60*/

    
        and
        (
            "Usage Tags":providerState='NY' and "Usage Tags":payerState in ('NY','<NULL>','UNKNOWN')
            or
            "Usage Tags":payerState='NY' and "Usage Tags":providerState in ('NY','<NULL>','UNKNOWN')
        )
    

)

select * from data
  )
/* {"app": "dbt", "dbt_version": "2025.11.4+d8fa9da", "profile_name": "user", "target_name": "prod", "node_id": "model.hha.FactVisitCallPerformance_Deleted_CR"} */;