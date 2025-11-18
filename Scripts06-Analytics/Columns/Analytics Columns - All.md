### Schema: `ANALYTICS.BI` **Consolidated Tables and Columns:**

1.  **`DIMCAREGIVER`**
    *   `"Caregiver Id"`
    *   `"SSN"`
    *   `"Status"`
    *   `"Caregiver Code"`
    *   `"Caregiver Fullname"`
    *   `"Caregiver Firstname"`
    *   `"Caregiver Lastname"`
    *   `"Application Caregiver Id"`

2.  **`DIMCONTRACT`**
    *   `"Contract Id"`
    *   `"Is Active"`
    *   `"Contract Name"`
    *   `"Application Contract Id"`

3.  **`DIMOFFICE`**
    *   `"Office Id"`
    *   `"Application Office Id"`
    *   `"Office Name"`
    *   `"Is Active"`

4.  **`DIMPATIENT`**
    *   `"Patient Id"`
    *   `"Admission Id"`
    *   `"Patient Name"`
    *   `"Patient Firstname"`
    *   `"Patient Lastname"`
    *   `"Medicaid Number"`
    *   `"Status"`
    *   `"Application Patient Id"`

5.  **`DIMPATIENTADDRESS`**
    *   `"Patient Address Id"`
    *   `"Application Patient Address Id"`
    *   `"Address Line 1"`
    *   `"Address Line 2"`
    *   `"City"`
    *   `"Address State"`
    *   `"Zip Code"`
    *   `"County"`
    *   `"Patient Id"`
    *   `"Application Patient Id"`
    *   `"Longitude"`
    *   `"Latitude"`
    *   `"Application Created UTC Timestamp"`
    *   `"Primary Address"`
    *   `"Address Type"`

6.  **`DIMPAYER`**
    *   `"Payer Id"`
    *   `"Application Payer Id"`
    *   `"Payer Name"`
    *   `"Is Active"`
    *   `"Is Demo"`
    *   `"Payer State"`

7.  **`DIMPAYERPROVIDER`**
    *   `"Application Payer Id"`
    *   `"Application Provider Id"`
    *   `"Payer Id"`
    *   `"Provider Id"`

8.  **`DIMPROVIDER`**
    *   `"Address State"`
    *   `"Application Provider Id"`
    *   `"Environemnt"`
    *   `"Federal Tax Number"`
    *   `"Is Active"`
    *   `"Is Demo"`
    *   `"Phone Number 1"`
    *   `"Provider Id"`
    *   `"Provider Name"`

9.  **`DIMSERVICECODE`**
    *   `"Service Code Id"`
    *   `"Application Service Code Id"`
    *   `"Service Code"`

10. **`DIMUSER`**
    *   `"Aggregator Database Name"`
    *   `"Application User Id"`
    *   `"Application Vendor Id"`
    *   `"User Email Address"`
    *   `"User Fullname"`
    *   `"User Id"`
    *   `"Vendor Id"`

11. **`DIMUSEROFFICES`**
    *   `"Office Id"`
    *   `"User Id"`
    *   `"Vendor Id"`
    *   `"Vendor Type"`

12. **`FACTCAREGIVERABSENCE`**
    *   `"Caregiver Vacation Id"`
    *   `"End Date"`
    *   `"Global Caregiver Id"`
    *   `"Office Id"`
    *   `"Provider Id"`
    *   `"Start Date"`

13. **`FACTCAREGIVERINSERVICE`**
    *   `"Application Caregiver Inservice Id"`
    *   `"Caregiver Id"`
    *   `"Inservice end date"`
    *   `"Inservice start date"`
    *   `"Office Id"`
    *   `"Provider Id"`

14. **`FACTVISITCALLPERFORMANCE_CR`**
    *   `"Application Contract Id"`
    *   `"Application Caregiver Id"`
    *   `"Application Office Id"`
    *   `"Application Payer Id"`
    *   `"Application Payer Patient Id"`
    *   `"Application Patient Id"`
    *   `"Application Provider Id"`
    *   `"Application Provider Patient Id"`
    *   `"Application Visit Id"`
    *   `"Bill Rate Non-Billed"`
    *   `"Bill Type"`
    *   `"Billed"`
    *   `"Billed Hours"`
    *   `"Billed Rate"`
    *   `"Call In Time"`
    *   `"Call In GPS Coordinates"`
    *   `"Call Out Device Type"`
    *   `"Call Out Time"`
    *   `"Call Out GPS Coordinates"`
    *   `"Caregiver Id"`
    *   `"Contract Id"`
    *   `"Invoice Date"`
    *   `"Is Missed"`
    *   `"Missed Visit Reason"`
    *   `"Office Id"`
    *   `"Patient Id"`
    *   `"Payer Id"`
    *   `"Payer Patient Id"`
    *   `"Provider Id"`
    *   `"Provider Patient Id"`
    *   `"Scheduled End Time"`
    *   `"Scheduled Start Time"`
    *   `"Service Code Id"`
    *   `"Total Billed Amount"`
    *   `"Visit Date"`
    *   `"Visit End Time"`
    *   `"Visit Id"`
    *   `"Visit Start Time"`
    *   `"Visit Updated Timestamp"`
    *   `"Visit Updated User Id"`

15. **`FACTVISITCALLPERFORMANCE_DELETED_CR`**
    *   `"Application Visit Id"`
    *   `"Service Code Id"`
    *   `"Application Office Id"`
    *   `"Scheduled Start Time"`
    *   `"Application Provider Id"`
    *   `"Visit Updated User Id"`
    *   `"Call In Time"`
    *   `"Visit Date"`
    *   `"Call Out Time"`
    *   `"Billed Rate"`
    *   `"Bill Rate Non-Billed"`
    *   `"Caregiver Id"`
    *   `"Visit Id"`
    *   `"Invoice Date"`
    *   `"Contract Id"`
    *   `"Call In GPS Coordinates"`
    *   `"Scheduled End Time"`
    *   `"Payer Patient Id"`
    *   `"Bill Type"`
    *   `"Application Payer Id"`
    *   `"Visit Start Time"`
    *   `"Patient Id"`
    *   `"Provider Id"`
    *   `"Application Contract Id"`
    *   `"Payer Id"`
    *   `"Visit Updated Timestamp"`
    *   `"Billed"`
    *   `"Provider Patient Id"`
    *   `"Application Provider Patient Id"`
    *   `"Missed Visit Reason"`
    *   `"Call Out GPS Coordinates"`
    *   `"Application Visit Updated User Id"`
    *   `"Application Patient Id"`
    *   `"Visit End Time"`
    *   `"Is Missed"`
    *   `"Application Caregiver Id"`
    *   `"Application Payer Patient Id"`
    *   `"Call Out Device Type"`
    *   `"Total Billed Amount"`
    *   `"Office Id"`
    *   `"Application Service Code Id"`
    *   `"Billed Hours"`