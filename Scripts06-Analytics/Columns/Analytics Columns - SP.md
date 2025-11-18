#### **Analytics Tables and Columns referred in stored procedures:**

1.  **`DIMPROVIDER`**
    *   `"Provider Id"`
    *   `"Application Provider Id"`
    *   `"Provider Name"`
    *   `"Is Active"`
    *   `"Is Demo"`
    *   `"Phone Number 1"`
    *   `"Federal Tax Number"`
    *   `"Environemnt"`

2.  **`DIMPAYERPROVIDER`**
    *   `"Payer Id"`
    *   `"Application Payer Id"`
    *   `"Provider Id"`
    *   `"Application Provider Id"`

3.  **`DIMPAYER`**
    *   `"Payer Id"`
    *   `"Application Payer Id"`
    *   `"Payer Name"`
    *   `"Is Active"`
    *   `"Is Demo"`
    *   `"Payer State"`

4.  **`FACTVISITCALLPERFORMANCE_CR`**
    *   `"Bill Rate Non-Billed"`
    *   `"Billed"`
    *   `"Billed Rate"`
    *   `"Missed Visit Reason"`
    *   `"Is Missed"`
    *   `"Call Out Device Type"`
    *   `"Total Billed Amount"`
    *   `"Provider Id"`
    *   `"Application Provider Id"`
    *   `"Visit Id"`
    *   `"Application Visit Id"`
    *   `"Visit Date"`
    *   `"Scheduled Start Time"`
    *   `"Scheduled End Time"`
    *   `"Visit Start Time"`
    *   `"Visit End Time"`
    *   `"Call In Time"`
    *   `"Call Out Time"`
    *   `"Caregiver Id"`
    *   `"Application Caregiver Id"`
    *   `"Office Id"`
    *   `"Application Office Id"`
    *   `"Payer Patient Id"`
    *   `"Application Payer Patient Id"`
    *   `"Provider Patient Id"`
    *   `"Application Provider Patient Id"`
    *   `"Patient Id"`
    *   `"Application Patient Id"`
    *   `"Call Out GPS Coordinates"`
    *   `"Call In GPS Coordinates"`
    *   `"Payer Id"`
    *   `"Application Payer Id"`
    *   `"Invoice Date"`
    *   `"Billed Hours"`
    *   `"Service Code Id"`
    *   `"Bill Type"`
    *   `"Visit Updated Timestamp"`
    *   `"Visit Updated User Id"`
    *   `"Application Contract Id"`
    *   `"Contract Id"`

5.  **`DIMCAREGIVER`**
    *   `"Caregiver Id"`
    *   `"SSN"`
    *   `"Status"`
    *   `"Caregiver Code"`
    *   `"Caregiver Fullname"`
    *   `"Caregiver Firstname"`
    *   `"Caregiver Lastname"`
    *   `"Application Caregiver Id"`

6.  **`DIMOFFICE`**
    *   `"Office Id"`
    *   `"Application Office Id"`
    *   `"Is Active"`
    *   `"Office Name"`

7.  **`DIMPATIENT`**
    *   `"Patient Id"`
    *   `"Admission Id"`
    *   `"Patient Name"`
    *   `"Patient Firstname"`
    *   `"Patient Lastname"`
    *   `"Medicaid Number"`
    *   `"Status"`

8.  **`DIMPATIENTADDRESS`**
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

9.  **`DIMCONTRACT`**
    *   `"Contract Id"`
    *   `"Is Active"`
    *   `"Contract Name"`

10. **`DIMSERVICECODE`**
    *   `"Service Code Id"`
    *   `"Application Service Code Id"`
    *   `"Service Code"`

11. **`DIMUSER`**
    *   `"User Id"`
    *   `"User Fullname"`

12. **`FACTCAREGIVERABSENCE`**
    *   `"Global Caregiver Id"`
    *   `"Provider Id"`
    *   `"Start Date"`
    *   `"End Date"`
    *   `"Caregiver Vacation Id"`
    *   `"Office Id"`

13. **`FACTVISITCALLPERFORMANCE_DELETED_CR`**
    *   `"Visit Id"`

14. **`FACTCAREGIVERINSERVICE`**
    *   `"Inservice start date"`
    *   `"Inservice end date"`
    *   `"Application Caregiver Inservice Id"`
    *   `"Caregiver Id"`
    *   `"Provider Id"`
    *   `"Office Id"`


[1 tool called]

I have analyzed the SQL scripts in the provided folder and compiled a list of all tables, views, and their respective columns referenced from the `ANALYTICS.BI` schema.

Here is the comprehensive list:

### Schema: `ANALYTICS.BI`

---

#### **Tables and Columns:**

1.  **`DIMPROVIDER`**
    *   `"Provider Id"`
    *   `"Application Provider Id"`
    *   `"Provider Name"`
    *   `"Is Active"`
    *   `"Is Demo"`
    *   `"Phone Number 1"`
    *   `"Federal Tax Number"`
    *   `"Environemnt"`

2.  **`DIMPAYERPROVIDER`**
    *   `"Payer Id"`
    *   `"Application Payer Id"`
    *   `"Provider Id"`
    *   `"Application Provider Id"`

3.  **`DIMPAYER`**
    *   `"Payer Id"`
    *   `"Application Payer Id"`
    *   `"Payer Name"`
    *   `"Is Active"`
    *   `"Is Demo"`
    *   `"Payer State"`

4.  **`FACTVISITCALLPERFORMANCE_CR`**
    *   `"Bill Rate Non-Billed"`
    *   `"Billed"`
    *   `"Billed Rate"`
    *   `"Missed Visit Reason"`
    *   `"Is Missed"`
    *   `"Call Out Device Type"`
    *   `"Total Billed Amount"`
    *   `"Provider Id"`
    *   `"Application Provider Id"`
    *   `"Visit Id"`
    *   `"Application Visit Id"`
    *   `"Visit Date"`
    *   `"Scheduled Start Time"`
    *   `"Scheduled End Time"`
    *   `"Visit Start Time"`
    *   `"Visit End Time"`
    *   `"Call In Time"`
    *   `"Call Out Time"`
    *   `"Caregiver Id"`
    *   `"Application Caregiver Id"`
    *   `"Office Id"`
    *   `"Application Office Id"`
    *   `"Payer Patient Id"`
    *   `"Application Payer Patient Id"`
    *   `"Provider Patient Id"`
    *   `"Application Provider Patient Id"`
    *   `"Patient Id"`
    *   `"Application Patient Id"`
    *   `"Call Out GPS Coordinates"`
    *   `"Call In GPS Coordinates"`
    *   `"Payer Id"`
    *   `"Application Payer Id"`
    *   `"Invoice Date"`
    *   `"Billed Hours"`
    *   `"Service Code Id"`
    *   `"Bill Type"`
    *   `"Visit Updated Timestamp"`
    *   `"Visit Updated User Id"`
    *   `"Application Contract Id"`
    *   `"Contract Id"`

5.  **`DIMCAREGIVER`**
    *   `"Caregiver Id"`
    *   `"SSN"`
    *   `"Status"`
    *   `"Caregiver Code"`
    *   `"Caregiver Fullname"`
    *   `"Caregiver Firstname"`
    *   `"Caregiver Lastname"`
    *   `"Application Caregiver Id"`

6.  **`DIMOFFICE`**
    *   `"Office Id"`
    *   `"Application Office Id"`
    *   `"Is Active"`
    *   `"Office Name"`

7.  **`DIMPATIENT`**
    *   `"Patient Id"`
    *   `"Admission Id"`
    *   `"Patient Name"`
    *   `"Patient Firstname"`
    *   `"Patient Lastname"`
    *   `"Medicaid Number"`
    *   `"Status"`

8.  **`DIMPATIENTADDRESS`**
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

9.  **`DIMCONTRACT`**
    *   `"Contract Id"`
    *   `"Is Active"`
    *   `"Contract Name"`

10. **`DIMSERVICECODE`**
    *   `"Service Code Id"`
    *   `"Application Service Code Id"`
    *   `"Service Code"`

11. **`DIMUSER`**
    *   `"User Id"`
    *   `"User Fullname"`

12. **`FACTCAREGIVERABSENCE`**
    *   `"Global Caregiver Id"`
    *   `"Provider Id"`
    *   `"Start Date"`
    *   `"End Date"`
    *   `"Caregiver Vacation Id"`
    *   `"Office Id"`

13. **`FACTVISITCALLPERFORMANCE_DELETED_CR`**
    *   `"Visit Id"`

14. **`FACTCAREGIVERINSERVICE`**
    *   `"Inservice start date"`
    *   `"Inservice end date"`
    *   `"Application Caregiver Inservice Id"`
    *   `"Caregiver Id"`
    *   `"Provider Id"`
    *   `"Office Id"`