# M02-01-Project-HealthCare-IoT-Cloud


## Generating Aggregate data
`python data_aggregation_test.py`

## Generate alerts data
`python alert_generation_test.py`

## Notes
* Code creates tables automatically if they do not exist.
* I chose to create one table for each datatype for aggregate and alerts.

## Screenshots 
* Things
    ![](.\screenshots\things.PNG)

    ![](.\screenshots\thing_bsm_g101_details.PNG)
* Type
    ![](.\screenshots\type.PNG)
* Group
    ![](.\screenshots\group.PNG)
* Policy
    ![](.\screenshots\policy.PNG)

    ![](.\screenshots\policy_details.PNG)
    
* Rule
    ![](.\screenshots\things.PNG)
* Data Table
    ![](.\screenshots\bsm_data_dynamodb_table.PNG)
* Table after running aggregate data generator script
    ![](.\screenshots\tables_generated_after_aggregation.PNG)
* Sample Aggregate data
    ![](.\screenshots\aggregate_data.PNG)
* Tables generated after running alerts generator script
    ![](.\screenshots\alert_tables_generated.PNG)
* Samaple alerts data
    ![](screenshots\alert_data_for_spo2.PNG)

