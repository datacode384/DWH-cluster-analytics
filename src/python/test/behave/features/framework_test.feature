#Usage:  

#behave -i framework_test.feature  - To test all scenarios in a feature file at once
#behave -i framework_test.feature -n 'load data into emtpy tables' - To test a specific scenario
#behave -i framework_test.feature -n 'restart after failure in upload file, raw zone and core'


# Created by private at 2019-12-13
Feature: Framework test

  Background: Empty db2 tables
    Given a spark session
    Given db2 table "job_step_execution" truncated
    And db2 table "job_execution" truncated

  Scenario: load data into emtpy tables
    Given dwh table "bearbnw" in "rawvault_bddtest" truncated
    And dwh table "bearbnw" in "core_bddtest" truncated
    And file "bearbnw.csv" created
      | BEARBID | BEARBEITERID |
      | 91      | Migration    |
      | 92      | Batch        |
    And the following jobs are executed
      | JOB_NAME                         |
      | fw_test1_jc_start.py             |
      | fw_test1_lf_load_data_bearbnw.py |
      | fw_test1_lf_raw_zone_bearbnw.py  |
      | fw_test1_lf_core_bearbnw.py      |
      | fw_test1_jc_stop.py              |
    Then dwh table "bearbnw" in "rawvault_bddtest" contains
        # count indicates number of records - optional; required
      | BEARBID | BEARBEITERID | PROCESS_ID |
      | 91      | Migration    | 1          |
      | 92      | Batch        | 1          |
    And dwh table "bearbnw" in "core_bddtest" contains
      | BEARBID | BEARBEITERID | PROCESS_ID |
      | 91      | Migration    | 1          |
      | 92      | Batch        | 1          |
    And db2 table "job_execution" contains
      | process_id | application_name | tenant_name | status   |
      | 1          | dwh              | fw_test1    | finished |
    And db2 table "job_step_execution" contains
      | process_step_id | process_id | step_name                          | step_status | read_count | filter_count | write_count | error_code | return_code |
      | 1               | 1          | bearbnw_ingest_fw_test1_dwh        | finished    | NULL       | NULL         | NULL        | 0          | 0           |
      | 2               | 1          | bearbnw_load_rawvault_fw_test1_dwh | finished    | 2          | 0            | 2           | 0          | 0           |
      | 3               | 1          | bearbnw_load_core_fw_test1_dwh     | finished    | 2          | 0            | 2           | 0          | 0           |


  Scenario: basic delta load
    Given dwh table "lf_lv" in "rawvault_bddtest" truncated
    And dwh table "lf_lv" in "core_bddtest" truncated
    And file "BEARBNW.csv" created
      | BEARBID | PDID               |
      | 91      | "ERO_KAP_VIC_1987" |
      | 92      | "ERO_KAP_XXX_1999" |
    And the following jobs are executed
      | JOB_NAME                    |
      | fw_test1_jc_start           |
      | fw_test1_lf_load_Data_lf_lv |
      | fw_test1_lf_raw_zone_lf_lv  |
      | fw_test1_lf_core_lf_lv      |
      | fw_test1_jc_stop            |
    And file "BEARBNW.csv" created
      | BEARBID | PDID               |
      | 91      | "ERO_KAP_VIC_1987" |
      | 93      | "ERO_KAP_XXX_1999" |
    And the following jobs are executed
      | JOB_NAME                    |
      | fw_test1_jc_start           |
      | fw_test1_lf_load_Data_lf_lv |
      | fw_test1_lf_raw_zone_lf_lv  |
      | fw_test1_lf_core_lf_lv      |
      | fw_test1_jc_stop            |
    Then dwh table "bearbnw" in "rawvault_bddtest" contains
      | BEARBID | PDID               | PROCESS_ID | count |
      | 91      | "ERO_KAP_VIC_1987" | 1          | 1     |
      | 92      | "ERO_KAP_XXX_1999" | 1          | 1     |
      | 91      | "ERO_KAP_VIC_1987" | 2          | 1     |
      | 93      | "ERO_KAP_XXX_1999" | 2          | 1     |
    And dwh table "bearbnw" in "core_bddtest" contains
      | BEARBID | PDID               | PROCESS_ID |
      | 91      | "ERO_KAP_VIC_1987" | 1          |
      | 92      | "ERO_KAP_XXX_1999" | 1          |
      | 93      | "ERO_KAP_XXX_1999" | 2          |
    And db2 table "job_execution" contains
      | process_id | application_name | tenant_name | status   |
      | 1          | dwh              | ergo        | finished |
      | 2          | dwh              | ergo        | finished |
    And db2 table "job_execution" contains
      | process_step_id | process_id | step_name                | step_status | read_count | filter_count | write_count | error_code | return_code |
      | 1               | 1          | "ingest_ergo_dwh"        | "finished"  | 2          | 0            | 2           | 0          | 0           |
      | 2               | 1          | "load_raw_zone_ergo_dwh" | "finished"  | 2          | 0            | 2           | 0          | 0           |
      | 3               | 1          | "load_core_dwh"          | "finished"  | 2          | 0            | 2           | 0          | 0           |
      | 1               | 2          | "ingest_ergo_dwh"        | "finished"  | 2          | 0            | 2           | 0          | 0           |
      | 2               | 2          | "load_raw_zone_ergo_dwh" | "finished"  | 2          | 0            | 2           | 0          | 0           |
      | 3               | 2          | "load_core_dwh"          | "finished"  | 2          | 1            | 1           | 0          | 0           |

  Scenario: restart after failure in upload file, raw zone and core
    Given dwh table "lv" in "rawvault_bddtest" truncated
    And dwh table "lv" in "core_bddtest" truncated
    And file "lv.csv" created
      | BEARBID | PDID               |
      | 91      | "ERO_KAP_VIC_1987" |
      | 92      | "ERO_KAP_XXX_1999" |
    And the following jobs are executed
      | JOB_NAME                    |
      | fw_test1_jc_start.py        |
      | fw_test1_lf_load_data_lv.py |
      | fw_test1_lf_load_data_lv.py |
      | fw_test1_lf_raw_zone_lv.py  |
      | fw_test1_lf_raw_zone_lv.py  |
      | fw_test1_lf_core_lv.py      |
      | fw_test1_lf_core_lv.py      |
      | fw_test1_jc_stop.py         |
    Then dwh table "lv" in "rawvault_bddtest" contains
      | BEARBID | PDID             | PROCESS_ID |
      | 91      | ERO_KAP_VIC_1987 | 1          |
      | 92      | ERO_KAP_XXX_1999 | 1          |
      | 91      | ERO_KAP_VIC_1987 | 1          |
      | 92      | ERO_KAP_XXX_1999 | 1          |
    And dwh table "lv" in "core_bddtest" contains
      | BEARBID | PDID             | PROCESS_ID |
      | 91      | ERO_KAP_VIC_1987 | 1          |
      | 92      | ERO_KAP_XXX_1999 | 1          |
      | 91      | ERO_KAP_VIC_1987 | 1          |
      | 92      | ERO_KAP_XXX_1999 | 1          |
    And db2 table "job_execution" contains
      | process_id | application_name | tenant_name | status   |
      | 1          | dwh              | fw_test1    | finished |
    And db2 table "job_step_execution" contains
      | process_step_id | process_id | step_name                     | step_status | read_count | filter_count | write_count | error_code | return_code |
      | 1               | 1          | lv_ingest_fw_test1_dwh        | finished    | NULL       | NULL         | NULL        | 0          | 0           |
      | 2               | 1          | lv_ingest_fw_test1_dwh        | finished    | NULL       | NULL         | NULL        | 0          | 0           |
      | 3               | 1          | lv_load_rawvault_fw_test1_dwh | finished    | 2          | 0            | 2           | 0          | 0           |
      | 4               | 1          | lv_load_rawvault_fw_test1_dwh | finished    | 2          | -2           | 4           | 0          | 0           |
      | 5               | 1          | lv_load_core_fw_test1_dwh     | finished    | 4          | 0            | 4           | 0          | 0           |
      | 6               | 1          | lv_load_core_fw_test1_dwh     | finished    | 4          | 4            | 4           | 0          | 0           |

  @wip
  Scenario: load data into empty tables for multiple tenants.
    Given dwh table "bearbnw" in "rawvault_bddtest" truncated
    And dwh table "bearbnw" in "core_bddtest" truncated
    And file "bearbnw.csv" created
      | BEARBID | BEARBEITERID |
      | 91      | Migration    |
      | 92      | Batch        |
    And the following jobs are executed
      | JOB_NAME                         |
      | fw_test1_jc_start.py             |
      | fw_test1_lf_load_data_bearbnw.py |
      | fw_test1_lf_raw_zone_bearbnw.py  |
      | fw_test1_lf_core_bearbnw.py      |
      | fw_test1_jc_stop.py              |
      | itergo_jc_start.py               |
      | itergo_lf_load_data_bearbnw.py   |
      | itergo_lf_raw_zone_bearbnw.py    |
      | itergo_lf_core_bearbnw.py        |
      | itergo_jc_stop.py                |
    Then dwh table "bearbnw" in "rawvault_bddtest" contains
        # count indicates number of records - optional; required
      | BEARBID | BEARBEITERID | PROCESS_ID |
      | 91      | Migration    | 1          |
      | 92      | Batch        | 1          |
      | 91      | Migration    | 2          |
      | 92      | Batch        | 2          | 
    And dwh table "bearbnw" in "core_bddtest" contains
      | BEARBID | BEARBEITERID | PROCESS_ID |
      | 91      | Migration    | 2          |
      | 92      | Batch        | 2          |
      | 91      | Migration    | 1          |
      | 92      | Batch        | 1          |
    And db2 table "job_execution" contains
      | process_id | application_name | tenant_name | status   |
      | 1          | dwh              | fw_test1    | finished |
      | 2          | dwh              | itergo      | finished |
    And db2 table "job_step_execution" contains
      | process_step_id | process_id | step_name                          | step_status | read_count | filter_count | write_count | error_code | return_code |
      | 1               | 1          | bearbnw_ingest_fw_test1_dwh        | finished    | NULL       | NULL         | NULL        | 0          | 0           |
      | 2               | 1          | bearbnw_load_rawvault_fw_test1_dwh | finished    | 2          | 0            | 2           | 0          | 0           |
      | 3               | 1          | bearbnw_load_core_fw_test1_dwh     | finished    | 2          | 0            | 2           | 0          | 0           |
      | 4               | 2          | bearbnw_ingest_itergo_dwh          | finished    | NULL       | NULL         | NULL        | 0          | 0           |
      | 5               | 2          | bearbnw_load_rawvault_itergo_dwh   | finished    | 2          | 0            | 2           | 0          | 0           |
      | 6               | 2          | bearbnw_load_core_itergo_dwh       | finished    | 4          | 2            | 2           | 0          | 0           |

