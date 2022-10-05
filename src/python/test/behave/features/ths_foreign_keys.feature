Feature: Framework test

  Background: Empty db2 tables
    Given a spark session
    Given db2 table "job_step_execution" truncated
    And db2 table "job_execution" truncated

  Scenario: load data into empty tables
    Given dwh table "bearbnw" in "rawvault_bddtest" truncated
    And dwh table "lv" in "rawvault_bddtest" truncated
    And dwh table "lv" in "core_bddtest" truncated
    And dwh table "bearbnw" in "core_bddtest" truncated
    And file "bearbnw.csv" created
      | BEARBID | BEARBIDABG |
      | 4711    | 999        |
      | 4712    | 998        |
    And file "lv.csv" created
      | BEARBID | BEARBIDABG |
      | 4711    | 999        |
      | 4712    | 998        |
    And the following jobs are executed
      | JOB_NAME                         |
      | fw_test1_jc_start.py             |
      | fw_test1_lf_load_data_bearbnw.py |
      | fw_test1_lf_raw_zone_bearbnw.py  |
      | fw_test1_lf_core_bearbnw.py      |
      | fw_test1_lf_load_data_lv.py      |
      | fw_test1_lf_raw_zone_lv.py       |
      | fw_test1_lf_core_lv.py           |
      | fw_test1_jc_stop.py              |
    Then dwh table "bearbnw" in "core_bddtest" contains
      | BEARBID | BEARBIDABG |
      | 4711    | 999        |
      | 4712    | 998        |
    And dwh table "lv" in "core_bddtest" contains
      | BEARBID | BEARBIDABG |
      | 4711    | 999        |
      | 4712    | 998        |
    And the sql statement on join returns
      | counter | B_BEARBIDABG | L_BEARBIDABG |
      | 1       | 998          | 998          |
      | 1       | 999          | 999          |
