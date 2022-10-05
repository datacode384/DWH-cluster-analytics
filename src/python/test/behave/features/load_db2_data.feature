Feature: Load db2 data

  Background: Empty db2 tables
    Given a spark session
#    Given db2 table "job_step_execution" truncated
#    And db2 table "job_execution" truncated

  @wip
  Scenario: load data in raw_zone
    Given the following scripts are executed
      | SCRIPT                      |
      | copy_lv_wrapper_rawvault.py |
    Then db2 table "job_execution" contains
      | process_id | application_name | tenant_name | status   |
      | 1          | dwh              | ITERGO      | finished |
    And db2 table "job_step_execution" contains
      | process_step_id | process_id | step_name                       | step_status | read_count | filter_count | write_count | error_code | return_code |
      | 1               | 1          | pas_lv_load_rawvault_ITERGO_DWH | finished    | NULL       | NULL         | NULL        | 0          | 0           |

