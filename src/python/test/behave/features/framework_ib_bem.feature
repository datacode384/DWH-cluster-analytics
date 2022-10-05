# usage: behave -t '@wip' ./framework_ib_bem.feature
# Created by private at 2020-02-12
Feature: #Enter feature name here
  # Enter feature description here

  Background: Empty queue
    Given message queue "L0.DWH.BACKOUT.1" is empty

  Scenario: test connectivity to MQ
    Given the following messages haven been sent to queue "L0.DWH.BACKOUT.1"
      | MESSAGE     |
      | Testmessage |
    Then read message from queue "L0.DWH.BACKOUT.1" gives
      | MESSAGE     |
      | Testmessage |
    And message queue "L0.DWH.BACKOUT.1" is empty
   
   #@wip
  Scenario: send standard messages to  MQ of 4MB
    Given dwh table "bpms_bem_l0_dwh_backout_1" in "raw_zone" truncated
    Given send "250" messages with length "4096000" to queue "L0.DWH.BACKOUT.1" #prefixed with running nr, filled with '#' to max len
    #And the "sbt" execute command is called in path "/shared/intellij_projetcs/"
   
   @wip
  Scenario: send standard messages to  MQ of 1K
    Given dwh table "bpms_bem_l0_dwh_backout_1" in "raw_zone" truncated
    Given send "5000" messages with length "1024" to queue "L0.DWH.BACKOUT.1" #prefixed with running nr, filled with '#' to max len

   #@wip 
  Scenario: send standard messages to  MQ of 100K
    Given dwh table "bpms_bem_l0_dwh_backout_1" in "raw_zone" truncated
    Given send "5000" messages with length "102400" to queue "L0.DWH.BACKOUT.1" #prefixed with running nr, filled with '#' to max len

   #@wip
  Scenario: send standard messages to  MQ of 100K
    Given dwh table "bpms_bem_l0_dwh_backout_1" in "raw_zone" truncated
    Given send "10000" messages with length "102400" to queue "L0.DWH.BACKOUT.1" #prefixed with running nr, filled with '#' to max len


