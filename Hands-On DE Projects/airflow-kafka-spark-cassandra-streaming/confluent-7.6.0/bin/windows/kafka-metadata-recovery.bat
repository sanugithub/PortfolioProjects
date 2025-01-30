@echo off
rem Copyright 2023 Confluent Inc.

"%~dp0kafka-run-class.bat" io.confluent.kafka.tools.recovery.MetadataRecoveryTool %*
