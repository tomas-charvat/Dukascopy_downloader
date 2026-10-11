@echo off
cd /d "%~dp0"
"%USERPROFILE%\miniconda3\envs\dukascopy-dowloader\python.exe" diag.py > diag_log.txt 2>&1
echo DONE >> diag_log.txt
