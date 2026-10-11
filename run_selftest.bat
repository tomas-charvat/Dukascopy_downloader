@echo off
REM Instaluje zavislosti a spusti offline + zivy test (vystup: selftest_log.txt)
cd /d "%~dp0"
set "PY=%USERPROFILE%\miniconda3\envs\dukascopy-dowloader\python.exe"
if not exist "%PY%" set "PY=python"
echo Python: %PY% > selftest_log.txt
"%PY%" --version >> selftest_log.txt 2>&1
echo === pip install === >> selftest_log.txt
"%PY%" -m pip install -r requirements.txt >> selftest_log.txt 2>&1
echo === offline unit tests === >> selftest_log.txt
"%PY%" -m unittest discover -s tests >> selftest_log.txt 2>&1
echo === live selftest === >> selftest_log.txt
set PYTHONIOENCODING=utf-8
"%PY%" duka.py selftest >> selftest_log.txt 2>&1
echo EXIT CODE %ERRORLEVEL% >> selftest_log.txt
echo === DONE === >> selftest_log.txt
