@echo off
setlocal EnableExtensions DisableDelayedExpansion
title TSE_LOB

rem Always run from the directory containing this batch file.
pushd "%~dp0" >nul 2>&1
if errorlevel 1 goto :directory_error
set "APP_DIR=%CD%"
set "VENV_DIR=%APP_DIR%\.venv"
set "PY_EXE=%VENV_DIR%\Scripts\python.exe"

rem Keep Python isolated from user-level packages and external PYTHONPATH values.
set "PYTHONNOUSERSITE=1"
set "PYTHONPATH="
set "PYTHONHOME="
set "PIP_DISABLE_PIP_VERSION_CHECK=1"
set "PIP_NO_INPUT=1"
set "TSE_LOB_LAUNCHED=1"

echo ============================================================
echo TSE_LOB - Local setup and launch
echo ============================================================
 echo.

if not exist "%APP_DIR%\TSE_LOB.py" goto :script_missing

if not exist "%PY_EXE%" goto :create_venv
"%PY_EXE%" -I -c "import sys; raise SystemExit(0 if sys.version_info >= (3,10) else 1)" >nul 2>&1
if errorlevel 1 goto :recreate_venv
goto :venv_ready

:create_venv
echo [1/3] Creating local Python environment...
where py >nul 2>&1
if not errorlevel 1 (
    py -3 -c "import sys; raise SystemExit(0 if sys.version_info >= (3,10) else 1)" >nul 2>&1
    if not errorlevel 1 (
        py -3 -m venv "%VENV_DIR%"
        if errorlevel 1 goto :venv_error
        goto :venv_ready
    )
)
where python >nul 2>&1
if errorlevel 1 goto :python_missing
python -c "import sys; raise SystemExit(0 if sys.version_info >= (3,10) else 1)" >nul 2>&1
if errorlevel 1 goto :python_missing
python -m venv "%VENV_DIR%"
if errorlevel 1 goto :venv_error
goto :venv_ready

:recreate_venv
echo Existing virtual environment is invalid. Recreating it...
if exist "%VENV_DIR%" rmdir /s /q "%VENV_DIR%"
if exist "%VENV_DIR%" goto :venv_error
goto :create_venv

:venv_ready
if not exist "%PY_EXE%" goto :venv_error
"%PY_EXE%" -I -c "import sys; raise SystemExit(0 if sys.version_info >= (3,10) else 1)" >nul 2>&1
if errorlevel 1 goto :python_version_error

echo [2/3] Checking required packages...
"%PY_EXE%" -I -c "import flask, requests, jdatetime" >nul 2>&1
if errorlevel 1 (
    echo Installing Flask, Requests and jdatetime into the local environment...
    "%PY_EXE%" -m pip install --disable-pip-version-check Flask requests jdatetime
    if errorlevel 1 goto :dependency_error
)

rem PyArrow is optional. CSV output remains available if it cannot be installed.
"%PY_EXE%" -I -c "import pyarrow" >nul 2>&1
if errorlevel 1 (
    echo Installing optional Parquet support...
    "%PY_EXE%" -m pip install --disable-pip-version-check pyarrow
    if errorlevel 1 echo WARNING: PyArrow installation failed. CSV output is still available.
)

echo [3/3] Starting TSE_LOB...
echo.
"%PY_EXE%" -I "%APP_DIR%\TSE_LOB.py" %*
set "APP_EXIT=%ERRORLEVEL%"
if not "%APP_EXIT%"=="0" goto :app_error

goto :success

:success
echo.
echo TSE_LOB has stopped.
popd
exit /b 0

:directory_error
echo ERROR: Cannot access the application directory.
goto :failure

:script_missing
echo ERROR: TSE_LOB.py was not found next to this batch file.
goto :failure

:python_missing
echo ERROR: Python 3.10 or newer was not found.
echo Install Python 3.10+ and enable the Python launcher or PATH option.
goto :failure

:python_version_error
echo ERROR: The local environment does not use Python 3.10 or newer.
goto :failure

:venv_error
echo ERROR: Failed to create or repair the local virtual environment.
goto :failure

:dependency_error
echo ERROR: Required package installation failed.
echo Check your internet connection and run this batch file again.
goto :failure

:app_error
echo ERROR: TSE_LOB exited with code %APP_EXIT%.
goto :failure

:failure
echo.
echo Setup or launch failed. Review the messages above.
pause
popd
exit /b 1
