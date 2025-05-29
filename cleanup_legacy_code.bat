@echo off
echo ==========================
echo Legacy Code Cleanup Script
echo ==========================

:: Activate the virtual environment
echo Activating virtual environment...
call "chatmrpt_venv\Scripts\activate.bat" 2>nul
if %ERRORLEVEL% NEQ 0 (
  echo Failed to activate chatmrpt_venv, trying to find it...
  for /d %%i in (*venv*) do (
    echo Found possible virtual environment: %%i
    call "%%i\Scripts\activate.bat" 2>nul
    if %ERRORLEVEL% EQU 0 (
      echo Successfully activated %%i
      goto :env_activated
    )
  )
  echo WARNING: Could not activate virtual environment. Cleanup may not work properly.
)

:env_activated
echo.
echo Setting up environment...

echo Running cleanup script...
python cleanup_legacy_code.py

echo.
echo Cleanup complete. Please restart your Flask application.
echo ==========================

pause 