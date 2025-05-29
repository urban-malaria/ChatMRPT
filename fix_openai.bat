@echo off
echo ==========================
echo OpenAI API Fix Script
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
  echo WARNING: Could not activate virtual environment. Fix may not work properly.
)

:env_activated
echo.
echo Setting up environment...

:: Check if OpenAI API key is set
if "%OPENAI_API_KEY%"=="" (
  echo WARNING: OPENAI_API_KEY environment variable not set.
  echo You should set it for the application to function properly.
  echo.
)

echo Running fix script...
python fix_openai.py

echo.
echo Fix complete. Please restart your Flask application.
echo ==========================

pause 