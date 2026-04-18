@echo off
setlocal
set PYTHON_EXE=C:\Tools\python\Scripts\python.exe
if not exist "%PYTHON_EXE%" (
  echo Python runtime not found at %PYTHON_EXE%
  exit /b 1
)
cd /d C:\YTSystem\apps\yt_summary_pdf_generator
"%PYTHON_EXE%" main.py
