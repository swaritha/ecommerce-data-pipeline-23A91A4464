@echo off
echo Running E-Commerce Pipeline Tests...
echo =====================================
pytest tests/ -v --cov-report=term-missing --cov-report=html
if %errorlevel% neq 0 (
    echo.
    echo ERROR: Tests failed!
    pause
    exit /b 1
)
echo.
echo SUCCESS: All tests PASSED! 
echo Coverage report: htmlcov/index.html
echo.
dir data\raw\*.csv
pause
