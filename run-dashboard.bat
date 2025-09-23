@echo off
REM Script de lancement du dashboard météo Streamlit
REM
REM Usage: run-dashboard.bat [chemin-données]
REM Exemple: run-dashboard.bat ../test-weather-data

echo =========================================
echo 🌤️ DASHBOARD MÉTÉO - KAFKA IPSSI
echo =========================================
echo.

REM Répertoire des données (argument ou valeur par défaut)
set DATA_PATH=%1
if "%DATA_PATH%"=="" set DATA_PATH=../test-weather-data

echo 📁 Répertoire de données: %DATA_PATH%
echo.

REM Vérification que le répertoire existe
if not exist "%DATA_PATH%" (
    echo ❌ ERREUR: Le répertoire %DATA_PATH% n'existe pas
    echo.
    echo Veuillez:
    echo   1. Générer des données de test: python generate_test_data.py
    echo   2. Ou utiliser vos données: run-dashboard.bat "../hdfs-exercise8"  
    echo.
    pause
    exit /b 1
)

echo 🚀 Lancement du dashboard Streamlit...
echo 🌐 Le dashboard sera accessible sur: http://localhost:8501
echo.
echo 💡 Pour arrêter le dashboard: Ctrl+C
echo.

REM Définir la variable d'environnement pour le chemin des données
set WEATHER_DATA_PATH=%DATA_PATH%

REM Lancement de Streamlit
"C:/Users/Nassim/AppData/Local/Programs/Python/Python312/python.exe" -m streamlit run dashboard_weather.py

pause