@echo off
if not "%~1"=="p" start /min cmd.exe /c %0 p&exit
set base_dir=%~dp0
%base_dir:~0,2%
pushd %base_dir%
title vnpy_recorder %base_dir%
python.exe .\vnpy_recorder.py
popd
