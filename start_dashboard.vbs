Set WshShell = CreateObject("WScript.Shell")
WshShell.CurrentDirectory = "C:\Users\mcnug\OneDrive\Desktop\etsy statments"
WshShell.Run "pythonw etsy_dashboard.py", 0, False
