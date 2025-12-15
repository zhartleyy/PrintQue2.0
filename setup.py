import sys
import os
from cx_Freeze import setup, Executable

# Read requirements.txt to automatically include dependencies
def get_requirements():
    if os.path.exists('requirements.txt'):
        with open('requirements.txt') as f:
            return [line.strip() for line in f if line.strip() and not line.startswith('#')]
    return []

# Use README.md for long description for standard open-source practice
# Assuming README.txt has been renamed to README.md
try:
    with open("README.md", "r", encoding="utf-8") as fh:
        long_description = fh.read()
except FileNotFoundError:
    long_description = "PrintQue 3D Printer Farm Management (Open Source)"

# Note: Explicitly including all necessary packages for cx_Freeze/standalone build
# Added necessary packages: aiohttp, paho.mqtt, cryptography, webbrowser
build_exe_options = {
    "packages": ["os", "flask", "flask_socketio", "aiohttp", "paho.mqtt", "cryptography", "webbrowser"],
    "excludes": ["generate_hardware_license", "tkinter", "unittest"], # Exclude deprecated script and unnecessary modules
    "include_files": [
        "LICENSE.txt",
        "README.md", 
        "default_settings.json",
        "templates/", 
        "static/",
        "config.py", 
        "config.ini", 
    ]
}

# Keeping original file's logic for 'base'
base = None
if sys.platform == "win32":
    # Removed Win32GUI base, keep it console for debugging/logging visibility
    pass 

setup(
    name="PrintQue",
    version="2.0",
    description="PrintQue 3D Printer Farm Management (Open Source)",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="2296281 Alberta Inc",
    license="MIT", 
    url="[ADD LINK TO YOUR GITHUB REPOSITORY HERE]",
    install_requires=get_requirements(),
    options={"build_exe": build_exe_options},
    executables=[Executable("app.py", base=base)]
)