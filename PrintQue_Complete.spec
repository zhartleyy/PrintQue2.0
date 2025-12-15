# -*- mode: python ; coding: utf-8 -*-
import sys
import os
from PyInstaller.utils.hooks import collect_all, collect_data_files, collect_submodules

block_cipher = None

# Collect all dependencies
datas = []
binaries = []
hiddenimports = []

# Flask and all extensions
for package in ['flask', 'flask_socketio', 'jinja2', 'click', 'itsdangerous', 'werkzeug', 'markupsafe']:
    tmp_ret = collect_all(package)
    datas += tmp_ret[0]
    binaries += tmp_ret[1]
    hiddenimports += tmp_ret[2]

# Socket.IO and Engine.IO
for package in ['socketio', 'engineio', 'python_socketio', 'python_engineio']:
    try:
        hiddenimports += collect_submodules(package)
    except:
        pass

# Other critical packages
packages_to_collect = [
    'eventlet',
    'eventlet.green',
    'eventlet.green.subprocess',
    'eventlet.green.ssl',
    'eventlet.green.threading',
    'eventlet.hubs',
    'dns',
    'dns.resolver',
    'cryptography',
    'cryptography.fernet',
    'aiohttp',
    'aiofiles',
    'requests',
    'urllib3',
    'certifi',
    'psutil',
    'dotenv',
    'simple_websocket',
    'bidict',
    'greenlet'
]

for package in packages_to_collect:
    try:
        hiddenimports.append(package)
    except:
        pass

# Add specific imports that are often missed
hiddenimports += [
    'engineio.async_drivers.threading',
    'engineio.async_drivers.eventlet',
    'flask.json.provider',
    'flask.json.tag',
    'flask.logging',
    'flask.templating',
    'flask.signals',
    'flask_socketio',
    'eventlet.wsgi',
    'eventlet.websocket',
    'werkzeug.routing',
    'werkzeug.serving',
    'jinja2.ext',
    'dns.rdataclass',
    'dns.rdatatype',
    'dns.exception',
    'six',
    'six.moves',
    'six.moves.urllib',
    'six.moves.urllib.parse',
    'pkg_resources',
    'pkg_resources.extern',
    'bambu_handler',
    'paho',
    'paho.mqtt',
    'paho.mqtt.client',
]

# Add your project files and folders
datas += [
    ('templates', 'templates'),
    ('static', 'static') if os.path.exists('static') else ('templates', '.'),
    ('README.txt', '.') if os.path.exists('README.txt') else ('requirements.txt', '.'),
    ('license.key', '.') if os.path.exists('license.key') else ('requirements.txt', '.'),
]

# Add all your Python modules
your_modules = [
    'routes',
    'state',
    'printer_manager',
    'config',
    'license_validator',
    'license_routes',
    'printer_routes',
    'order_routes',
    'misc_routes',
    'retry_utils',
    'default_settings',
    'utils',
    'bambu_handler',
]

for module in your_modules:
    if os.path.exists(f'{module}.py'):
        hiddenimports.append(module)

a = Analysis(
    ['run_app.py'],
    pathex=[os.path.abspath('.')],
    binaries=binaries,
    datas=datas,
    hiddenimports=hiddenimports,
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=['matplotlib', 'numpy', 'pandas', 'scipy', 'PIL', 'tkinter'],
    win_no_prefer_redirects=False,
    win_private_assemblies=False,
    cipher=block_cipher,
    noarchive=False,
)

pyz = PYZ(a.pure, a.zipped_data, cipher=block_cipher)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.zipfiles,
    a.datas,
    [],
    name='PrintQue',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=True,
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon='printque.ico' if os.path.exists('printque.ico') else None
)
