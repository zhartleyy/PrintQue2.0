PrintQue - Print Farm Manager
=============================

Quick Start:
1. Double-click 'Start_PrintQue.bat' to launch the server
2. Open your web browser and go to: http://localhost:5000
3. Add your printers and start managing your print farm!

Configuration:
- Copy 'config_template.env' to '.env' and modify the settings
- Default port is 5000 (change in .env if needed)
- License key is in 'license.key'

Data Storage:
- Printer data: data/printers.json
- Orders: data/orders.json
- Uploaded files: uploads/
- Certificates: certs/

First Run:
- The application will create all necessary data files automatically
- A default encryption key will be generated (WARNING shown in console)
- For production use, set your own ENCRYPTION_KEY in .env file

Troubleshooting:
- If port 5000 is in use, change PORT in .env file
- Check console window for error messages
- Ensure firewall allows the application
- First run may be slow due to Windows Defender scan

Support:
- GitHub: [Your GitHub URL]
- Email: [Your Support Email]

Version: 1.0
