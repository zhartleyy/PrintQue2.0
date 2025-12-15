# PrintQue - 3D Printer Management System (Open Source)

PrintQue is a powerful and easy-to-use management system designed for 3D print farms.
It provides centralized control, monitoring, and queue management for multiple 3D printers, helping you maximize efficiency and productivity.

## Features

- **Centralized Control**: Manage all your 3D printers from a single web interface
- **Queue Management**: Create, prioritize, and distribute print jobs automatically
- **Real-time Monitoring**: Track printer status, progress, and temperatures
- **Group Organization**: Organize printers into groups for specialized workloads
- **Automatic Ejection**: Configure custom end G-code for automated part removal
- **Statistics Tracking**: Monitor filament usage and printer performance
- **Full Feature Access**: All advanced features, including API access and multi-tenant support, are enabled.

## Getting Started

### System Requirements

- Windows 10 or newer (Linux/macOS support is community-driven)
- 4GB RAM (8GB recommended)
- 500MB free disk space
- Network connectivity to your 3D printers

### Installation (Source)

1. Clone the repository: `git clone [repository-url]`
2. Install dependencies: `pip install -r requirements.txt`
3. Run the application: `python app.py`

### Accessing the Web Interface

PrintQue provides several ways to access its web interface:

- **Automatic Launch**: The web interface opens automatically when PrintQue starts
- **Manual Access**: Open any web browser and navigate to http://localhost:5000

## Printer Setup

1. From the web interface, click "Add Printer"
2. Enter printer details:
   - Name: A descriptive name for the printer
   - IP Address: The IP address of the printer (must be on the same network)
   - API Key: The OctoPrint API key for the printer
   - Group: Assign the printer to a group (optional)

3. Click "Add" to connect the printer

## Creating Print Jobs

1. Click "New Print Job" on the main dashboard
2. Upload your G-code file
3. Select quantity and target printer group(s)
4. Enable ejection and configure end G-code if needed
5. Click "Create Job" to add it to the queue

PrintQue will automatically distribute jobs to available printers based on group assignments and printer availability.

## Licensing and Version

**Version:** 2.0
**License:** MIT License. See `LICENSE.txt` for full details.

## Troubleshooting

### Common Issues

- **Connection Issues**: Ensure printers are powered on and connected to the network
- **API Key Errors**: Verify your OctoPrint API key is correct

### Logs

Logs are stored in your user directory under PrintQueData/app.log and can help diagnose issues.

### Support

For community support, please use the project's public issue tracker on GitHub (or equivalent platform).

## Legal

PrintQue is **Open Source** under the **MIT License**.
Copyright (c) 2025 2296281 Alberta Inc.