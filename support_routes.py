from flask import Blueprint, render_template, request, flash, redirect, url_for, current_app, send_file
import smtplib
import ssl
import os
import io
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime
import logging
from state import PRINTERS, printers_rwlock, ReadLock
from license_validator import get_license_info 

# Create a Blueprint for support-related routes
support_bp = Blueprint('support', __name__, url_prefix='/support')

def send_support_email(name, email, subject, message, license_info, system_info):
    """
    STUB: Support email feature is DISABLED in the open-source version.
    Users are directed to the project's public issue tracker.
    """
    logging.warning("Support email feature is DISABLED in the open-source version. Please use the public issue tracker.")
    
    # Log the support message for local debugging purposes
    log_message = (
        f"--- SUPPORT REQUEST LOG (NOT SENT) ---\n"
        f"Name: {name}\nEmail: {email}\nSubject: {subject}\n"
        f"Message:\n{message}\n"
        f"System Info: {system_info}\nLicense Info: {license_info}\n"
        f"----------------------------------------\n"
    )
    logging.info(log_message)
    
    return False, "This feature is disabled in the open-source version. Please submit issues to the project's public issue tracker on GitHub (or equivalent platform)."


@support_bp.route('/', methods=['GET', 'POST'])
def support_page():
    """Display the support page and handle form submissions."""
    
    # Check if this is a form submission
    if request.method == 'POST':
        name = request.form.get('name')
        email = request.form.get('email')
        subject = request.form.get('subject')
        message = request.form.get('message')
        
        # Gather info 
        system_info = {'version': current_app.config.get('VERSION', 'Unknown')}
        license_info = get_license_info()
        
        success, error_msg = send_support_email(name, email, subject, message, license_info, system_info)
        
        if success:
            flash('Your support request has been sent successfully.', 'success')
            return redirect(url_for('support.support_page'))
        else:
            flash(f'Failed to send support request: {error_msg}', 'error')
    
    # Get license info for display
    license_info = get_license_info()
    
    return render_template('support.html', license=license_info)


@support_bp.route('/download-logs')
def download_logs():
    """Download logs from the last 5 minutes"""
    try:
        # Import the logger module to access the function
        from logger import get_recent_logs

        # Get logs from the last 5 minutes
        logs_content = get_recent_logs(minutes=5)

        # Create a BytesIO object to serve as a file
        log_buffer = io.BytesIO()
        log_buffer.write(logs_content.encode('utf-8'))
        log_buffer.seek(0)

        # Generate filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = f'printque_logs_{timestamp}.txt'

        # Send the file
        return send_file(
            log_buffer,
            mimetype='text/plain',
            as_attachment=True,
            download_name=filename
        )

    except Exception as e:
        logging.error(f"Error downloading logs: {str(e)}")
        flash('Failed to download logs. Please try again later.', 'error')
        return redirect(url_for('support.support_page'))